import math
import uuid
from datetime import datetime
from typing import Any, Dict, List, Tuple

import pandas as pd
import streamlit as st
from supabase import create_client, Client


# =========================
# 고정 설정값
# =========================
SAFETY_WEEKS: float = 1.0
ROTATION_TABLE_NAME: str = "stock_rotation_plan_step2"
ACTION_TABLE_NAME: str = "inventory_action_plan_step2"


# =========================
# Streamlit 기본 설정
# =========================
st.set_page_config(
    page_title="Inventory Action Plan Step2",
    layout="wide"
)


# =========================
# Supabase 연결
# =========================
@st.cache_resource
def get_supabase_client() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)


# =========================
# 공통 유틸
# =========================
def to_float(v: Any, default: float = 0.0) -> float:
    x = pd.to_numeric(v, errors="coerce")
    if pd.isna(x):
        return default
    return float(x)


def to_int(v: Any, default: int = 0) -> int:
    x = pd.to_numeric(v, errors="coerce")
    if pd.isna(x):
        return default
    return int(round(float(x)))


def parse_year_week_sort_key(year_week: str) -> Tuple[int, int]:
    try:
        s = str(year_week).strip()
        y, w = s.split("-")
        return int(y), int(w)
    except Exception:
        return (0, 0)


def year_week_to_timestamp(year_week: str) -> pd.Timestamp:
    try:
        y, w = parse_year_week_sort_key(year_week)
        return pd.to_datetime(f"{y}-W{w:02d}-1", format="%G-W%V-%u", errors="coerce")
    except Exception:
        return pd.NaT


def timestamp_to_year_week(ts: pd.Timestamp) -> str:
    if pd.isna(ts):
        return ""
    iso = ts.isocalendar()
    return f"{int(iso.year)}-{int(iso.week):02d}"


def current_year_week() -> str:
    return timestamp_to_year_week(pd.Timestamp.today())


def shift_year_week(year_week: str, delta_weeks: int) -> str:
    ts = year_week_to_timestamp(year_week)
    if pd.isna(ts):
        return "NOW"
    shifted = ts + pd.Timedelta(weeks=delta_weeks)
    return timestamp_to_year_week(shifted)


def fetch_all_rows(
    client: Client,
    table_name: str,
    select_cols: str = "*",
    page_size: int = 1000
) -> pd.DataFrame:
    all_rows: List[Dict[str, Any]] = []
    start = 0

    while True:
        end = start + page_size - 1
        resp = (
            client.table(table_name)
            .select(select_cols)
            .range(start, end)
            .execute()
        )
        rows = resp.data or []

        if not rows:
            break

        all_rows.extend(rows)

        if len(rows) < page_size:
            break

        start += page_size

    return pd.DataFrame(all_rows)


# =========================
# 원천 데이터 로딩
# =========================
@st.cache_data(ttl=300)
def load_source_tables() -> Dict[str, pd.DataFrame]:
    client = get_supabase_client()

    return {
        "step1": fetch_all_rows(client, "store_inventory_status_step1", "*"),
        "rotation": fetch_all_rows(client, ROTATION_TABLE_NAME, "*"),
        "center_stock": fetch_all_rows(client, "center_stock", "*"),
        "inbound_schedule": fetch_all_rows(client, "inbound_schedule", "*"),
        "sku_weekly_forecast": fetch_all_rows(
            client,
            "sku_weekly_forecast",
            "sty,sku,plant,store_name,year_week,sale_qty,is_forecast,begin_stock"
        ),
    }


# =========================
# 전처리
# =========================
def prepare_step1_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "sty", "sku", "plant",
            "store_classification", "lead_time", "current_qty",
            "stock_weeks", "shortage_qty", "surplus_qty"
        ])

    out = df.copy()

    for col in ["sty", "sku", "plant", "store_classification"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].astype(str).str.strip()

    for col in ["lead_time", "stock_weeks"]:
        if col not in out.columns:
            out[col] = 0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)

    for col in ["current_qty", "shortage_qty", "surplus_qty"]:
        if col not in out.columns:
            out[col] = 0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype(int)

    return out


def prepare_rotation_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["sku", "from_plant", "to_plant", "transfer_qty"])

    out = df.copy()

    # 실제 컬럼명이 다르면 여기만 수정
    for col in ["sku", "from_plant", "to_plant"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].astype(str).str.strip()

    if "transfer_qty" not in out.columns:
        out["transfer_qty"] = 0
    out["transfer_qty"] = pd.to_numeric(out["transfer_qty"], errors="coerce").fillna(0).astype(int)

    return out


def prepare_center_stock_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["sku", "stock_qty"])

    out = df.copy()

    if "sku" not in out.columns:
        out["sku"] = ""
    if "stock_qty" not in out.columns:
        out["stock_qty"] = 0

    out["sku"] = out["sku"].astype(str).str.strip()
    out["stock_qty"] = pd.to_numeric(out["stock_qty"], errors="coerce").fillna(0).astype(int)

    return out


def prepare_inbound_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["sku", "inbound_date", "inbound_amount", "inbound_year_week"])

    out = df.copy()

    sku_col = None
    amount_col = None
    date_col = None

    for c in out.columns:
        cl = str(c).strip().lower()
        if cl == "sku":
            sku_col = c
        elif cl in ["inbound_amount", "qty", "quantity", "inbound_qty"]:
            amount_col = c
        elif cl in ["inbound_date", "date"]:
            date_col = c

    if sku_col is None:
        out["sku"] = ""
        sku_col = "sku"
    if amount_col is None:
        out["inbound_amount"] = 0
        amount_col = "inbound_amount"
    if date_col is None:
        out["inbound_date"] = None
        date_col = "inbound_date"

    out["sku"] = out[sku_col].astype(str).str.strip()
    out["inbound_amount"] = pd.to_numeric(out[amount_col], errors="coerce").fillna(0).astype(int)
    out["inbound_date"] = pd.to_datetime(out[date_col], errors="coerce")
    out["inbound_year_week"] = out["inbound_date"].apply(timestamp_to_year_week)

    return out[["sku", "inbound_date", "inbound_amount", "inbound_year_week"]].copy()


def prepare_weekly_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["sty", "sku", "plant", "year_week", "sale_qty", "is_forecast"])

    out = df.copy()

    for col in ["sty", "sku", "plant", "year_week"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].astype(str).str.strip()

    if "sale_qty" not in out.columns:
        out["sale_qty"] = 0
    out["sale_qty"] = pd.to_numeric(out["sale_qty"], errors="coerce").fillna(0)

    if "is_forecast" not in out.columns:
        out["is_forecast"] = False

    out["sort_key"] = out["year_week"].apply(parse_year_week_sort_key)
    return out


# =========================
# 계산 함수
# =========================
def calc_required_qty_for_target_weeks(target_weeks: float, forecast_sales: List[float]) -> int:
    target_weeks = max(0.0, float(target_weeks))
    if target_weeks <= 0:
        return 0

    total_needed = 0.0
    remain_weeks = target_weeks

    for sales in forecast_sales:
        sales = max(0.0, float(sales))

        if remain_weeks >= 1.0:
            total_needed += sales
            remain_weeks -= 1.0
        else:
            total_needed += sales * remain_weeks
            break

    return int(math.ceil(total_needed))


def find_shortage_start_year_week(current_qty: int, forecast_rows: List[Tuple[str, float]]) -> str:
    remaining = float(max(0, current_qty))

    if remaining <= 0 and forecast_rows:
        return forecast_rows[0][0]

    for year_week, sales in forecast_rows:
        sales = max(0.0, float(sales))

        if sales == 0:
            continue

        if remaining >= sales:
            remaining -= sales
        else:
            return year_week

    return ""


def build_rotation_adjustment_map(rotation_df: pd.DataFrame) -> Dict[Tuple[str, str], Dict[str, int]]:
    result: Dict[Tuple[str, str], Dict[str, int]] = {}

    if rotation_df.empty:
        return result

    for _, row in rotation_df.iterrows():
        sku = str(row.get("sku", "")).strip()
        from_plant = str(row.get("from_plant", "")).strip()
        to_plant = str(row.get("to_plant", "")).strip()
        qty = to_int(row.get("transfer_qty"), 0)

        if not sku or qty <= 0:
            continue

        if from_plant:
            key = (sku, from_plant)
            result.setdefault(key, {"in": 0, "out": 0})
            result[key]["out"] += qty

        if to_plant:
            key = (sku, to_plant)
            result.setdefault(key, {"in": 0, "out": 0})
            result[key]["in"] += qty

    return result


def build_center_current_map(center_df: pd.DataFrame) -> Dict[str, int]:
    if center_df.empty:
        return {}

    grouped = center_df.groupby("sku", as_index=False)["stock_qty"].sum()
    return {
        str(r["sku"]).strip(): int(r["stock_qty"])
        for _, r in grouped.iterrows()
    }


def build_inbound_map(inbound_df: pd.DataFrame) -> Dict[str, List[Tuple[str, int]]]:
    result: Dict[str, List[Tuple[str, int]]] = {}

    if inbound_df.empty:
        return result

    tmp = (
        inbound_df.groupby(["sku", "inbound_year_week"], as_index=False)["inbound_amount"]
        .sum()
        .sort_values(["sku", "inbound_year_week"])
    )

    for _, row in tmp.iterrows():
        sku = str(row["sku"]).strip()
        yw = str(row["inbound_year_week"]).strip()
        qty = int(row["inbound_amount"])

        if not sku or not yw or qty <= 0:
            continue

        result.setdefault(sku, []).append((yw, qty))

    return result


def get_forecast_rows_for_store(weekly_df: pd.DataFrame, sku: str, plant: str) -> List[Tuple[str, float]]:
    sub = weekly_df[
        (weekly_df["sku"] == str(sku).strip()) &
        (weekly_df["plant"] == str(plant).strip()) &
        (weekly_df["is_forecast"] == True)
    ].copy()

    if sub.empty:
        return []

    sub = sub.sort_values("sort_key")
    return [(str(r["year_week"]).strip(), float(r["sale_qty"])) for _, r in sub.iterrows()]


def cumulative_inbound_before_week(inbound_list: List[Tuple[str, int]], limit_week: str) -> int:
    if not inbound_list or not limit_week:
        return 0

    limit_key = parse_year_week_sort_key(limit_week)
    total = 0

    for yw, qty in inbound_list:
        if parse_year_week_sort_key(yw) <= limit_key:
            total += int(qty)

    return total


def allocate_center_to_shortage_rows(
    rows: List[Dict[str, Any]],
    center_current_qty: int,
    inbound_list: List[Tuple[str, int]]
) -> List[Dict[str, Any]]:
    """
    같은 SKU 내에서
    1. shortage_qty_after_rotation 큰 순
    2. shortage_start_year_week 빠른 순
    으로 센터 배정
    """
    if not rows:
        return rows

    rows = sorted(
        rows,
        key=lambda x: (
            -to_int(x["shortage_qty_after_rotation"], 0),
            parse_year_week_sort_key(x["shortage_start_year_week"])
        )
    )

    center_remaining_now = int(center_current_qty)
    inbound_remaining = [[yw, int(qty)] for yw, qty in inbound_list]

    updated_rows: List[Dict[str, Any]] = []

    for rank, row in enumerate(rows, start=1):
        need_week = str(row.get("shortage_start_year_week", "")).strip()
        need_qty = to_int(row.get("shortage_qty_after_rotation", 0), 0)

        alloc_now = 0
        alloc_inbound = 0
        ready_week = ""
        reorder_qty = 0

        if need_qty <= 0:
            row["center_alloc_qty"] = 0
            row["center_inbound_before_need_qty"] = cumulative_inbound_before_week(inbound_list, need_week)
            row["center_alloc_ready_year_week"] = ""
            row["reorder_qty"] = 0
            row["priority_rank"] = rank
            row["final_action"] = "NONE"
            row["reason"] = "회전 후 부족 없음"
            updated_rows.append(row)
            continue

        # 현재 센터재고 우선
        if center_remaining_now > 0:
            alloc_now = min(center_remaining_now, need_qty)
            center_remaining_now -= alloc_now
            need_qty -= alloc_now
            if alloc_now > 0:
                ready_week = "NOW"

        # 부족 시작 주차 이전 입고 예정분 사용
        if need_qty > 0 and need_week:
            for item in inbound_remaining:
                yw, qty = item[0], item[1]

                if qty <= 0:
                    continue
                if parse_year_week_sort_key(yw) > parse_year_week_sort_key(need_week):
                    continue

                take = min(qty, need_qty)
                if take > 0:
                    item[1] -= take
                    alloc_inbound += take
                    need_qty -= take
                    ready_week = yw

                if need_qty <= 0:
                    break

        total_center_alloc = alloc_now + alloc_inbound
        reorder_qty = max(0, need_qty)

        row["center_inbound_before_need_qty"] = cumulative_inbound_before_week(inbound_list, need_week)
        row["center_alloc_qty"] = total_center_alloc
        row["center_alloc_ready_year_week"] = ready_week
        row["reorder_qty"] = reorder_qty
        row["priority_rank"] = rank

        if row["shortage_qty_after_rotation"] <= 0:
            row["final_action"] = "NONE"
            row["reason"] = "회전 후 부족 없음"
        elif total_center_alloc > 0 and reorder_qty == 0:
            if ready_week == "NOW":
                row["final_action"] = "CENTER_ONLY"
                row["reason"] = "센터 현재재고로 부족 해소 가능"
            else:
                row["final_action"] = "WAIT_INBOUND"
                row["reason"] = "센터 입고 예정분으로 부족 해소 가능"
        elif total_center_alloc > 0 and reorder_qty > 0:
            row["final_action"] = "CENTER_AND_REORDER"
            row["reason"] = "센터 배분 후 남는 부족분은 추가 발주 필요"
        else:
            row["final_action"] = "REORDER_ONLY"
            row["reason"] = "센터 재고 및 입고 예정분으로 부족 해소 불가"

        updated_rows.append(row)

    return updated_rows


def build_inventory_action_plan_step2(
    step1_df: pd.DataFrame,
    rotation_df: pd.DataFrame,
    center_df: pd.DataFrame,
    inbound_df: pd.DataFrame,
    weekly_df: pd.DataFrame,
) -> pd.DataFrame:
    step1_df = prepare_step1_df(step1_df)
    rotation_df = prepare_rotation_df(rotation_df)
    center_df = prepare_center_stock_df(center_df)
    inbound_df = prepare_inbound_df(inbound_df)
    weekly_df = prepare_weekly_df(weekly_df)

    rotation_map = build_rotation_adjustment_map(rotation_df)
    center_map = build_center_current_map(center_df)
    inbound_map = build_inbound_map(inbound_df)

    result_rows: List[Dict[str, Any]] = []

    # 회전 반영 후 매장별 부족 계산
    for _, row in step1_df.iterrows():
        sty = str(row.get("sty", "")).strip()
        sku = str(row.get("sku", "")).strip()
        plant = str(row.get("plant", "")).strip()
        lead_time = to_float(row.get("lead_time", 0), 0.0)
        current_qty = to_int(row.get("current_qty", 0), 0)

        adj = rotation_map.get((sku, plant), {"in": 0, "out": 0})
        rotation_in_qty = int(adj.get("in", 0))
        rotation_out_qty = int(adj.get("out", 0))

        current_qty_after_rotation = max(0, current_qty + rotation_in_qty - rotation_out_qty)

        forecast_rows = get_forecast_rows_for_store(weekly_df, sku, plant)
        forecast_sales = [sales for _, sales in forecast_rows]

        shortage_target_weeks = lead_time + SAFETY_WEEKS
        shortage_required_qty = calc_required_qty_for_target_weeks(
            target_weeks=shortage_target_weeks,
            forecast_sales=forecast_sales
        )
        shortage_qty_after_rotation = max(
            0,
            int(math.ceil(shortage_required_qty - current_qty_after_rotation))
        )

        shortage_start_year_week = find_shortage_start_year_week(
            current_qty=current_qty_after_rotation,
            forecast_rows=forecast_rows
        )

        result_rows.append({
            "sty": sty,
            "sku": sku,
            "plant": plant,
            "lead_time": round(lead_time, 2),
            "current_qty_after_rotation": current_qty_after_rotation,
            "rotation_in_qty": rotation_in_qty,
            "rotation_out_qty": rotation_out_qty,
            "shortage_start_year_week": shortage_start_year_week,
            "shortage_qty_after_rotation": shortage_qty_after_rotation,
            "center_current_qty": center_map.get(sku, 0),
            "center_inbound_before_need_qty": 0,
            "center_alloc_qty": 0,
            "center_alloc_ready_year_week": "",
            "reorder_qty": 0,
            "reorder_action_year_week": "",
            "final_action": "NONE",
            "priority_rank": None,
            "reason": "",
        })

    plan_df = pd.DataFrame(result_rows)

    if plan_df.empty:
        return plan_df

    final_rows: List[Dict[str, Any]] = []

    for sku, sku_df in plan_df.groupby("sku"):
        sku_rows = sku_df.to_dict(orient="records")
        center_current_qty = center_map.get(str(sku).strip(), 0)
        inbound_list = inbound_map.get(str(sku).strip(), [])

        allocated_rows = allocate_center_to_shortage_rows(
            rows=sku_rows,
            center_current_qty=center_current_qty,
            inbound_list=inbound_list
        )

        for row in allocated_rows:
            shortage_start = str(row.get("shortage_start_year_week", "")).strip()
            reorder_qty = to_int(row.get("reorder_qty", 0), 0)
            lead_time = math.ceil(to_float(row.get("lead_time", 0), 0.0))

            if reorder_qty > 0:
                if shortage_start:
                    action_week = shift_year_week(shortage_start, -lead_time)
                    if parse_year_week_sort_key(action_week) <= parse_year_week_sort_key(current_year_week()):
                        action_week = "NOW"
                else:
                    action_week = "NOW"
            else:
                action_week = ""

            row["reorder_action_year_week"] = action_week
            final_rows.append(row)

    return pd.DataFrame(final_rows)


# =========================
# 저장
# =========================
def clear_inventory_action_plan_step2(client: Client) -> None:
    sentinel = "__never_match_sku__"
    (
        client.table(ACTION_TABLE_NAME)
        .delete()
        .neq("sku", sentinel)
        .execute()
    )


def insert_inventory_action_plan_step2(
    client: Client,
    df: pd.DataFrame,
    run_id: str,
    batch_size: int = 500
) -> int:
    if df.empty:
        return 0

    out = df.copy()
    out["run_id"] = run_id

    records = out.to_dict(orient="records")
    inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        client.table(ACTION_TABLE_NAME).insert(batch).execute()
        inserted += len(batch)

    return inserted


def rebuild_inventory_action_plan_step2(
    client: Client,
    tables: Dict[str, pd.DataFrame]
) -> Tuple[str, pd.DataFrame]:
    result_df = build_inventory_action_plan_step2(
        step1_df=tables["step1"],
        rotation_df=tables["rotation"],
        center_df=tables["center_stock"],
        inbound_df=tables["inbound_schedule"],
        weekly_df=tables["sku_weekly_forecast"],
    )

    run_id = f"ACT2-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"

    clear_inventory_action_plan_step2(client)
    insert_inventory_action_plan_step2(client, result_df, run_id)

    return run_id, result_df


# =========================
# 화면
# =========================
def main():
    st.title("inventory_action_plan_step2 생성기")

    st.write("회전 결과 + center_stock + inbound_schedule을 반영해 최종 액션 계획을 계산하고 저장합니다.")

    with st.expander("계산 기준", expanded=True):
        st.markdown(
            """
- 회전 후 매장 재고 = `step1 current_qty + 회전받은수량 - 회전보낸수량`
- 부족 시작 주차 = `sku_weekly_forecast` 미래 판매량 기준으로 재고가 처음 부족해지는 주차
- 센터는 `부족수량이 큰 매장부터` 우선 배정
- 센터 현재재고를 먼저 사용
- 부족 시작 주차 전까지 들어오는 `inbound_schedule` 예정분도 사용
- 그래도 부족하면 `reorder_qty` 계산
- 리오더 시점 = `부족 시작 주차 - 리드타임`
            """
        )

    if st.button("inventory_action_plan_step2 계산 후 Supabase 저장", type="primary"):
        tables = load_source_tables()
        st.session_state["tables_action2"] = tables

        client = get_supabase_client()
        run_id, result_df = rebuild_inventory_action_plan_step2(client, tables)

        st.session_state["action2_df"] = result_df
        st.session_state["action2_run_id"] = run_id

        st.success(f"{ACTION_TABLE_NAME} 저장 완료: {len(result_df):,}건 / run_id={run_id}")

    tables = st.session_state.get("tables_action2")
    if tables:
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("step1 행수", len(tables["step1"]))
        c2.metric("rotation 행수", len(tables["rotation"]))
        c3.metric("center_stock 행수", len(tables["center_stock"]))
        c4.metric("inbound_schedule 행수", len(tables["inbound_schedule"]))
        c5.metric("sku_weekly_forecast 행수", len(tables["sku_weekly_forecast"]))

    result_df = st.session_state.get("action2_df")
    if result_df is not None:
        st.subheader("계산 결과 미리보기")
        st.dataframe(result_df, use_container_width=True)

        csv = result_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="CSV 다운로드",
            data=csv,
            file_name="inventory_action_plan_step2.csv",
            mime="text/csv"
        )


if __name__ == "__main__":
    main()
