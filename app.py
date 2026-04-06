import math
from typing import Any, Dict, List, Tuple

import pandas as pd
import streamlit as st
from supabase import create_client, Client


# =========================
# 고정 설정값
# =========================
SAFETY_WEEKS: float = 1.0


# =========================
# Streamlit 기본 설정
# =========================
st.set_page_config(
    page_title="Store Inventory Status Step1",
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
    """
    예: '2026-14' -> (2026, 14)
    """
    try:
        s = str(year_week).strip()
        y, w = s.split("-")
        return int(y), int(w)
    except Exception:
        return (0, 0)


def fetch_all_rows(
    client: Client,
    table_name: str,
    select_cols: str = "*",
    page_size: int = 1000
) -> pd.DataFrame:
    """
    Supabase 전체 테이블 읽기
    """
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
# 판매주수/필요수량 계산
# =========================
def calc_stock_weeks_from_forecast(
    current_qty: int,
    forecast_sales: List[float]
) -> float:
    """
    현재재고를 미래 주차 판매예측으로 순서대로 차감해서
    몇 주 버티는지 계산
    """
    remaining = float(max(0, current_qty))
    weeks = 0.0

    if remaining <= 0:
        return 0.0

    if not forecast_sales:
        return 999.0

    for sales in forecast_sales:
        sales = max(0.0, float(sales))

        if sales == 0:
            weeks += 1.0
            continue

        if remaining >= sales:
            remaining -= sales
            weeks += 1.0
        else:
            weeks += remaining / sales
            return round(weeks, 2)

    # 예측 구간 전체를 다 버틴 경우
    # 충분히 긴 재고라는 의미로 예측 길이만큼 반환
    return round(weeks, 2)


def calc_required_qty_for_target_weeks(
    target_weeks: float,
    forecast_sales: List[float]
) -> int:
    """
    특정 주수(target_weeks) 동안 버티기 위해 필요한 총 수량 계산
    """
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
            remain_weeks = 0.0
            break

    return int(math.ceil(total_needed))


# =========================
# 원천 데이터 로딩
# =========================
@st.cache_data(ttl=300)
def load_source_tables() -> Dict[str, pd.DataFrame]:
    client = get_supabase_client()

    tables = {
        "center_stock": fetch_all_rows(
            client,
            "center_stock",
            "*"
        ),
        "inbound_schedule": fetch_all_rows(
            client,
            "inbound_schedule",
            "*"
        ),
        "reorder": fetch_all_rows(
            client,
            "reorder",
            "*"
        ),
        "sku_forecast_run": fetch_all_rows(
            client,
            "sku_forecast_run",
            "*"
        ),
        "sku_monthly_forecast": fetch_all_rows(
            client,
            "sku_monthly_forecast",
            "*"
        ),
        "sku_weekly_forecast": fetch_all_rows(
            client,
            "sku_weekly_forecast",
            "sty,sku,plant,store_name,year_week,sale_qty,is_forecast,begin_stock"
        ),
    }

    return tables


# =========================
# 데이터 전처리
# =========================
def prepare_weekly_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()

    text_cols = ["sty", "sku", "plant", "store_name", "year_week"]
    for col in text_cols:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].astype(str).str.strip()

    num_cols = ["sale_qty", "begin_stock"]
    for col in num_cols:
        if col not in out.columns:
            out[col] = 0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)

    if "is_forecast" not in out.columns:
        out["is_forecast"] = False

    out["sort_key"] = out["year_week"].apply(parse_year_week_sort_key)
    return out


def prepare_reorder_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["sku", "lead_time"])

    out = df.copy()

    if "sku" not in out.columns:
        out["sku"] = ""
    if "lead_time" not in out.columns:
        out["lead_time"] = 0

    out["sku"] = out["sku"].astype(str).str.strip()
    out["lead_time"] = pd.to_numeric(out["lead_time"], errors="coerce").fillna(0)

    return out


def prepare_forecast_run_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["sku", "plant", "sty"])

    out = df.copy()

    for col in ["sku", "plant"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].astype(str).str.strip()

    if "style_code" not in out.columns:
        out["style_code"] = ""
    out["style_code"] = out["style_code"].astype(str).str.strip()

    return out


# =========================
# step1 계산
# =========================
def build_store_inventory_status_step1(
    weekly_df: pd.DataFrame,
    reorder_df: pd.DataFrame,
    forecast_run_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    store_inventory_status_step1 적재용 데이터 생성
    """

    if weekly_df.empty:
        return pd.DataFrame(columns=[
            "sty", "sku", "plant",
            "store_classification",
            "lead_time", "current_qty", "stock_weeks",
            "shortage_qty", "surplus_qty"
        ])

    weekly_df = prepare_weekly_df(weekly_df)
    reorder_df = prepare_reorder_df(reorder_df)
    forecast_run_df = prepare_forecast_run_df(forecast_run_df)

    lead_time_map = (
        reorder_df.drop_duplicates(subset=["sku"])
        .set_index("sku")["lead_time"]
        .to_dict()
    )

    # sku_forecast_run에서 style_code fallback
    style_map: Dict[Tuple[str, str], str] = {}
    if not forecast_run_df.empty:
        for _, row in forecast_run_df.iterrows():
            key = (str(row.get("sku", "")).strip(), str(row.get("plant", "")).strip())
            val = str(row.get("style_code", "")).strip()
            if key[0] and val:
                style_map[key] = val

    result_rows: List[Dict[str, Any]] = []

    group_cols = ["sku", "plant"]

    for (sku, plant), g in weekly_df.groupby(group_cols):
        g = g.sort_values("sort_key").reset_index(drop=True)

        actual_df = g[g["is_forecast"] == False].copy()
        forecast_df = g[g["is_forecast"] == True].copy().sort_values("sort_key")

        # sty 결정
        sty_candidates = g["sty"].dropna().astype(str).str.strip()
        sty_candidates = sty_candidates[sty_candidates != ""]
        if len(sty_candidates) > 0:
            sty = sty_candidates.iloc[0]
        else:
            sty = style_map.get((str(sku).strip(), str(plant).strip()), "")

        # 현재재고: 마지막 실적 주차의 begin_stock
        if actual_df.empty:
            current_qty = 0
        else:
            current_qty = to_int(actual_df.iloc[-1]["begin_stock"])

        forecast_sales = forecast_df["sale_qty"].astype(float).tolist()

        # 리드타임(일 -> 주)
        lead_time_days = float(lead_time_map.get(str(sku).strip(), 0.0))
        lead_time_weeks = round(lead_time_days / 7.0, 2) if lead_time_days > 0 else 0.0

        # 판매주수(=재고주수)
        stock_weeks = calc_stock_weeks_from_forecast(
            current_qty=current_qty,
            forecast_sales=forecast_sales
        )

        # 부족 수량: lead_time + 1주까지 버티기 위한 부족량
        shortage_target_weeks = lead_time_weeks + SAFETY_WEEKS
        shortage_required_qty = calc_required_qty_for_target_weeks(
            target_weeks=shortage_target_weeks,
            forecast_sales=forecast_sales
        )
        shortage_qty = max(0, int(math.ceil(shortage_required_qty - current_qty)))

        # 여유 수량: lead_time + 3주 초과분
        surplus_target_weeks = lead_time_weeks + 3.0
        surplus_required_qty = calc_required_qty_for_target_weeks(
            target_weeks=surplus_target_weeks,
            forecast_sales=forecast_sales
        )
        surplus_qty = max(0, int(math.floor(current_qty - surplus_required_qty)))

        # 분류
        if stock_weeks <= (lead_time_weeks + SAFETY_WEEKS):
            store_classification = "부족매장"
        elif stock_weeks > (lead_time_weeks + 3.0):
            store_classification = "여유매장"
        else:
            store_classification = "유지매장"

        result_rows.append({
            "sty": sty,
            "sku": str(sku).strip(),
            "plant": str(plant).strip(),
            "store_classification": store_classification,
            "lead_time": round(lead_time_weeks, 2),
            "current_qty": current_qty,
            "stock_weeks": round(stock_weeks, 2),
            "shortage_qty": shortage_qty,
            "surplus_qty": surplus_qty,
        })

    return pd.DataFrame(result_rows)


# =========================
# step1 저장
# =========================
def clear_store_inventory_status_step1(client: Client) -> None:
    sentinel = "__never_match_sku__"
    (
        client.table("store_inventory_status_step1")
        .delete()
        .neq("sku", sentinel)
        .execute()
    )


def insert_store_inventory_status_step1(
    client: Client,
    df: pd.DataFrame,
    batch_size: int = 500
) -> int:
    if df.empty:
        return 0

    records = df.to_dict(orient="records")
    inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        client.table("store_inventory_status_step1").insert(batch).execute()
        inserted += len(batch)

    return inserted


# =========================
# 화면
# =========================
def main():
    st.title("store_inventory_status_step1 생성기")

    st.write("기존 Supabase 테이블을 읽어 store_inventory_status_step1 값을 계산하고 저장합니다.")

    with st.expander("이번 step1 계산 기준", expanded=True):
        st.markdown(
            """
- 현재재고: `sku_weekly_forecast`의 마지막 실적 주차 `begin_stock`
- 판매주수(=재고주수): 미래 주차 `sale_qty`를 순서대로 차감해서 계산
- 부족매장: `stock_weeks <= lead_time + 1`
- 여유매장: `stock_weeks > lead_time + 3`
- 유지매장: 그 사이
- shortage_qty: `lead_time + 1주`까지 버티기 위해 부족한 수량
- surplus_qty: `lead_time + 3주` 초과분
            """
        )

    if st.button("원천 테이블 불러오기"):
        tables = load_source_tables()
        st.session_state["tables"] = tables
        st.success("원천 테이블을 불러왔습니다.")

    tables = st.session_state.get("tables")

    if tables:
        c1, c2, c3 = st.columns(3)
        c1.metric("sku_weekly_forecast 행수", len(tables["sku_weekly_forecast"]))
        c2.metric("reorder 행수", len(tables["reorder"]))
        c3.metric("sku_forecast_run 행수", len(tables["sku_forecast_run"]))

        with st.expander("sku_weekly_forecast 미리보기"):
            st.dataframe(tables["sku_weekly_forecast"].head(30), use_container_width=True)

        if st.button("step1 계산하기", type="primary"):
            result_df = build_store_inventory_status_step1(
                weekly_df=tables["sku_weekly_forecast"],
                reorder_df=tables["reorder"],
                forecast_run_df=tables["sku_forecast_run"],
            )
            st.session_state["step1_df"] = result_df
            st.success(f"계산 완료: {len(result_df):,}건")

    step1_df = st.session_state.get("step1_df")

    if step1_df is not None:
        st.subheader("계산 결과 미리보기")
        st.dataframe(step1_df, use_container_width=True)

        col1, col2 = st.columns(2)

        with col1:
            if st.button("store_inventory_status_step1 전체 삭제 후 다시 저장"):
                client = get_supabase_client()
                clear_store_inventory_status_step1(client)
                inserted = insert_store_inventory_status_step1(client, step1_df)
                st.success(f"{inserted:,}건 저장 완료")

        with col2:
            csv = step1_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                label="CSV 다운로드",
                data=csv,
                file_name="store_inventory_status_step1.csv",
                mime="text/csv"
            )


if __name__ == "__main__":
    main()
