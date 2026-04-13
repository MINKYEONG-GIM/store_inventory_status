import math
import os
import traceback
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

try:
    from supabase import create_client as _create_supabase_client
except ImportError:
    _create_supabase_client = None


# -----------------------------
# 공통 오류 표시
# -----------------------------
def show_detailed_exception(err: BaseException, title: str = "오류가 발생했습니다") -> None:
    st.error(title)
    st.markdown(f"**예외 종류:** `{type(err).__name__}`")
    st.code(str(err) if str(err) else "(메시지 없음)", language="text")
    tb = traceback.format_exc()
    with st.expander("전체 스택 트레이스", expanded=True):
        st.code(tb, language="text")
    print(f"[{type(err).__name__}] {err}\n{tb}", flush=True)


# -----------------------------
# Supabase 연결
# -----------------------------
def get_supabase_client():
    if _create_supabase_client is None:
        return None

    url = ""
    key = ""

    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            sec = dict(st.secrets["supabase"])
            url = str(sec.get("url") or "").strip()
            key = str(
                sec.get("service_role_key")
                or sec.get("key")
                or sec.get("anon_key")
                or ""
            ).strip()
    except Exception:
        pass

    try:
        if not url:
            url = str(st.secrets.get("SUPABASE_URL") or "").strip()
        if not key:
            key = str(
                st.secrets.get("SUPABASE_SERVICE_ROLE_KEY")
                or st.secrets.get("SUPABASE_KEY")
                or st.secrets.get("SUPABASE_ANON_KEY")
                or ""
            ).strip()
    except Exception:
        pass

    if not url:
        url = (os.getenv("SUPABASE_URL") or "").strip()
    if not key:
        key = (
            os.getenv("SUPABASE_SERVICE_ROLE_KEY")
            or os.getenv("SUPABASE_KEY")
            or os.getenv("SUPABASE_ANON_KEY")
            or ""
        ).strip()

    if not url or not key:
        return None

    return _create_supabase_client(url, key)


# -----------------------------
# 테이블명 설정
# -----------------------------
def get_step1_table_name() -> str:
    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            v = st.secrets["supabase"].get("store_inventory_status_step1_table")
            if v is not None and str(v).strip():
                return str(v).strip()
    except Exception:
        pass
    return (os.getenv("SUPABASE_STORE_INVENTORY_STATUS_STEP1_TABLE") or "store_inventory_status_step1").strip()


def get_center_stock_table_name() -> str:
    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            v = st.secrets["supabase"].get("center_stock_table")
            if v is not None and str(v).strip():
                return str(v).strip()
    except Exception:
        pass
    return (os.getenv("SUPABASE_CENTER_STOCK_TABLE") or "center_stock").strip()


def get_weekly_stock_table_name() -> str:
    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            v = st.secrets["supabase"].get("weekly_stock_table")
            if v is not None and str(v).strip():
                return str(v).strip()
    except Exception:
        pass
    return (os.getenv("SUPABASE_WEEKLY_STOCK_TABLE") or "weekly_stock").strip()


def get_step2_table_name() -> str:
    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            v = st.secrets["supabase"].get("store_inventory_status_step2_table")
            if v is not None and str(v).strip():
                return str(v).strip()
    except Exception:
        pass
    return (os.getenv("SUPABASE_STORE_INVENTORY_STATUS_STEP2_TABLE") or "store_inventory_status_step2").strip()


# -----------------------------
# 유틸
# -----------------------------
def _to_float(v: Any) -> float:
    x = pd.to_numeric(v, errors="coerce")
    if pd.isna(x):
        return 0.0
    return float(x)


def _first_existing_col(df: pd.DataFrame, candidates: List[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    return ""


def _weekly_loss_source_col(weekly_df: pd.DataFrame) -> str:
    """
    weekly_stock에서 주간 loss에 해당하는 컬럼명.
    loss / total_loss 우선, 없으면 sale_qty 등으로 대체(스키마별 이름 차이 대응).
    """
    loss_col = _first_existing_col(
        weekly_df,
        ["loss", "LOSS", "total_loss", "TOTAL_LOSS", "weekly_loss", "WEEKLY_LOSS"],
    )
    if loss_col:
        return loss_col
    return _first_existing_col(weekly_df, ["sale_qty", "SALE_QTY"])


def _year_week_to_week_start(year_week: Any) -> pd.Timestamp:
    """
    year_week 예:
    - '202615'
    - '2026-15'
    - '2026_15'
    - '26W15' 같은 형식은 미지원
    반환값: 해당 ISO week의 월요일 날짜
    """
    s = str(year_week or "").strip()
    if not s:
        return pd.NaT

    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) == 6:
        year = int(digits[:4])
        week = int(digits[4:6])
    elif len(digits) == 4:
        year = 2000 + int(digits[:2])
        week = int(digits[2:4])
    else:
        return pd.NaT

    try:
        return pd.Timestamp.fromisocalendar(year, week, 1)
    except Exception:
        return pd.NaT


def fetch_supabase_table_all_rows(client, table_name: str, batch_size: int = 1000) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    off = 0

    while True:
        try:
            resp = (
                client.table(table_name)
                .select("*")
                .limit(batch_size)
                .offset(off)
                .execute()
            )
        except Exception:
            resp = (
                client.table(table_name)
                .select("*")
                .range(off, off + batch_size - 1)
                .execute()
            )

        chunk = resp.data if resp.data else []
        if not chunk:
            break

        rows.extend(chunk)

        if len(chunk) < batch_size:
            break

        off += batch_size

    return rows


def clear_table_all_rows(client, table_name: str, key_col: str = "id") -> None:
    client.table(table_name).delete().gte(key_col, 0).execute()


def bulk_insert_rows(client, table_name: str, rows: List[Dict[str, Any]], batch_size: int = 200) -> int:
    if not rows:
        return 0

    total = 0
    tbl = client.table(table_name)

    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        tbl.insert(chunk).execute()
        total += len(chunk)

    return total


# -----------------------------
# weekly_stock에서 shortage_start_week 계산
# -----------------------------
def build_shortage_start_week_map(
    weekly_rows: List[Dict[str, Any]],
    center_rows: List[Dict[str, Any]],
) -> pd.DataFrame:
    if not weekly_rows:
        return pd.DataFrame(columns=["sku", "shortage_start_week"])

    weekly_df = pd.DataFrame(weekly_rows)
    center_df = pd.DataFrame(center_rows) if center_rows else pd.DataFrame()

    # weekly_stock 컬럼 찾기
    weekly_sku_col = _first_existing_col(weekly_df, ["sku", "SKU"])
    year_week_col = _first_existing_col(weekly_df, ["year_week", "YEAR_WEEK"])
    cumulative_loss_col = _first_existing_col(weekly_df, ["cumulative_loss", "CUMULATIVE_LOSS"])
    loss_col = _weekly_loss_source_col(weekly_df)

    if not weekly_sku_col or not year_week_col or (not cumulative_loss_col and not loss_col):
        return pd.DataFrame(columns=["sku", "shortage_start_week"])

    weekly_df["sku_norm"] = weekly_df[weekly_sku_col].fillna("").astype(str).str.strip()
    weekly_df = weekly_df[weekly_df["sku_norm"] != ""].copy()

    weekly_df["week_start"] = weekly_df[year_week_col].apply(_year_week_to_week_start)
    weekly_df = weekly_df.dropna(subset=["week_start"]).copy()

    if cumulative_loss_col:
        weekly_df["cumulative_loss_num"] = weekly_df[cumulative_loss_col].apply(_to_float)

        # sku + week 기준 cumulative_loss 대표값(중복 행이 있으면 최대값 사용)
        wk = (
            weekly_df.groupby(["sku_norm", "week_start"], as_index=False)
            .agg(cumulative_loss=("cumulative_loss_num", "max"))
            .sort_values(["sku_norm", "week_start"])
            .reset_index(drop=True)
        )
    else:
        weekly_df["loss_num"] = weekly_df[loss_col].apply(_to_float)

        # sku + week 기준 loss 합계
        wk = (
            weekly_df.groupby(["sku_norm", "week_start"], as_index=False)
            .agg(loss=("loss_num", "sum"))
            .sort_values(["sku_norm", "week_start"])
            .reset_index(drop=True)
        )

    # center_stock에서 sku별 총 센터재고 계산
    if center_df.empty:
        center_agg = pd.DataFrame(columns=["sku", "total_center_stock"])
    else:
        center_sku_col = _first_existing_col(center_df, ["sku", "SKU"])
        center_stock_col = _first_existing_col(center_df, ["stock_qty", "STOCK_QTY", "stock"])

        if not center_sku_col or not center_stock_col:
            center_agg = pd.DataFrame(columns=["sku", "total_center_stock"])
        else:
            center_df["sku_norm"] = center_df[center_sku_col].fillna("").astype(str).str.strip()
            center_df = center_df[center_df["sku_norm"] != ""].copy()
            center_df["center_stock_qty_num"] = center_df[center_stock_col].apply(_to_float)

            center_agg = (
                center_df.groupby("sku_norm", as_index=False)
                .agg(total_center_stock=("center_stock_qty_num", "sum"))
                .rename(columns={"sku_norm": "sku"})
            )

    wk = wk.merge(center_agg, how="left", left_on="sku_norm", right_on="sku")
    wk["total_center_stock"] = wk["total_center_stock"].fillna(0.0)

    # 누적 loss 준비: weekly_stock에 cumulative_loss가 없으면 직접 계산
    if "cumulative_loss" not in wk.columns:
        wk["cumulative_loss"] = wk.groupby("sku_norm")["loss"].cumsum()

    # cumulative_loss > total_center_stock 가 처음 성립하는 주
    crossed = wk[wk["cumulative_loss"] > wk["total_center_stock"]].copy()
    if crossed.empty:
        return pd.DataFrame(columns=["sku", "shortage_start_week"])

    first_cross = (
        crossed.groupby("sku_norm", as_index=False)
        .agg(shortage_start_week=("week_start", "min"))
        .rename(columns={"sku_norm": "sku"})
    )

    return first_cross


def _weekly_sku_loss_frame(weekly_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    weekly_stock에서 sku별 주차·loss를 정규화한 프레임.
    loss/total_loss 등이 없고 sale_qty 등 대체 컬럼도 없으면 빈 프레임 반환.
    """
    if not weekly_rows:
        return pd.DataFrame(columns=["sku_norm", "week_start", "loss"])

    weekly_df = pd.DataFrame(weekly_rows)
    weekly_sku_col = _first_existing_col(weekly_df, ["sku", "SKU"])
    year_week_col = _first_existing_col(weekly_df, ["year_week", "YEAR_WEEK"])
    loss_col = _weekly_loss_source_col(weekly_df)

    if not weekly_sku_col or not year_week_col or not loss_col:
        return pd.DataFrame(columns=["sku_norm", "week_start", "loss"])

    weekly_df["sku_norm"] = weekly_df[weekly_sku_col].fillna("").astype(str).str.strip()
    weekly_df = weekly_df[weekly_df["sku_norm"] != ""].copy()
    weekly_df["week_start"] = weekly_df[year_week_col].apply(_year_week_to_week_start)
    weekly_df = weekly_df.dropna(subset=["week_start"]).copy()
    weekly_df["loss"] = weekly_df[loss_col].apply(_to_float)

    out = weekly_df[["sku_norm", "week_start", "loss"]].copy()
    return out


# -----------------------------
# step2 계산
# -----------------------------
def build_step2_rows(
    step1_rows: List[Dict[str, Any]],
    center_rows: List[Dict[str, Any]],
    weekly_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not step1_rows:
        return []

    step1_df = pd.DataFrame(step1_rows)
    center_df = pd.DataFrame(center_rows) if center_rows else pd.DataFrame()

    sku_col = _first_existing_col(step1_df, ["sku", "SKU"])
    style_col = _first_existing_col(step1_df, ["style_code", "style", "STYLE_CODE"])
    shortage_col = _first_existing_col(step1_df, ["shortage_qty", "SHORTAGE_QTY"])
    surplus_col = _first_existing_col(step1_df, ["surplus_qty", "SURPLUS_QTY"])
    lead_time_col = _first_existing_col(step1_df, ["lead_time", "LEAD_TIME"])

    if not sku_col:
        step1_df["sku"] = None
        sku_col = "sku"
    if not style_col:
        step1_df["style_code"] = None
        style_col = "style_code"
    if not shortage_col:
        step1_df["shortage_qty"] = 0
        shortage_col = "shortage_qty"
    if not surplus_col:
        step1_df["surplus_qty"] = 0
        surplus_col = "surplus_qty"
    if not lead_time_col:
        step1_df["lead_time"] = 0
        lead_time_col = "lead_time"

    step1_df["sku_norm"] = step1_df[sku_col].fillna("").astype(str).str.strip()
    step1_df = step1_df[step1_df["sku_norm"] != ""].copy()

    step1_df["style_code_norm"] = step1_df[style_col].fillna("").astype(str).str.strip()
    step1_df["shortage_qty_num"] = step1_df[shortage_col].apply(_to_float)
    step1_df["surplus_qty_num"] = step1_df[surplus_col].apply(_to_float)
    step1_df["lead_time_num"] = step1_df[lead_time_col].apply(_to_float)

    step1_agg = (
        step1_df.groupby("sku_norm", as_index=False)
        .agg(
            style_code=("style_code_norm", lambda s: next((x for x in s if str(x).strip()), "")),
            shortage_qty=("shortage_qty_num", "sum"),
            surplus_qty=("surplus_qty_num", "sum"),
            shortage_store_count=("shortage_qty_num", lambda s: int((s > 0).sum())),
            lead_time=("lead_time_num", "max"),
        )
        .rename(columns={"sku_norm": "sku"})
    )

    if center_df.empty:
        center_agg = pd.DataFrame(columns=["sku", "center_stock_qty"])
    else:
        center_sku_col = _first_existing_col(center_df, ["sku", "SKU"])
        center_stock_col = _first_existing_col(center_df, ["stock_qty", "STOCK_QTY", "stock"])

        if not center_sku_col:
            center_df["sku"] = None
            center_sku_col = "sku"
        if not center_stock_col:
            center_df["stock_qty"] = 0
            center_stock_col = "stock_qty"

        center_df["sku_norm"] = center_df[center_sku_col].fillna("").astype(str).str.strip()
        center_df = center_df[center_df["sku_norm"] != ""].copy()
        center_df["center_stock_qty_num"] = center_df[center_stock_col].apply(_to_float)

        center_agg = (
            center_df.groupby("sku_norm", as_index=False)
            .agg(center_stock_qty=("center_stock_qty_num", "sum"))
            .rename(columns={"sku_norm": "sku"})
        )

    shortage_week_agg = build_shortage_start_week_map(weekly_rows, center_rows)

    merged = step1_agg.merge(center_agg, how="left", on="sku")
    merged = merged.merge(shortage_week_agg, how="left", on="sku")
    merged["center_stock_qty"] = merged["center_stock_qty"].fillna(0.0)

    wk_loss = _weekly_sku_loss_frame(weekly_rows)
    avg_weekly_loss_by_sku = (
        wk_loss.groupby("sku_norm", as_index=True)["loss"].mean()
        if not wk_loss.empty
        else pd.Series(dtype=float)
    )

    out: List[Dict[str, Any]] = []

    for _, r in merged.iterrows():
        shortage_qty = _to_float(r["shortage_qty"])
        surplus_qty = _to_float(r["surplus_qty"])
        center_stock_qty = _to_float(r["center_stock_qty"])
        lead_time = int(math.ceil(max(0.0, _to_float(r["lead_time"]))))

        remain_qty = shortage_qty - surplus_qty - center_stock_qty
        current_shortage_qty = max(0, int(math.ceil(remain_qty)))
        reorder_needed = remain_qty > 0

        if shortage_qty <= 0:
            reorder_urgency = "불필요"
        elif remain_qty <= 0:
            reorder_urgency = "센터출고"
        else:
            reorder_urgency = "발주필요"

        shortage_start_week = pd.to_datetime(r.get("shortage_start_week"), errors="coerce")

        sku_key = str(r["sku"]).strip()

        # total_reorder_amount: 부족 시작 주 이후 weekly_stock.loss 합(데이터에 있는 구간 = 성숙기까지 예측 판매 프록시)
        total_reorder_amount: Optional[float] = None
        if not wk_loss.empty and pd.notna(shortage_start_week):
            ssw = pd.Timestamp(shortage_start_week).normalize()
            sub = wk_loss[(wk_loss["sku_norm"] == sku_key) & (wk_loss["week_start"] >= ssw)]
            total_reorder_amount = float(sub["loss"].sum())

        # due_date_reorder_amount: (리드타임 일수 + 안전 4주) 동안의 예상 판매량 = (lead_time/7 + 4) * 주간 평균 loss
        due_date_reorder_amount: Optional[float] = None
        if sku_key in avg_weekly_loss_by_sku.index and pd.notna(avg_weekly_loss_by_sku[sku_key]):
            avg_w = float(avg_weekly_loss_by_sku[sku_key])
            weeks_cover = (float(lead_time) / 7.0) + 4.0
            due_date_reorder_amount = max(0.0, weeks_cover * avg_w)

        if pd.isna(shortage_start_week):
            order_due_date: Optional[str] = None
            shortage_start_week_value: Optional[str] = None
        else:
            safety_days = 14  # 안전주수 2주
            order_due_date_ts = shortage_start_week - pd.Timedelta(days=lead_time + safety_days)
            order_due_date = order_due_date_ts.date().isoformat()
            shortage_start_week_value = shortage_start_week.date().isoformat()

        out.append(
            {
                "style_code": str(r["style_code"]).strip() if str(r["style_code"]).strip() else "",
                "sku": str(r["sku"]).strip(),
                "current_shortage_qty": int(current_shortage_qty),
                "shortage_store_count": int(r["shortage_store_count"]),
                "lead_time": float(lead_time),
                "reorder_needed": bool(reorder_needed),
                "reorder_urgency": reorder_urgency,
                "order_due_date": order_due_date,
                "center_stock_qty": float(center_stock_qty),
                "surplus_qty": float(surplus_qty),
                "shortage_qty": float(shortage_qty),
                "shortage_start_week": shortage_start_week_value,
                "total_reorder_amount": total_reorder_amount,
                "due_date_reorder_amount": due_date_reorder_amount,
            }
        )

    out.sort(key=lambda x: (x.get("sku") or ""))
    return out


def load_step2() -> Dict[str, Any]:
    client = get_supabase_client()
    if client is None:
        raise RuntimeError("Supabase 연결 불가: SUPABASE_URL / SUPABASE_KEY 설정을 확인하세요.")

    step1_table = get_step1_table_name()
    center_table = get_center_stock_table_name()
    weekly_table = get_weekly_stock_table_name()
    step2_table = get_step2_table_name()

    step1_rows = fetch_supabase_table_all_rows(client, step1_table)
    center_rows = fetch_supabase_table_all_rows(client, center_table)
    weekly_rows = fetch_supabase_table_all_rows(client, weekly_table)

    result_rows = build_step2_rows(step1_rows, center_rows, weekly_rows)

    clear_table_all_rows(client, step2_table)
    inserted = bulk_insert_rows(client, step2_table, result_rows)

    sample = []
    try:
        resp = (
            client.table(step2_table)
            .select(
                "sku, current_shortage_qty, shortage_start_week, order_due_date, "
                "center_stock_qty, surplus_qty, shortage_qty, total_reorder_amount, due_date_reorder_amount"
            )
            .limit(10)
            .execute()
        )
        sample = resp.data if resp and getattr(resp, "data", None) else []
    except Exception:
        sample = []

    return {
        "step1_rows": len(step1_rows),
        "center_rows": len(center_rows),
        "weekly_rows": len(weekly_rows),
        "inserted_rows": inserted,
        "sample_rows": sample,
    }


def main():
    st.set_page_config(page_title="step2 loader", layout="centered")

    st.markdown(
        """
        <style>
        header, footer, [data-testid="stToolbar"], [data-testid="stDecoration"] {
            display: none !important;
        }
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
            max-width: 320px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if st.button("데이터 쌓기", use_container_width=True):
        try:
            with st.spinner("적재 중..."):
                r = load_step2()
            st.success(
                f"완료: step1 {r['step1_rows']:,}행, "
                f"center {r['center_rows']:,}행 기준, "
                f"weekly {r['weekly_rows']:,}행 기준, "
                f"step2 {r['inserted_rows']:,}행 저장"
            )
            if r.get("sample_rows"):
                st.markdown("**적재 결과 샘플(최대 10행)**")
                st.dataframe(pd.DataFrame(r["sample_rows"]), use_container_width=True)
        except Exception as e:
            show_detailed_exception(e, title="적재 실패")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        show_detailed_exception(e, title="앱 실행 중 오류")
