import math
import os
import re
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


def get_sku_weekly_forecast_2_table_name() -> str:
    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            v = st.secrets["supabase"].get("sku_weekly_forecast_2_table")
            if v is not None and str(v).strip():
                return str(v).strip()
    except Exception:
        pass
    return (os.getenv("SUPABASE_SKU_WEEKLY_FORECAST_2_TABLE") or "sku_weekly_forecast_2").strip()


def get_sku_weekly_forecast_table_name() -> str:
    """step2 total_sale_qty 집계용 예측 테이블. 기본은 sku_weekly_forecast (sum(SALE_QTY) 기준과 동일)."""
    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            v = st.secrets["supabase"].get("sku_weekly_forecast_table")
            if v is not None and str(v).strip():
                return str(v).strip()
    except Exception:
        pass
    return (os.getenv("SUPABASE_SKU_WEEKLY_FORECAST_TABLE") or "sku_weekly_forecast").strip()


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


def parse_style_codes(text: str) -> List[str]:
    if not text:
        return []

    separators = [",", "\n", "\t", ";", "|"]
    normalized = text
    for sep in separators:
        normalized = normalized.replace(sep, ",")

    values: List[str] = []
    seen = set()

    for x in normalized.split(","):
        v = str(x).strip()
        if not v:
            continue
        if v not in seen:
            seen.add(v)
            values.append(v)

    return values


def filter_rows_by_style_codes(
    rows: List[Dict[str, Any]],
    style_codes: List[str],
) -> List[Dict[str, Any]]:
    if not rows or not style_codes:
        return rows

    df = pd.DataFrame(rows)
    if df.empty:
        return rows

    style_col = _first_existing_col(df, ["style_code", "style", "STYLE_CODE"])
    if not style_col:
        return rows

    wanted = {str(x).strip() for x in style_codes if str(x).strip()}
    if not wanted:
        return rows

    df[style_col] = df[style_col].fillna("").astype(str).str.strip()
    df = df[df[style_col].isin(wanted)].copy()

    return df.to_dict(orient="records")


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
        try:
            tbl.insert(chunk).execute()
        except Exception as e:
            # PostgREST schema cache mismatch(없는 컬럼) 방어:
            # 예: "Could not find the '월물' column ..."
            msg = ""
            try:
                msg = str(getattr(e, "args", [""])[0] or str(e))
            except Exception:
                msg = str(e)

            m = re.search(r"Could not find the '([^']+)' column", msg)
            if not m:
                raise

            missing_col = m.group(1)
            cleaned = []
            for r in chunk:
                if isinstance(r, dict) and missing_col in r:
                    rr = dict(r)
                    rr.pop(missing_col, None)
                    cleaned.append(rr)
                else:
                    cleaned.append(r)

            tbl.insert(cleaned).execute()
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
    wk["total_center_stock"] = wk["total_center_stock"].fillna(0.0).clip(lower=0.0)

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
def _forecast_total_sale_agg(forecast_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """예측 테이블(sku_weekly_forecast 등)에서 sku별 SALE_QTY(또는 동일 의미 컬럼) 합계."""
    if not forecast_rows:
        return pd.DataFrame(columns=["sku", "total_sale_qty"])

    forecast_df = pd.DataFrame(forecast_rows)
    sku_col = _first_existing_col(forecast_df, ["sku", "SKU"])
    # sum(SALE_QTY)와 동일하게 맞추려면 주간 행 기준 수량 컬럼을 우선 (total_sale_qty 컬럼명 충돌 방지)
    qty_col = _first_existing_col(
        forecast_df,
        [
            "SALE_QTY",
            "sale_qty",
            "weekly_sale_qty",
            "forecast_sale_qty",
            "sold_qty",
            "qty",
            "total_sale_qty",
        ],
    )
    if not sku_col or not qty_col:
        return pd.DataFrame(columns=["sku", "total_sale_qty"])

    forecast_df["sku_norm"] = forecast_df[sku_col].fillna("").astype(str).str.strip()
    forecast_df = forecast_df[forecast_df["sku_norm"] != ""].copy()
    forecast_df["qty_num"] = forecast_df[qty_col].apply(_to_float)

    return (
        forecast_df.groupby("sku_norm", as_index=False)
        .agg(total_sale_qty=("qty_num", "sum"))
        .rename(columns={"sku_norm": "sku"})
    )


def build_step2_rows(
    step1_rows: List[Dict[str, Any]],
    center_rows: List[Dict[str, Any]],
    weekly_rows: List[Dict[str, Any]],
    forecast_rows: Optional[List[Dict[str, Any]]] = None,
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

    sale_start_agg = pd.DataFrame(columns=["sku", "sale_start_date"])
    if forecast_rows:
        forecast_df = pd.DataFrame(forecast_rows)

        forecast_sku_col = _first_existing_col(forecast_df, ["sku", "SKU"])
        forecast_year_week_col = _first_existing_col(forecast_df, ["year_week", "YEAR_WEEK"])
        forecast_sale_qty_col = _first_existing_col(forecast_df, ["SALE_QTY", "sale_qty"])

        if forecast_sku_col and forecast_year_week_col and forecast_sale_qty_col:
            forecast_df["sku_norm"] = forecast_df[forecast_sku_col].fillna("").astype(str).str.strip()
            forecast_df = forecast_df[forecast_df["sku_norm"] != ""].copy()

            forecast_df["sale_qty_num"] = forecast_df[forecast_sale_qty_col].apply(_to_float)
            forecast_df["sale_start_date"] = forecast_df[forecast_year_week_col].apply(_year_week_to_week_start)

            sale_start_agg = (
                forecast_df[
                    (forecast_df["sale_qty_num"] >= 1) &
                    (forecast_df["sale_start_date"].notna())
                ]
                .groupby("sku_norm", as_index=False)
                .agg(sale_start_date=("sale_start_date", "min"))
                .rename(columns={"sku_norm": "sku"})
            )

    sale_end_agg = pd.DataFrame(columns=["sku", "sale_end_date"])
    if forecast_rows:
        forecast_df = pd.DataFrame(forecast_rows)

        forecast_sku_col = _first_existing_col(forecast_df, ["sku", "SKU"])
        forecast_year_week_col = _first_existing_col(forecast_df, ["year_week", "YEAR_WEEK"])
        forecast_stage_col = _first_existing_col(forecast_df, ["stage", "STAGE"])

        if forecast_sku_col and forecast_year_week_col and forecast_stage_col:
            forecast_df["sku_norm"] = forecast_df[forecast_sku_col].fillna("").astype(str).str.strip()
            forecast_df = forecast_df[forecast_df["sku_norm"] != ""].copy()

            forecast_df["stage_norm"] = forecast_df[forecast_stage_col].fillna("").astype(str).str.strip()
            forecast_df["sale_end_date"] = forecast_df[forecast_year_week_col].apply(_year_week_to_week_start)

            sale_end_agg = (
                forecast_df[
                    (forecast_df["stage_norm"] == "쇠퇴") &
                    (forecast_df["sale_end_date"].notna())
                ]
                .groupby("sku_norm", as_index=False)
                .agg(sale_end_date=("sale_end_date", "min"))
                .rename(columns={"sku_norm": "sku"})
            )

    forecast_sale_agg = _forecast_total_sale_agg(forecast_rows or [])

    merged = step1_agg.merge(center_agg, how="left", on="sku")
    merged = merged.merge(shortage_week_agg, how="left", on="sku")
    merged = merged.merge(sale_start_agg, how="left", on="sku")
    merged = merged.merge(sale_end_agg, how="left", on="sku")
    merged = merged.merge(forecast_sale_agg, how="left", on="sku")
    merged["center_stock_qty"] = merged["center_stock_qty"].fillna(0.0)
    if "total_sale_qty" in merged.columns:
        merged["total_sale_qty"] = merged["total_sale_qty"].fillna(0.0)

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
        total_reorder_amount: Optional[int] = None
        if not wk_loss.empty and pd.notna(shortage_start_week):
            ssw = pd.Timestamp(shortage_start_week).normalize()
            sub = wk_loss[(wk_loss["sku_norm"] == sku_key) & (wk_loss["week_start"] >= ssw)]
            total_reorder_amount = int(round(float(sub["loss"].sum())))

        # due_date_reorder_amount: (리드타임 일수 + 안전 4주) 동안의 예상 판매량 = (lead_time/7 + 4) * 주간 평균 loss
        due_date_reorder_amount: Optional[int] = None
        if sku_key in avg_weekly_loss_by_sku.index and pd.notna(avg_weekly_loss_by_sku[sku_key]):
            avg_w = float(avg_weekly_loss_by_sku[sku_key])
            weeks_cover = (float(lead_time) / 7.0) + 4.0
            due_date_reorder_amount = int(round(max(0.0, weeks_cover * avg_w)))

        if pd.isna(shortage_start_week):
            order_due_date: Optional[str] = None
            shortage_start_week_value: Optional[str] = None
        else:
            safety_days = 14  # 안전주수 2주
            order_due_date_ts = shortage_start_week - pd.Timedelta(days=lead_time + safety_days)
            order_due_date = order_due_date_ts.date().isoformat()
            shortage_start_week_value = shortage_start_week.date().isoformat()

        sale_start_raw = r.get("sale_start_date")
        if pd.isna(sale_start_raw):
            sale_start_date_value: Optional[str] = None
        else:
            sale_start_date_value = pd.Timestamp(sale_start_raw).normalize().date().isoformat()

        sale_end_raw = r.get("sale_end_date")
        if pd.isna(sale_end_raw):
            sale_end_date_value: Optional[str] = None
        else:
            sale_end_date_value = pd.Timestamp(sale_end_raw).normalize().date().isoformat()

        style_for_monthly = str(r["style_code"]).strip() if str(r["style_code"]).strip() else ""
        monthly_code = style_for_monthly[6] if len(style_for_monthly) >= 7 else ""

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
                "sale_start_date": sale_start_date_value,
                "total_sale_qty": float(_to_float(r.get("total_sale_qty"))),
                "monthly_code": monthly_code,
                "sale_end_date": sale_end_date_value,
            }
        )

    out.sort(key=lambda x: (x.get("sku") or ""))
    return out


def load_step2(
    style_codes: Optional[List[str]] = None,
    replace_mode: bool = True,
) -> Dict[str, Any]:
    client = get_supabase_client()
    if client is None:
        raise RuntimeError("Supabase 연결 불가: SUPABASE_URL / SUPABASE_KEY 설정을 확인하세요.")

    step1_table = get_step1_table_name()
    center_table = get_center_stock_table_name()
    weekly_table = get_weekly_stock_table_name()
    forecast_table = get_sku_weekly_forecast_table_name()
    step2_table = get_step2_table_name()

    step1_rows = fetch_supabase_table_all_rows(client, step1_table)
    center_rows = fetch_supabase_table_all_rows(client, center_table)
    weekly_rows = fetch_supabase_table_all_rows(client, weekly_table)
    forecast_rows = fetch_supabase_table_all_rows(client, forecast_table)

    if style_codes:
        step1_rows = filter_rows_by_style_codes(step1_rows, style_codes)
        center_rows = filter_rows_by_style_codes(center_rows, style_codes)
        weekly_rows = filter_rows_by_style_codes(weekly_rows, style_codes)
        forecast_rows = filter_rows_by_style_codes(forecast_rows, style_codes)

    result_rows = build_step2_rows(
        step1_rows, center_rows, weekly_rows, forecast_rows
    )

    if replace_mode:
        clear_table_all_rows(client, step2_table)
    inserted = bulk_insert_rows(client, step2_table, result_rows)

    sample = []
    try:
        resp = (
            client.table(step2_table)
            .select(
                "sku, current_shortage_qty, shortage_start_week, order_due_date, "
                "reorder_urgency, center_stock_qty, surplus_qty, shortage_qty, total_reorder_amount, due_date_reorder_amount, "
                "sale_start_date, total_sale_qty, monthly_code, sale_end_date"
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
        "forecast_rows": len(forecast_rows),
        "inserted_rows": inserted,
        "sample_rows": sample,
        "replace_mode": replace_mode,
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

    style_code_text = st.text_area(
        "적재할 style_code 입력",
        placeholder="예:\nSPPPG25U01\nSPRPG24G51\nSPRPG24C62\n\n쉼표(,)나 줄바꿈으로 여러 개 입력 가능",
        height=120,
    )
    style_codes = parse_style_codes(style_code_text)
    if style_codes:
        st.caption(f"선택된 style_code {len(style_codes)}개")

    col1, col2 = st.columns(2)

    append_clicked = col1.button("누적해서 쌓기", use_container_width=True)
    replace_clicked = col2.button("기존 데이터 삭제 후 쌓기", use_container_width=True)

    if append_clicked or replace_clicked:
        try:
            replace_mode = replace_clicked

            with st.spinner("적재 중..."):
                r = load_step2(
                    style_codes=style_codes,
                    replace_mode=replace_mode,
                )

            mode_text = "기존 데이터 삭제 후 적재" if replace_mode else "누적 적재"

            if style_codes:
                st.success(
                    f"{mode_text} 완료: 선택한 style_code {len(style_codes)}개 기준 "
                    f"step2 {r['inserted_rows']:,}행 저장"
                )
            else:
                st.success(
                    f"{mode_text} 완료: "
                    f"step1 {r['step1_rows']:,}행, "
                    f"center {r['center_rows']:,}행, "
                    f"weekly {r['weekly_rows']:,}행, "
                    f"forecast {r.get('forecast_rows', 0):,}행 기준 "
                    f"step2 {r['inserted_rows']:,}행 저장"
                )

            if r.get("sample_rows"):
                st.markdown("**적재 결과 샘플(최대 10행)**")
                sample_df = pd.DataFrame(r["sample_rows"]).rename(
                    columns={
                        "reorder_urgency": "회전/리오더",
                        "sale_start_date": "판매시작일",
                        "total_sale_qty": "판매량",
                        "monthly_code": "월물",
                        "sale_end_date": "판매종료일",
                    }
                )
                sample_df = sample_df.drop(columns=["reorder_needed", "발주필요"], errors="ignore")

                preferred_order = [
                    "sku",
                    "current_shortage_qty",
                    "회전/리오더",
                    "shortage_start_week",
                    "order_due_date",
                    "center_stock_qty",
                    "surplus_qty",
                    "shortage_qty",
                    "total_reorder_amount",
                    "due_date_reorder_amount",
                    "판매시작일",
                    "판매종료일",
                    "판매량",
                    "월물",
                ]
                ordered_cols = [c for c in preferred_order if c in sample_df.columns]
                remaining_cols = [c for c in sample_df.columns if c not in ordered_cols]
                sample_df = sample_df[ordered_cols + remaining_cols]

                st.dataframe(sample_df, use_container_width=True)

        except Exception as e:
            show_detailed_exception(e, title="적재 실패")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        show_detailed_exception(e, title="앱 실행 중 오류")
