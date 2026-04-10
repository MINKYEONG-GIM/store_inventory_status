"""
Supabase 저장 결과 조회기.

- public.store_inventory_status_step1 : 배치/외부 파이프라인이 채운 매장 재고 상태(1단계)
- public.sku_weekly_forecast_2       : 주차별 예측·실적 스냅샷(참고용 조회)

이 스크립트는 PLC/RAW/reorder를 읽어 새로 예측을 계산하지 않습니다.
"""
import os
import traceback
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

try:
    from supabase import create_client as _create_supabase_client
except ImportError:
    _create_supabase_client = None

st.set_page_config(page_title="재고 상태 조회", layout="wide")


def show_detailed_exception(err: BaseException, title: str = "오류가 발생했습니다") -> None:
    st.error(title)
    st.markdown(f"**예외 종류:** `{type(err).__name__}`")
    st.caption("메시지(내용에 기호가 있어도 그대로 표시)")
    st.code(str(err) if str(err) else "(메시지 없음)", language="text")
    tb = traceback.format_exc()
    with st.expander("전체 스택 트레이스", expanded=True):
        st.code(tb, language="text")
    print(f"[{type(err).__name__}] {err}\n{tb}", flush=True)


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


def get_store_inventory_status_step1_table_name() -> str:
    """
    public.store_inventory_status_step1
    secrets [supabase] store_inventory_status_step1_table
    환경변수 SUPABASE_STORE_INVENTORY_STATUS_STEP1_TABLE
    """
    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            v = st.secrets["supabase"].get("store_inventory_status_step1_table")
            if v is not None and str(v).strip():
                return str(v).strip()
    except Exception:
        pass
    return (
        os.getenv("SUPABASE_STORE_INVENTORY_STATUS_STEP1_TABLE")
        or "store_inventory_status_step1"
    ).strip()


def get_sku_weekly_forecast_table_name() -> str:
    """
    public.sku_weekly_forecast_2
    secrets [supabase] sku_weekly_forecast_table
    환경변수 SUPABASE_SKU_WEEKLY_FORECAST_TABLE
    """
    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            v = st.secrets["supabase"].get("sku_weekly_forecast_table")
            if v is not None and str(v).strip():
                return str(v).strip()
    except Exception:
        pass
    return (os.getenv("SUPABASE_SKU_WEEKLY_FORECAST_TABLE") or "sku_weekly_forecast_2").strip()


def fetch_supabase_table_all_rows(
    client,
    table_name: str,
    batch_size: int = 1000,
) -> List[Dict[str, Any]]:
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


def fetch_step1_with_optional_filters(
    client,
    *,
    sku: Optional[str] = None,
    plant: Optional[str] = None,
    style_code: Optional[str] = None,
) -> pd.DataFrame:
    """
    store_inventory_status_step1 조회.
    필터가 있으면 PostgREST eq 체인(AND), 없으면 전체 순회 로드.
    """
    tbl = get_store_inventory_status_step1_table_name()
    sku_s = (sku or "").strip()
    plant_s = (plant or "").strip()
    style_s = (style_code or "").strip()

    if not sku_s and not plant_s and not style_s:
        records = fetch_supabase_table_all_rows(client, tbl)
        return pd.DataFrame(records) if records else pd.DataFrame()

    q = client.table(tbl).select("*")
    if sku_s:
        q = q.eq("sku", sku_s)
    if plant_s:
        q = q.eq("plant", plant_s)
    if style_s:
        q = q.eq("style_code", style_s)

    try:
        resp = q.execute()
        data = resp.data if resp.data else []
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception:
        records = fetch_supabase_table_all_rows(client, tbl)
        df = pd.DataFrame(records) if records else pd.DataFrame()
        if df.empty:
            return df
        if sku_s:
            df = df[df.get("sku", "").astype(str).str.strip() == sku_s]
        if plant_s:
            df = df[df.get("plant", "").astype(str).str.strip() == plant_s]
        if style_s:
            df = df[df.get("style_code", "").astype(str).str.strip() == style_s]
        return df


def fetch_weekly_forecast_rows_by_sku(client, sku: str) -> List[Dict[str, Any]]:
    """sku_weekly_forecast_2에서 단일 SKU 전 행."""
    sku_s = str(sku).strip()
    if not sku_s:
        return []
    tbl_name = get_sku_weekly_forecast_table_name()
    rows: List[Dict[str, Any]] = []
    off = 0
    batch_size = 1000
    while True:
        try:
            resp = (
                client.table(tbl_name)
                .select("*")
                .eq("sku", sku_s)
                .limit(batch_size)
                .offset(off)
                .execute()
            )
        except Exception:
            resp = (
                client.table(tbl_name)
                .select("*")
                .eq("sku", sku_s)
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


def main() -> None:
    st.title("매장 재고 상태 · 주차별 예측 (조회 전용)")
    st.caption(
        "예측을 새로 계산하지 않습니다. "
        f"`{get_store_inventory_status_step1_table_name()}` 는 배치가 채운 결과를, "
        f"`{get_sku_weekly_forecast_table_name()}` 는 저장된 주차 스냅샷을 봅니다."
    )

    sb = get_supabase_client()
    if sb is None:
        st.error("Supabase 연결 불가: secrets [supabase] url·key 또는 환경변수를 설정하세요.")
        return

    st.subheader("1) store_inventory_status_step1")
    st.caption(
        "컬럼 예: style_code, sku, plant, store_classification, lead_time, "
        "current_qty, stock_weeks, shortage_qty, surplus_qty"
    )
    c1, c2, c3 = st.columns(3)
    with c1:
        f_sku = st.text_input("SKU 필터 (비우면 전체)", key="s1_sku")
    with c2:
        f_plant = st.text_input("plant 필터", key="s1_plant")
    with c3:
        f_style = st.text_input("style_code 필터", key="s1_style")

    if st.button("step1 테이블 조회", type="primary", key="btn_step1"):
        try:
            df = fetch_step1_with_optional_filters(
                sb,
                sku=f_sku or None,
                plant=f_plant or None,
                style_code=f_style or None,
            )
            st.success(f"{len(df):,}행")
            if not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.info("조건에 맞는 행이 없습니다.")
        except Exception as e:
            show_detailed_exception(e, title="step1 조회 실패")

    st.divider()
    st.subheader("2) sku_weekly_forecast_2 (SKU별)")
    st.caption(
        "컬럼 예: year_week, sale_qty, stage, style_code, sku, plant, "
        "BASE_STOCK_QTY, IPGO_QTY, shape_type, week_no …"
    )
    wf_sku = st.text_input("SKU (MATERIAL)", key="wf_sku", placeholder="필수")
    if st.button("주차별 예측 테이블 조회", type="secondary", key="btn_wf"):
        if not str(wf_sku).strip():
            st.warning("SKU를 입력하세요.")
        else:
            try:
                rows = fetch_weekly_forecast_rows_by_sku(sb, str(wf_sku).strip())
                st.success(f"{len(rows):,}행")
                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
                else:
                    st.info("해당 SKU 행이 없습니다.")
            except Exception as e:
                show_detailed_exception(e, title="sku_weekly_forecast_2 조회 실패")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        show_detailed_exception(e, title="앱 실행 중 오류")
