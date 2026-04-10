import os
import traceback
from typing import Any, Dict, List

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
# 테이블명
# -----------------------------
def get_forecast_table_name() -> str:
    return (os.getenv("SUPABASE_SKU_WEEKLY_FORECAST_2_TABLE") or "sku_weekly_forecast_2").strip()


def get_center_stock_table_name() -> str:
    return (os.getenv("SUPABASE_CENTER_STOCK_TABLE") or "center_stock").strip()


def get_weekly_stock_table_name() -> str:
    return (os.getenv("SUPABASE_WEEKLY_STOCK_TABLE") or "weekly_stock").strip()


# -----------------------------
# 유틸
# -----------------------------
def _to_float(v: Any) -> float:
    x = pd.to_numeric(v, errors="coerce")
    if pd.isna(x):
        return 0.0
    return float(x)


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


def bulk_insert_rows(client, table_name: str, rows: List[Dict[str, Any]], batch_size: int = 500) -> int:
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
# weekly_stock 계산
# -----------------------------
def build_weekly_stock_rows(
    forecast_rows: List[Dict[str, Any]],
    center_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not forecast_rows:
        return []

    forecast_df = pd.DataFrame(forecast_rows)
    center_df = pd.DataFrame(center_rows) if center_rows else pd.DataFrame()

    required_cols = ["year_week", "style_code", "sku", "sale_qty", "BASE_STOCK_QTY"]
    for col in required_cols:
        if col not in forecast_df.columns:
            forecast_df[col] = None

    forecast_df["sku_norm"] = forecast_df["sku"].fillna("").astype(str).str.strip()
    forecast_df["style_code_norm"] = forecast_df["style_code"].fillna("").astype(str).str.strip()
    forecast_df["year_week_norm"] = forecast_df["year_week"].fillna("").astype(str).str.strip()
    forecast_df["sale_qty_num"] = forecast_df["sale_qty"].apply(_to_float)
    forecast_df["base_stock_qty_num"] = forecast_df["BASE_STOCK_QTY"].apply(_to_float)

    forecast_df = forecast_df[
        (forecast_df["sku_norm"] != "") &
        (forecast_df["year_week_norm"] != "")
    ].copy()

    if forecast_df.empty:
        return []

    # sku + year_week 기준 집계
    weekly_sales = (
        forecast_df.groupby(["sku_norm", "year_week_norm"], as_index=False)
        .agg(
            style_code=("style_code_norm", lambda s: next((x for x in s if str(x).strip()), "")),
            sale_qty=("sale_qty_num", "sum"),
            base_stock_qty=("base_stock_qty_num", "max"),
        )
    )

    # center_stock sku별 합계
    if center_df.empty:
        center_sum = pd.DataFrame(columns=["sku_norm", "center_stock"])
    else:
        if "sku" not in center_df.columns:
            center_df["sku"] = None
        if "stock_qty" not in center_df.columns:
            center_df["stock_qty"] = 0

        center_df["sku_norm"] = center_df["sku"].fillna("").astype(str).str.strip()
        center_df["stock_qty_num"] = center_df["stock_qty"].apply(_to_float)

        center_sum = (
            center_df[center_df["sku_norm"] != ""]
            .groupby("sku_norm", as_index=False)
            .agg(center_stock=("stock_qty_num", "sum"))
        )

    df = weekly_sales.merge(center_sum, how="left", on="sku_norm")
    df["center_stock"] = df["center_stock"].fillna(0.0)

    # year_week 정렬용
    def parse_year_week(yyww: str):
        txt = str(yyww).strip()
        if "-" in txt:
            parts = txt.split("-")
        elif len(txt) == 6 and txt.isdigit():
            parts = [txt[:4], txt[4:]]
        else:
            return (9999, 9999)

        try:
            y = int(parts[0])
            w = int(parts[1])
            return (y, w)
        except Exception:
            return (9999, 9999)

    df["year_num"] = df["year_week_norm"].apply(lambda x: parse_year_week(x)[0])
    df["week_num"] = df["year_week_norm"].apply(lambda x: parse_year_week(x)[1])

    df = df.sort_values(["sku_norm", "year_num", "week_num"]).reset_index(drop=True)

    out: List[Dict[str, Any]] = []

    for sku, g in df.groupby("sku_norm", sort=False):
        g = g.sort_values(["year_num", "week_num"]).reset_index(drop=True)

        running_stock = None

        for i, row in g.iterrows():
            base_stock_qty = _to_float(row["base_stock_qty"])
            sale_qty = _to_float(row["sale_qty"])
            raw_center_stock = _to_float(row["center_stock"])

            # 첫 주에만 center_stock 반영
            applied_center_stock = raw_center_stock if i == 0 else 0.0

            if i == 0:
                stock = base_stock_qty + applied_center_stock - sale_qty
            else:
                stock = running_stock - sale_qty

            running_stock = stock

            out.append(
                {
                    "year_week": str(row["year_week_norm"]).strip(),
                    "style_code": str(row["style_code"]).strip(),
                    "sku": str(row["sku_norm"]).strip(),
                    "stock": float(stock),
                    "center_stock": float(applied_center_stock),
                    "sale_qty": float(sale_qty),
                }
            )

    out.sort(key=lambda x: (x.get("sku") or "", x.get("year_week") or ""))
    return out


def load_weekly_stock() -> Dict[str, Any]:
    client = get_supabase_client()
    if client is None:
        raise RuntimeError("Supabase 연결 불가: SUPABASE_URL / SUPABASE_KEY 설정을 확인하세요.")

    forecast_table = get_forecast_table_name()
    center_table = get_center_stock_table_name()
    weekly_stock_table = get_weekly_stock_table_name()

    forecast_rows = fetch_supabase_table_all_rows(client, forecast_table)
    center_rows = fetch_supabase_table_all_rows(client, center_table)

    result_rows = build_weekly_stock_rows(forecast_rows, center_rows)

    clear_table_all_rows(client, weekly_stock_table)
    inserted = bulk_insert_rows(client, weekly_stock_table, result_rows)

    sample = []
    try:
        resp = (
            client.table(weekly_stock_table)
            .select("year_week, style_code, sku, stock, center_stock, sale_qty")
            .limit(20)
            .execute()
        )
        sample = resp.data if resp and getattr(resp, "data", None) else []
    except Exception:
        sample = []

    return {
        "forecast_rows": len(forecast_rows),
        "center_rows": len(center_rows),
        "inserted_rows": inserted,
        "sample_rows": sample,
    }


# -----------------------------
# 화면
# -----------------------------
def main():
    st.set_page_config(page_title="weekly_stock loader", layout="centered")

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

    if st.button("데이터 넣기", use_container_width=True):
        try:
            with st.spinner("적재 중..."):
                r = load_weekly_stock()
            st.success(
                f"완료: forecast {r['forecast_rows']:,}행, "
                f"center {r['center_rows']:,}행 기준, "
                f"weekly_stock {r['inserted_rows']:,}행 저장"
            )
            if r.get("sample_rows"):
                st.markdown("**적재 결과 샘플**")
                st.dataframe(pd.DataFrame(r["sample_rows"]), use_container_width=True)
        except Exception as e:
            show_detailed_exception(e, title="적재 실패")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        show_detailed_exception(e, title="앱 실행 중 오류")
