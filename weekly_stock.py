import os
import traceback
from typing import Any, Dict, List, Tuple

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
    # weekly_stock 적재의 소스 테이블 (forecast_2 사용 안 함)
    return (os.getenv("SUPABASE_SKU_WEEKLY_FORECAST_TABLE") or "sku_weekly_forecast").strip()


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


def _parse_style_codes(text: str) -> List[str]:
    raw = (text or "").replace("\n", ",").replace("\t", ",")
    parts = [p.strip() for p in raw.split(",")]

    out: List[str] = []
    seen = set()

    for p in parts:
        if not p:
            continue
        if p in seen:
            continue
        seen.add(p)
        out.append(p)

    return out


def parse_year_week(yyww: Any) -> Tuple[int, int]:
    txt = str(yyww or "").strip()

    if not txt:
        return (9999, 9999)

    if "-" in txt:
        parts = txt.split("-")
    elif len(txt) == 6 and txt.isdigit():
        parts = [txt[:4], txt[4:]]
    else:
        return (9999, 9999)

    try:
        year_num = int(parts[0])
        week_num = int(parts[1])
        return (year_num, week_num)
    except Exception:
        return (9999, 9999)


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


def fetch_supabase_table_rows_by_style_codes(
    client,
    table_name: str,
    style_codes: List[str],
    batch_size: int = 1000,
) -> List[Dict[str, Any]]:
    if not style_codes:
        return fetch_supabase_table_all_rows(client, table_name, batch_size=batch_size)

    rows: List[Dict[str, Any]] = []
    off = 0

    while True:
        try:
            resp = (
                client.table(table_name)
                .select("*")
                .in_("style_code", style_codes)
                .limit(batch_size)
                .offset(off)
                .execute()
            )
        except Exception:
            resp = (
                client.table(table_name)
                .select("*")
                .in_("style_code", style_codes)
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

    # forecast 필수 컬럼 보정
    forecast_required_cols = [
        "year_week", "sku", "sale_qty", "BASE_STOCK_QTY", "IPGO_QTY", "loss"
    ]
    for col in forecast_required_cols:
        if col not in forecast_df.columns:
            forecast_df[col] = None

    forecast_df["year_week"] = forecast_df["year_week"].fillna("").astype(str).str.strip()
    forecast_df["sku"] = forecast_df["sku"].fillna("").astype(str).str.strip()
    forecast_df["sale_qty_num"] = forecast_df["sale_qty"].apply(_to_float)
    forecast_df["base_stock_qty_num"] = forecast_df["BASE_STOCK_QTY"].apply(_to_float)
    forecast_df["ipgo_qty_num"] = forecast_df["IPGO_QTY"].apply(_to_float)
    forecast_df["loss_num"] = forecast_df["loss"].apply(_to_float)

    forecast_df = forecast_df[
        (forecast_df["year_week"] != "") &
        (forecast_df["sku"] != "")
    ].copy()

    if forecast_df.empty:
        return []

    # sku + year_week 집계
    weekly_df = (
        forecast_df.groupby(["year_week", "sku"], as_index=False)
        .agg(
            total_sale_qty=("sale_qty_num", "sum"),
            total_base_stock_qty=("base_stock_qty_num", "sum"),
            total_ipgo_qty=("ipgo_qty_num", "sum"),
            total_loss=("loss_num", "sum"),
        )
    )

    # center_stock sku별 합계
    if center_df.empty:
        center_sum_df = pd.DataFrame(columns=["sku", "total_center_stock"])
    else:
        if "sku" not in center_df.columns:
            center_df["sku"] = None
        if "stock_qty" not in center_df.columns:
            center_df["stock_qty"] = 0

        center_df["sku"] = center_df["sku"].fillna("").astype(str).str.strip()
        center_df["stock_qty_num"] = center_df["stock_qty"].apply(_to_float).clip(lower=0)

        center_sum_df = (
            center_df[center_df["sku"] != ""]
            .groupby("sku", as_index=False)
            .agg(total_center_stock=("stock_qty_num", "sum"))
        )

    # merge
    df = weekly_df.merge(center_sum_df, how="left", on="sku")
    df["total_center_stock"] = df["total_center_stock"].fillna(0.0)

    # year_week 정렬
    df["year_num"] = df["year_week"].apply(lambda x: parse_year_week(x)[0])
    df["week_num"] = df["year_week"].apply(lambda x: parse_year_week(x)[1])

    df = df.sort_values(["sku", "year_num", "week_num"]).reset_index(drop=True)

    # sku별 cumulative_loss
    df["cumulative_loss"] = (
        df.groupby("sku")["total_loss"]
        .cumsum()
    )

    out: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        out.append(
            {
                "year_week": str(row["year_week"]).strip(),
                "sku": str(row["sku"]).strip(),
                "total_sale_qty": float(_to_float(row["total_sale_qty"])),
                "total_base_stock_qty": float(_to_float(row["total_base_stock_qty"])),
                "total_ipgo_qty": float(_to_float(row["total_ipgo_qty"])),
                "total_loss": float(_to_float(row["total_loss"])),
                "cumulative_loss": float(_to_float(row["cumulative_loss"])),
                "total_center_stock": float(_to_float(row["total_center_stock"])),
            }
        )

    return out


def load_weekly_stock(
    style_codes: List[str],
    delete_before_insert: bool = True,
) -> Dict[str, Any]:
    client = get_supabase_client()
    if client is None:
        raise RuntimeError("Supabase 연결 불가: SUPABASE_URL / SUPABASE_KEY 설정을 확인하세요.")

    forecast_table = get_forecast_table_name()
    center_table = get_center_stock_table_name()
    weekly_stock_table = get_weekly_stock_table_name()

    forecast_rows = fetch_supabase_table_rows_by_style_codes(
        client,
        forecast_table,
        style_codes,
    )
    center_rows = fetch_supabase_table_all_rows(client, center_table)

    result_rows = build_weekly_stock_rows(forecast_rows, center_rows)

    if delete_before_insert:
        clear_table_all_rows(client, weekly_stock_table)
    inserted = bulk_insert_rows(client, weekly_stock_table, result_rows)

    sample = []
    try:
        resp = (
            client.table(weekly_stock_table)
            .select(
                "year_week, sku, total_sale_qty, total_base_stock_qty, "
                "total_ipgo_qty, total_loss, cumulative_loss, total_center_stock"
            )
            .order("sku")
            .order("year_week")
            .limit(30)
            .execute()
        )
        sample = resp.data if resp and getattr(resp, "data", None) else []
    except Exception:
        sample = []

    return {
        "filtered_style_codes": style_codes,
        "forecast_rows": len(forecast_rows),
        "center_rows": len(center_rows),
        "inserted_rows": inserted,
        "sample_rows": sample,
    }


# -----------------------------
# 화면
# -----------------------------
def main():
    st.set_page_config(page_title="loader", layout="centered")

    st.markdown(
        """
        <style>
        header, footer, [data-testid="stToolbar"], [data-testid="stDecoration"] {
            display: none !important;
        }
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
            max-width: 420px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("### weekly_stock 적재")
    st.caption("`sku_weekly_forecast`에서 원하는 `style_code`만 골라 `weekly_stock`를 계산해 저장합니다.")

    style_code_text = st.text_input(
        "가져올 style_code 입력 (여러 개는 콤마로 구분)",
        placeholder="예: SPPPG25U0, SPRPG24G5",
    )

    style_codes = _parse_style_codes(style_code_text)

    if style_codes:
        st.caption(f"선택된 style_code: {', '.join(style_codes)}")
    else:
        st.caption("style_code를 비우면 전체 데이터를 가져옵니다.")

    delete_before_insert = st.checkbox("기존 weekly_stock 삭제 후 적재", value=True)
    if delete_before_insert:
        st.caption("현재 설정: 기존 `weekly_stock`를 **삭제 후 재적재**합니다.")
    else:
        st.caption("현재 설정: 기존 `weekly_stock`를 유지하고 **누적 적재**합니다. (중복 적재될 수 있어요)")

    if st.button("weekly_stock 데이터 넣기", use_container_width=True):
        try:
            with st.spinner("적재 중..."):
                r = load_weekly_stock(
                    style_codes=style_codes,
                    delete_before_insert=delete_before_insert,
                )

            style_code_msg = (
                ", ".join(r.get("filtered_style_codes", []))
                if r.get("filtered_style_codes")
                else "전체"
            )
            st.success(
                f"완료: style_code [{style_code_msg}] 기준 "
                f"forecast {r['forecast_rows']:,}행, "
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
