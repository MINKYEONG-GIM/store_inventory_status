import os
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

try:
    from supabase import create_client as _create_supabase_client
except ImportError:
    _create_supabase_client = None


# -------------------------------------------------
# 페이지 설정
# -------------------------------------------------
st.set_page_config(
    page_title="Dashboard 적재",
    page_icon="📦",
    layout="wide",
)


# -------------------------------------------------
# 스타일
# -------------------------------------------------
def inject_css() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background: #f3f3f3;
        }

        .block-container {
            max-width: 920px;
            padding-top: 2.8rem;
            padding-bottom: 2rem;
        }

        .load-card {
            background: transparent;
            border-radius: 12px;
            padding: 6px 0 0 0;
        }

        .field-label {
            font-size: 18px;
            font-weight: 600;
            color: #2d2d2d;
            margin-bottom: 10px;
        }

        .hint-text {
            color: #7d7d7d;
            font-size: 13px;
            margin-top: 8px;
        }

        div[data-testid="stTextInput"] input {
            background: #e9ecef !important;
            border: 1px solid #e9ecef !important;
            border-radius: 10px !important;
            color: #333333 !important;
            min-height: 48px !important;
            font-size: 18px !important;
        }

        div[data-testid="stTextInput"] label {
            font-size: 17px !important;
            font-weight: 600 !important;
            color: #2d2d2d !important;
        }

        div.stButton > button {
            min-height: 48px !important;
            border-radius: 10px !important;
            font-size: 18px !important;
            font-weight: 700 !important;
            padding: 0 22px !important;
        }

        .preview-title {
            margin-top: 28px;
            margin-bottom: 8px;
            font-size: 18px;
            font-weight: 700;
            color: #2d2d2d;
        }

        .status-box {
            background: white;
            border-radius: 12px;
            border: 1px solid #e4e4e4;
            padding: 14px 16px;
            margin-top: 14px;
            color: #333;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


inject_css()


# -------------------------------------------------
# 공통 유틸
# -------------------------------------------------
def normalize_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_num(value: Any) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def iso_year_week(dt: datetime) -> str:
    iso = dt.isocalendar()
    return f"{iso.year}-{iso.week:02d}"


def parse_year_week_to_monday(year_week: str) -> Optional[date]:
    """
    '2026-16' -> 해당 ISO week의 월요일 date
    """
    try:
        year_week = normalize_text(year_week)
        if not year_week or "-" not in year_week:
            return None
        year_str, week_str = year_week.split("-", 1)
        year = int(year_str)
        week = int(week_str)
        return date.fromisocalendar(year, week, 1)
    except Exception:
        return None


def get_week_keys_from_today(weeks: int = 5) -> List[str]:
    today = datetime.today()
    monday = date.fromisocalendar(today.isocalendar().year, today.isocalendar().week, 1)
    result = []
    for i in range(weeks):
        d = pd.Timestamp(monday) + pd.Timedelta(weeks=i)
        iso = d.isocalendar()
        result.append(f"{iso.year}-{iso.week:02d}")
    return result


# -------------------------------------------------
# Supabase 연결
# -------------------------------------------------
def get_supabase_client():
    if _create_supabase_client is None:
        raise ImportError("supabase 패키지가 없습니다. requirements.txt에 supabase를 추가하세요.")

    url = ""
    key = ""

    try:
        if hasattr(st, "secrets"):
            url = str(st.secrets.get("SUPABASE_URL") or "").strip()
            key = str(st.secrets.get("SUPABASE_KEY") or "").strip()

            if (not url or not key) and "supabase" in st.secrets:
                sec = dict(st.secrets["supabase"])
                url = str(sec.get("url") or url or "").strip()
                key = str(
                    sec.get("service_role_key")
                    or sec.get("key")
                    or sec.get("anon_key")
                    or key
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
        raise ValueError("Supabase 접속 정보가 없습니다. SUPABASE_URL, SUPABASE_KEY를 설정하세요.")

    return _create_supabase_client(url, key)


def fetch_supabase_table_all_rows(client, table_name: str, batch_size: int = 1000) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    off = 0

    while True:
        try:
            resp = client.table(table_name).select("*").limit(batch_size).offset(off).execute()
        except Exception:
            resp = client.table(table_name).select("*").range(off, off + batch_size - 1).execute()

        chunk = resp.data if getattr(resp, "data", None) else []
        if not chunk:
            break

        rows.extend(chunk)

        if len(chunk) < batch_size:
            break

        off += batch_size

    return rows


# -------------------------------------------------
# 데이터 로드
# -------------------------------------------------
@st.cache_data(ttl=300)
def load_forecast_df() -> pd.DataFrame:
    client = get_supabase_client()
    rows = fetch_supabase_table_all_rows(client, "sku_weekly_forecast_2")

    columns = [
        "id", "created_at", "year_week", "sale_qty", "stage", "style_code", "sku",
        "is_peak_week", "plant", "last_year_ratio_pct", "BASE_STOCK_QTY", "is_forecast",
        "loss", "IPGO_QTY", "shape_type", "week_no", "sale_end_date",
    ]

    if not rows:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(rows)

    text_cols = ["year_week", "stage", "style_code", "sku", "plant", "shape_type"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].apply(normalize_text)

    num_cols = ["sale_qty", "last_year_ratio_pct", "BASE_STOCK_QTY", "loss", "IPGO_QTY", "week_no"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    if "sale_end_date" in df.columns:
        df["sale_end_date"] = pd.to_datetime(df["sale_end_date"], errors="coerce")

    df["year_week_date"] = df["year_week"].apply(parse_year_week_to_monday)

    return df


# -------------------------------------------------
# dashboard 적재용 데이터 생성
# -------------------------------------------------
def build_dashboard_df(forecast_df: pd.DataFrame, style_code_filter: str = "") -> pd.DataFrame:
    if forecast_df.empty:
        return pd.DataFrame(columns=[
            "style_code", "sku", "plant", "total_reorder",
            "w0_reorder", "w0_lackplant",
            "w1_reorder", "w1_lackplant",
            "w2_reorder", "w2_lackplant",
            "w3_reorder", "w3_lackplant",
            "w4_reorder", "w4_lackplant",
        ])

    f = forecast_df.copy()

    # style_code 필터
    style_code_filter = normalize_text(style_code_filter)
    if style_code_filter:
        f = f[f["style_code"] == style_code_filter].copy()

    if f.empty:
        return pd.DataFrame(columns=[
            "style_code", "sku", "plant", "total_reorder",
            "w0_reorder", "w0_lackplant",
            "w1_reorder", "w1_lackplant",
            "w2_reorder", "w2_lackplant",
            "w3_reorder", "w3_lackplant",
            "w4_reorder", "w4_lackplant",
        ])

    today_key = iso_year_week(datetime.today())
    today_date = parse_year_week_to_monday(today_key)
    target_weeks = get_week_keys_from_today(weeks=5)

    # loss는 부족 수량으로 사용. 음수 방지
    f["loss"] = pd.to_numeric(f["loss"], errors="coerce").fillna(0)
    f["loss"] = f["loss"].clip(lower=0)

    # 현재 주차 이후 전체 리오더 합계
    future_all = f[f["year_week_date"].notna()].copy()
    if today_date is not None:
        future_all = future_all[future_all["year_week_date"] >= today_date].copy()

    total_reorder_df = (
        future_all.groupby(["style_code", "sku", "plant"], dropna=False)["loss"]
        .sum()
        .reset_index()
        .rename(columns={"loss": "total_reorder"})
    )

    # W+0 ~ W+4 대상 주차만
    f_5w = f[f["year_week"].isin(target_weeks)].copy()

    grouped = (
        f_5w.groupby(["style_code", "sku", "plant", "year_week"], dropna=False)["loss"]
        .sum()
        .reset_index()
    )

    # 부족매장수 = 같은 sku, 같은 주차에서 loss > 0인 plant 수
    lackplant_df = (
        grouped.assign(is_lack=lambda x: (x["loss"] > 0).astype(int))
        .groupby(["sku", "year_week"], dropna=False)["is_lack"]
        .sum()
        .reset_index()
    )

    lackplant_map = {
        (normalize_text(r["sku"]), normalize_text(r["year_week"])): int(r["is_lack"])
        for _, r in lackplant_df.iterrows()
    }

    # base row
    base = (
        f[["style_code", "sku", "plant"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    result = base.merge(
        total_reorder_df,
        on=["style_code", "sku", "plant"],
        how="left"
    )

    # W+0 ~ W+4
    for i, yw in enumerate(target_weeks):
        reorder_col = f"w{i}_reorder"
        lack_col = f"w{i}_lackplant"

        tmp = grouped[grouped["year_week"] == yw].copy()
        tmp = tmp.rename(columns={"loss": reorder_col})

        result = result.merge(
            tmp[["style_code", "sku", "plant", reorder_col]],
            on=["style_code", "sku", "plant"],
            how="left"
        )

        result[lack_col] = result["sku"].apply(
            lambda x: lackplant_map.get((normalize_text(x), yw), 0)
        )

    # null -> 0
    for col in [
        "total_reorder",
        "w0_reorder", "w0_lackplant",
        "w1_reorder", "w1_lackplant",
        "w2_reorder", "w2_lackplant",
        "w3_reorder", "w3_lackplant",
        "w4_reorder", "w4_lackplant",
    ]:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0)

    # 정수형 정리
    int_cols = [
        "total_reorder",
        "w0_reorder", "w0_lackplant",
        "w1_reorder", "w1_lackplant",
        "w2_reorder", "w2_lackplant",
        "w3_reorder", "w3_lackplant",
        "w4_reorder", "w4_lackplant",
    ]
    for col in int_cols:
        result[col] = result[col].round(0).astype(int)

    result = result.sort_values(["style_code", "sku", "plant"]).reset_index(drop=True)

    return result[[
        "style_code", "sku", "plant", "total_reorder",
        "w0_reorder", "w0_lackplant",
        "w1_reorder", "w1_lackplant",
        "w2_reorder", "w2_lackplant",
        "w3_reorder", "w3_lackplant",
        "w4_reorder", "w4_lackplant",
    ]]


# -------------------------------------------------
# dashboard 적재
# -------------------------------------------------
def delete_dashboard_rows(client, style_code_filter: str = "") -> None:
    style_code_filter = normalize_text(style_code_filter)

    if style_code_filter:
        client.table("dashboard").delete().eq("style_code", style_code_filter).execute()
    else:
        client.table("dashboard").delete().neq("id", 0).execute()


def insert_dashboard_rows(client, dash_df: pd.DataFrame, batch_size: int = 1000) -> None:
    if dash_df.empty:
        return

    records = dash_df.to_dict(orient="records")

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        client.table("dashboard").insert(batch).execute()


# -------------------------------------------------
# UI
# -------------------------------------------------
def main() -> None:
    st.markdown('<div class="load-card">', unsafe_allow_html=True)

    st.markdown(
        '<div class="field-label">적재할 style_code</div>',
        unsafe_allow_html=True
    )

    style_code_input = st.text_input(
        label="",
        placeholder="예: SPPPG25U01",
        key="style_code_input"
    )

    c1, c2, c3 = st.columns([1.25, 0.2, 2.6])

    with c1:
        append_clicked = st.button("누적해서 쌓기", use_container_width=True, type="primary")

    with c3:
        replace_clicked = st.button("기존 데이터 삭제 후 쌓기", use_container_width=False)

    st.markdown('</div>', unsafe_allow_html=True)

    if append_clicked or replace_clicked:
        try:
            with st.spinner("데이터를 불러오고 있습니다..."):
                forecast_df = load_forecast_df()

            if forecast_df.empty:
                st.warning("sku_weekly_forecast_2 테이블에 데이터가 없습니다.")
                st.stop()

            dash_df = build_dashboard_df(forecast_df, style_code_input)

            if dash_df.empty:
                st.warning("조건에 맞는 적재 대상 데이터가 없습니다.")
                st.stop()

            client = get_supabase_client()

            with st.spinner("dashboard 테이블에 적재 중입니다..."):
                if replace_clicked:
                    delete_dashboard_rows(client, style_code_input)

                insert_dashboard_rows(client, dash_df)

            st.success(f"적재 완료: {len(dash_df):,}건")

            st.markdown('<div class="preview-title">적재 미리보기</div>', unsafe_allow_html=True)
            st.dataframe(dash_df, use_container_width=True, hide_index=True, height=420)

        except Exception as e:
            st.error(f"오류가 발생했습니다: {e}")


if __name__ == "__main__":
    main()
