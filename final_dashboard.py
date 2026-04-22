import os
from typing import Optional

import pandas as pd
import streamlit as st
from supabase import create_client, Client


# =========================
# 기본 설정
# =========================
st.set_page_config(page_title="판매량 / 리오더 대시보드", layout="wide")
st.title("판매량 / 리오더 필요 수량 대시보드")
st.caption("dashboard 테이블 기준으로 판매량과 리오더 필요 수량만 조회하는 화면")


# =========================
# Supabase 연결
# =========================
@st.cache_resource
def get_supabase_client() -> Client:
    url = st.secrets.get("SUPABASE_URL", os.getenv("SUPABASE_URL"))
    key = st.secrets.get("SUPABASE_KEY", os.getenv("SUPABASE_KEY"))

    if not url or not key:
        raise ValueError("SUPABASE_URL 또는 SUPABASE_KEY가 없습니다.")

    return create_client(url, key)


# =========================
# 데이터 조회
# =========================
@st.cache_data(ttl=300)
def load_dashboard_data() -> pd.DataFrame:
    supabase = get_supabase_client()

    response = (
        supabase.table("dashboard")
        .select(
            "id, created_at, style_code, sku, plant, total_reorder, "
            "w0_reorder, w0_lackplant, "
            "w1_reorder, w1_lackplant, "
            "w2_reorder, w2_lackplant, "
            "w3_reorder, w3_lackplant, "
            "w4_reorder, w4_lackplant, "
            "base_stock, w1_sale_prev, w2_sale_prev"
        )
        .order("created_at", desc=True)
        .execute()
    )

    data = response.data or []
    df = pd.DataFrame(data)

    if df.empty:
        return df

    numeric_cols = [
        "total_reorder",
        "w0_reorder", "w0_lackplant",
        "w1_reorder", "w1_lackplant",
        "w2_reorder", "w2_lackplant",
        "w3_reorder", "w3_lackplant",
        "w4_reorder", "w4_lackplant",
        "base_stock", "w1_sale_prev", "w2_sale_prev"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    # 최근 2주 평균 판매량
    df["avg_sale_prev_2w"] = (df["w1_sale_prev"] + df["w2_sale_prev"]) / 2

    # 미래 주차 리오더 합계
    df["sum_reorder_5w"] = (
        df["w0_reorder"]
        + df["w1_reorder"]
        + df["w2_reorder"]
        + df["w3_reorder"]
        + df["w4_reorder"]
    )

    # 미래 주차 부족 매장 수 합계
    df["sum_lackplant_5w"] = (
        df["w0_lackplant"]
        + df["w1_lackplant"]
        + df["w2_lackplant"]
        + df["w3_lackplant"]
        + df["w4_lackplant"]
    )

    return df


# =========================
# 필터 적용 함수
# =========================
def apply_filters(
    df: pd.DataFrame,
    style_code: Optional[str],
    sku: Optional[str],
    plant_list: list[str],
    reorder_only: bool,
) -> pd.DataFrame:
    filtered = df.copy()

    if style_code and style_code != "전체":
        filtered = filtered[filtered["style_code"] == style_code]

    if sku and sku != "전체":
        filtered = filtered[filtered["sku"] == sku]

    if plant_list:
        filtered = filtered[filtered["plant"].isin(plant_list)]

    if reorder_only:
        filtered = filtered[filtered["sum_reorder_5w"] > 0]

    return filtered


# =========================
# 데이터 로드
# =========================
try:
    df = load_dashboard_data()
except Exception as e:
    st.error(f"데이터 로드 중 오류가 발생했습니다: {e}")
    st.stop()

if df.empty:
    st.warning("dashboard 테이블에 데이터가 없습니다.")
    st.stop()


# =========================
# 사이드바 필터
# =========================
st.sidebar.header("조회 조건")

style_options = ["전체"] + sorted([x for x in df["style_code"].dropna().unique().tolist() if x])
selected_style = st.sidebar.selectbox("스타일코드", style_options)

sku_source = df if selected_style == "전체" else df[df["style_code"] == selected_style]
sku_options = ["전체"] + sorted([x for x in sku_source["sku"].dropna().unique().tolist() if x])
selected_sku = st.sidebar.selectbox("SKU", sku_options)

plant_options = sorted([x for x in df["plant"].dropna().unique().tolist() if x])
selected_plants = st.sidebar.multiselect("매장(plant)", plant_options)

reorder_only = st.sidebar.checkbox("리오더 필요한 건만 보기", value=False)

if st.sidebar.button("새로고침"):
    st.cache_data.clear()
    st.rerun()


filtered_df = apply_filters(df, selected_style, selected_sku, selected_plants, reorder_only)

if filtered_df.empty:
    st.warning("조건에 맞는 데이터가 없습니다.")
    st.stop()


# =========================
# KPI 영역
# =========================
total_rows = len(filtered_df)
total_style = filtered_df["style_code"].nunique()
total_sku = filtered_df["sku"].nunique()
total_sales_2w = filtered_df["w1_sale_prev"].sum() + filtered_df["w2_sale_prev"].sum()
total_avg_sales = filtered_df["avg_sale_prev_2w"].sum()
total_reorder = filtered_df["sum_reorder_5w"].sum()
total_base_stock = filtered_df["base_stock"].sum()

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("행 수", f"{total_rows:,}")
c2.metric("스타일 수", f"{total_style:,}")
c3.metric("SKU 수", f"{total_sku:,}")
c4.metric("최근 2주 판매량 합", f"{total_sales_2w:,.0f}")
c5.metric("평균 판매량", f"{total_avg_sales:,.1f}")
c6.metric("총 리오더 필요 수량", f"{total_reorder:,.0f}")

st.divider()


# =========================
# 판매량 / 리오더 요약
# =========================
left, right = st.columns(2)

with left:
    st.subheader("스타일별 판매량 / 리오더")
    style_summary = (
        filtered_df.groupby("style_code", as_index=False)
        .agg(
            recent_sales=("avg_sale_prev_2w", "sum"),
            reorder_qty=("sum_reorder_5w", "sum"),
            base_stock=("base_stock", "sum"),
        )
        .sort_values(["reorder_qty", "recent_sales"], ascending=[False, False])
    )

    st.bar_chart(
        style_summary.set_index("style_code")[["recent_sales", "reorder_qty"]],
        use_container_width=True,
    )

with right:
    st.subheader("주차별 리오더 필요 수량")
    weekly_reorder = pd.DataFrame(
        {
            "week": ["w0", "w1", "w2", "w3", "w4"],
            "reorder_qty": [
                filtered_df["w0_reorder"].sum(),
                filtered_df["w1_reorder"].sum(),
                filtered_df["w2_reorder"].sum(),
                filtered_df["w3_reorder"].sum(),
                filtered_df["w4_reorder"].sum(),
            ],
            "lackplant_cnt": [
                filtered_df["w0_lackplant"].sum(),
                filtered_df["w1_lackplant"].sum(),
                filtered_df["w2_lackplant"].sum(),
                filtered_df["w3_lackplant"].sum(),
                filtered_df["w4_lackplant"].sum(),
            ],
        }
    )
    st.bar_chart(weekly_reorder.set_index("week")[["reorder_qty"]], use_container_width=True)

st.divider()


# =========================
# 상세 테이블
# =========================
st.subheader("상세 데이터")

detail_df = filtered_df[
    [
        "created_at",
        "style_code",
        "sku",
        "plant",
        "base_stock",
        "w1_sale_prev",
        "w2_sale_prev",
        "avg_sale_prev_2w",
        "w0_reorder",
        "w1_reorder",
        "w2_reorder",
        "w3_reorder",
        "w4_reorder",
        "sum_reorder_5w",
        "w0_lackplant",
        "w1_lackplant",
        "w2_lackplant",
        "w3_lackplant",
        "w4_lackplant",
        "sum_lackplant_5w",
        "total_reorder",
    ]
].copy()

detail_df = detail_df.sort_values(["sum_reorder_5w", "avg_sale_prev_2w"], ascending=[False, False])

st.dataframe(detail_df, use_container_width=True, height=500)


# =========================
# SKU별 요약
# =========================
st.subheader("SKU별 요약")
sku_summary = (
    filtered_df.groupby(["style_code", "sku"], as_index=False)
    .agg(
        plant_cnt=("plant", "nunique"),
        base_stock=("base_stock", "sum"),
        recent_sales=("avg_sale_prev_2w", "sum"),
        reorder_qty=("sum_reorder_5w", "sum"),
        lackplant_cnt=("sum_lackplant_5w", "sum"),
    )
    .sort_values(["reorder_qty", "recent_sales"], ascending=[False, False])
)

st.dataframe(sku_summary, use_container_width=True, height=350)


# =========================
# CSV 다운로드
# =========================
csv = detail_df.to_csv(index=False).encode("utf-8-sig")
st.download_button(
    label="상세 데이터 CSV 다운로드",
    data=csv,
    file_name="dashboard_sales_reorder_detail.csv",
    mime="text/csv",
)
