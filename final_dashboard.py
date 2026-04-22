import os
from typing import Optional

import pandas as pd
import streamlit as st
from supabase import create_client, Client


# =========================
# 기본 설정
# =========================
st.set_page_config(page_title="SCM Agent", layout="wide")
st.title("SCM Agent")
st.caption("매장별 SKU단위로 측정하여 리오더수량 제안합니다")


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
            "id, created_at, style_code, sku, plant, plant_nm, total_reorder, "
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
total_style = filtered_df["style_code"].nunique()
total_sku = filtered_df["sku"].nunique()

# 금주(w0) 리오더 필요 스타일 / SKU 수
w0_reorder_df = filtered_df[filtered_df["w0_reorder"] > 0]
w0_reorder_style_cnt = w0_reorder_df["style_code"].nunique()
w0_reorder_sku_cnt = w0_reorder_df["sku"].nunique()

# 차주(w1) 리오더 필요 스타일 / SKU 수
w1_reorder_df = filtered_df[filtered_df["w1_reorder"] > 0]
w1_reorder_style_cnt = w1_reorder_df["style_code"].nunique()
w1_reorder_sku_cnt = w1_reorder_df["sku"].nunique()

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("총 스타일수", f"{total_style:,}")
c2.metric("총 SKU수", f"{total_sku:,}")
c3.metric("금주 리오더필요 스타일", f"{w0_reorder_style_cnt:,}")
c4.metric("금주 리오더필요 SKU", f"{w0_reorder_sku_cnt:,}")
c5.metric("차주 리오더필요 스타일", f"{w1_reorder_style_cnt:,}")
c6.metric("차주 리오더필요 SKU", f"{w1_reorder_sku_cnt:,}")

st.divider()

# =========================
# SKU별 요약
# =========================
st.subheader("상세 내역")
sku_summary = (
    filtered_df.groupby(["style_code", "sku"], as_index=False)
    .agg(
        base_stock=("base_stock", "sum"),
        total_sales=("avg_sale_prev_2w", "sum"),
        total_reorder=("total_reorder", "sum"),

        w0_loss=("w0_reorder", "sum"),
        w0_lackplant=("w0_lackplant", "sum"),

        w1_loss=("w1_reorder", "sum"),
        w1_lackplant=("w1_lackplant", "sum"),

        w2_loss=("w2_reorder", "sum"),
        w2_lackplant=("w2_lackplant", "sum"),

        w3_loss=("w3_reorder", "sum"),
        w3_lackplant=("w3_lackplant", "sum"),

        w4_loss=("w4_reorder", "sum"),
        w4_lackplant=("w4_lackplant", "sum"),
    )
    .sort_values(["total_reorder", "total_sales"], ascending=[False, False])
)

sku_summary = sku_summary.rename(columns={
    "base_stock": "현 총 매장재고",
    "total_sales": "누적 판매량",
    "total_reorder": "총 리오더 필요수량\n엔딩까지",

    "w0_loss": "금주 예상 매출 loss",
    "w0_lackplant": "금주 부족매장수",

    "w1_loss": "W+1 부족수량",
    "w1_lackplant": "W+1 부족매장수",

    "w2_loss": "W+2 부족수량",
    "w2_lackplant": "W+2 부족매장수",

    "w3_loss": "W+3 부족수량",
    "w3_lackplant": "W+3 부족매장수",

    "w4_loss": "W+4 부족수량",
    "w4_lackplant": "W+4 부족매장수",
})

st.dataframe(sku_summary, use_container_width=True, height=350)

# =========================
# 상세 테이블
# =========================
st.subheader("매장별 상세 내역")

detail_df = filtered_df[
    [  "style_code",
        "sku",
        "plant",
        "plant_nm",
        "base_stock",
        "total_reorder",
        "w1_sale_prev",
        "w2_sale_prev",
        "avg_sale_prev_2w",
        "w0_reorder",
        "w0_lackplant",
        "w1_reorder",
        "w1_lackplant",
        "w2_reorder",
        "w2_lackplant",
        "w3_reorder",
        "w3_lackplant",
        "w4_reorder",
        "w4_lackplant",
        
    ]
].copy()
# -- 상세 내역 열 이름(컬럼명) 수정
detail_df = detail_df.rename(columns={
    "base_stock": "현 매장재고",
    "plant_nm": "매장명",
    "avg_sale_prev_2w": "최근 2주 주판량",
    "w1_sale_prev": "전주 판매량", 
    "w2_sale_prev": "2주전 판매량",
    "w0_reorder": "금주 부족수량", 
    "w0_lackplant": "금주 부족매장수", 
    "w1_reorder": "w+1 부족수량", 
    "w1_lackplant": "w+1 부족매장수", 
    "w2_reorder": "w+2 부족수량", 
    "w2_lackplant": "w+2 부족매장수",
    "w3_reorder": "w+3 부족수량", 
    "w3_lackplant": "w+3 부족매장수", 
    "w4_reorder": "w+4 부족수량", 
    "w4_lackplant": "w+4 부족매장수", 
    "total_reorder": "총 리오더수량"
    
    
})

detail_df = detail_df.sort_values(["w+1 부족수량",  "w+2 부족수량",  "총 리오더수량" ], ascending=[False, False, False])

st.dataframe(detail_df, use_container_width=True, height=500)




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
