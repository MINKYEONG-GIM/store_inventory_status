import os
from typing import Optional
from io import BytesIO
import altair as alt
import json

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
def load_item_plc_data() -> pd.DataFrame:
    supabase = get_supabase_client()

    response = (
        supabase.table("item_plc")
        .select("item_code, year_week, sales, last_year_ratio_pct, week_no")
        .order("week_no")
        .execute()
    )

    data = response.data or []
    df = pd.DataFrame(data)

    if df.empty:
        return df

    df["week_no"] = pd.to_numeric(df["week_no"], errors="coerce")
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce").fillna(0)
    df["last_year_ratio_pct"] = pd.to_numeric(df["last_year_ratio_pct"], errors="coerce").fillna(0)

    return df


@st.cache_data(ttl=300)
def load_forecast_curve_data() -> pd.DataFrame:
    supabase = get_supabase_client()

    response = (
        supabase.table("sku_weekly_forecast_2")
        .select("style_code, sku, plant, year_week, sale_qty, week_no")
        .order("week_no")
        .execute()
    )

    data = response.data or []
    df = pd.DataFrame(data)

    if df.empty:
        return df

    df["week_no"] = pd.to_numeric(df["week_no"], errors="coerce")
    df["sale_qty"] = pd.to_numeric(df["sale_qty"], errors="coerce").fillna(0)

    df["sku"] = df["sku"].astype(str).str.strip().str.upper()
    df["plant"] = df["plant"].astype(str).str.strip().str.upper()
    df["year_week"] = df["year_week"].astype(str).str.strip()

    return df

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
    df["sku"] = df["sku"].astype(str).str.strip().str.upper()
    df["plant"] = df["plant"].astype(str).str.strip().str.upper()

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
    item_plc_df = load_item_plc_data()
    forecast_curve_df = load_forecast_curve_data()
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

# item_code 생성: MATERIAL 3~4번째 자리와 동일한 규칙
sku_summary["item_code"] = sku_summary["sku"].astype(str).str[2:4]

# item_plc 기준 PLC 생성
item_plc_curve_map = {}
if not item_plc_df.empty:
    plc_grouped = (
        item_plc_df.sort_values(["item_code", "week_no"])
        .groupby("item_code", dropna=False)
    )
    for item_code, g in plc_grouped:
        item_plc_curve_map[item_code] = [
            {
                "week_no": int(row["week_no"]) if pd.notna(row["week_no"]) else None,
                "value": float(row["last_year_ratio_pct"]) if pd.notna(row["last_year_ratio_pct"]) else 0.0,
                "label": str(row["year_week"]) if pd.notna(row["year_week"]) else ""
            }
            for _, row in g.iterrows()
        ]

# sku_weekly_forecast_2 기준 올해 예상 매출 PLC 생성
forecast_curve_map = {}
if not forecast_curve_df.empty:
    fc_grouped = (
        forecast_curve_df.sort_values(["sku", "plant", "week_no"])
        .groupby(["sku", "plant"], dropna=False)
    )
    for (sku, plant), g in fc_grouped:
        forecast_curve_map[(sku, plant)] = [
            {
                "week_no": int(row["week_no"]) if pd.notna(row["week_no"]) else None,
                "value": float(row["sale_qty"]) if pd.notna(row["sale_qty"]) else 0.0,
                "label": str(row["year_week"]) if pd.notna(row["year_week"]) else ""
            }
            for _, row in g.iterrows()
        ]

# SKU별로 매장(PLANT)별 예상곡선 묶기
plant_curve_map = {}
if not forecast_curve_df.empty:
    for sku, g_sku in forecast_curve_df.groupby("sku", dropna=False):
        plant_curve_map[sku] = {}
        for plant, g_plant in g_sku.sort_values("week_no").groupby("plant", dropna=False):
            plant_curve_map[sku][str(plant)] = [
                {
                    "week_no": int(row["week_no"]) if pd.notna(row["week_no"]) else None,
                    "value": float(row["sale_qty"]) if pd.notna(row["sale_qty"]) else 0.0,
                    "label": str(row["year_week"]) if pd.notna(row["year_week"]) else ""
                }
                for _, row in g_plant.iterrows()
            ]

sku_summary["기준 PLC"] = sku_summary["item_code"].map(
    lambda x: json.dumps(item_plc_curve_map.get(x, []), ensure_ascii=False)
)

sku_summary["올해 예상 매출 PLC"] = sku_summary["sku"].map(
    lambda x: json.dumps(plant_curve_map.get(x, {}), ensure_ascii=False)
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



# 화면 표시용 요약 테이블
grid_df = sku_summary.drop(columns=["item_code", "기준 PLC", "올해 예상 매출 PLC"], errors="ignore").copy()

st.dataframe(grid_df, use_container_width=True, height=350)






# =========================
# 상세 테이블
# =========================
st.subheader("매장별 상세 내역")

detail_source_df = filtered_df[
    [
        "style_code",
        "sku",
        "plant",
        "plant_nm",
        "base_stock",
        "total_reorder",
        "w1_sale_prev",
        "w2_sale_prev",
        "avg_sale_prev_2w",
        "w0_reorder",
        "w1_reorder",
        "w2_reorder",
        "w3_reorder",
        "w4_reorder",
    ]
].copy()

detail_source_df["item_code"] = detail_source_df["sku"].astype(str).str[2:4]

detail_df = detail_source_df.rename(columns={
    "base_stock": "현 매장재고",
    "plant_nm": "매장명",
    "avg_sale_prev_2w": "최근 2주 주판량",
    "w1_sale_prev": "전주 판매량",
    "w2_sale_prev": "2주전 판매량",
    "w0_reorder": "금주 부족수량",
    "w1_reorder": "w+1 부족수량",
    "w2_reorder": "w+2 부족수량",
    "w3_reorder": "w+3 부족수량",
    "w4_reorder": "w+4 부족수량",
    "total_reorder": "총 리오더수량"
})

detail_df["__idx"] = detail_df.index
detail_source_df["__idx"] = detail_source_df.index

detail_df = detail_df.sort_values(
    ["w+1 부족수량", "w+2 부족수량", "총 리오더수량"],
    ascending=[False, False, False]
).reset_index(drop=True)

detail_source_df = detail_source_df.set_index("__idx").loc[detail_df["__idx"]].reset_index(drop=True)
detail_df = detail_df.drop(columns="__idx")



detail_event = st.dataframe(
    detail_df,
    use_container_width=True,
    height=500,
    on_select="rerun",
    selection_mode="single-row"
)


# =========================
# CSV 다운로드
# =========================


output = BytesIO()
with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
    detail_df.to_excel(writer, index=False, sheet_name="detail")

st.download_button(
    label="엑셀 다운로드",
    data=output.getvalue(),
    file_name="dashboard_sales_reorder_detail.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)



# =========================
# 선택한 매장 상세 PLC 그래프
# =========================

st.subheader("선택한 매장 상세 PLC 그래프")

selected_rows = detail_event.selection.rows

if selected_rows:
    selected_idx = selected_rows[0]
    selected_row = detail_source_df.iloc[selected_idx]

    selected_sku = str(selected_row["sku"]).strip().upper()
    selected_plant = str(selected_row["plant"]).strip().upper()
    selected_item_code = str(selected_row["item_code"])

    # 파란선: item_plc 기준 PLC
    base_chart_df = item_plc_df[
        item_plc_df["item_code"].astype(str) == selected_item_code
    ].copy()
    base_chart_df = base_chart_df.sort_values("week_no")
    base_chart_df = base_chart_df[["week_no", "year_week", "last_year_ratio_pct"]].rename(
        columns={"last_year_ratio_pct": "기준 PLC"}
    )

    # 빨간선: sku_weekly_forecast_2 sale_qty
    current_chart_df = forecast_curve_df[
        (forecast_curve_df["sku"].astype(str).str.strip().str.upper() == selected_sku) &
        (forecast_curve_df["plant"].astype(str).str.strip().str.upper() == selected_plant)
    ].copy()
    
    st.write("선택 SKU:", selected_sku)
    st.write("선택 PLANT:", selected_plant)
    st.write("빨간선 데이터 건수:", len(current_chart_df))
    
    if current_chart_df.empty:
        st.write(
            forecast_curve_df[
                forecast_curve_df["sku"].astype(str).str.strip().str.upper() == selected_sku
            ][["sku", "plant", "year_week", "sale_qty", "week_no"]]
            .sort_values(["plant", "week_no"])
        )
    
    current_chart_df = current_chart_df.sort_values("week_no")
    current_chart_df = current_chart_df.drop_duplicates(subset=["week_no"])
    current_chart_df = current_chart_df[["week_no", "year_week", "sale_qty"]].rename(
        columns={"sale_qty": "올해 실판매 + 엔딩까지 예측"}
    )

    # week_no 기준 outer join
    chart_df = pd.merge(
        base_chart_df,
        current_chart_df,
        on="week_no",
        how="outer",
        suffixes=("_base", "_curr")
    ).sort_values("week_no")


    # year_week 보정
    chart_df["year_week"] = chart_df["year_week_curr"].combine_first(chart_df["year_week_base"])

    # week_no 없으면 제거
    chart_df = chart_df.dropna(subset=["week_no"]).copy()
    chart_df["week_no"] = pd.to_numeric(chart_df["week_no"], errors="coerce")

    st.markdown(f"**SKU: {selected_sku} / PLANT: {selected_plant}**")
    st.caption("그래프 계열값 | 파란선: 작년 기준 PLC | 빨간선: 올해 실판매 + 엔딩까지 예측")

    base_line = (
        alt.Chart(chart_df.dropna(subset=["기준 PLC"]))
        .mark_line(point=True, interpolate="linear", color="#1f77b4")
        .encode(
            x=alt.X("week_no:Q", title="주차"),
            y=alt.Y("기준 PLC:Q", title="기준 PLC", axis=alt.Axis(titleColor="#1f77b4")),
            tooltip=[
                alt.Tooltip("year_week:N", title="주차"),
                alt.Tooltip("week_no:Q", title="week_no"),
                alt.Tooltip("기준 PLC:Q", title="기준 PLC")
            ]
        )
        .properties(height=380)
    )

    current_line = (
        alt.Chart(chart_df.dropna(subset=["올해 실판매 + 엔딩까지 예측"]))
        .mark_line(point=True, interpolate="linear", color="#d62728", strokeWidth=3)
        .encode(
            x=alt.X("week_no:Q", title="주차"),
            y=alt.Y(
                "올해 실판매 + 엔딩까지 예측:Q",
                title="올해 실판매 + 엔딩까지 예측",
                axis=alt.Axis(titleColor="#d62728")
            ),
            tooltip=[
                alt.Tooltip("year_week:N", title="주차"),
                alt.Tooltip("week_no:Q", title="week_no"),
                alt.Tooltip("올해 실판매 + 엔딩까지 예측:Q", title="올해 실판매 + 엔딩까지 예측")
            ]
        )
    )

    legend_df = pd.DataFrame({
        "x": [1, 1],
        "y": [chart_df["기준 PLC"].max() if chart_df["기준 PLC"].notna().any() else 0,
              (chart_df["기준 PLC"].max() if chart_df["기준 PLC"].notna().any() else 0) * 0.9],
        "label": ["파란선: 작년 기준 PLC", "빨간선: 올해 실판매 + 엔딩까지 예측"],
        "color": ["#1f77b4", "#d62728"]
    })
    
    legend_text = (
        alt.Chart(legend_df)
        .mark_text(align="left", dx=10, fontSize=12)
        .encode(
            x=alt.value(10),
            y=alt.Y("y:Q"),
            text="label:N",
            color=alt.Color("color:N", scale=None)
        )
    )
    
    
    line_chart = alt.layer(base_line, current_line, legend_text).resolve_scale(
        y="independent"
    )

    st.altair_chart(line_chart, use_container_width=True)
else:
    st.info("매장별 상세내역 표 체크박스를 선택 시 그래프가 아래에 표시됩니다.")



