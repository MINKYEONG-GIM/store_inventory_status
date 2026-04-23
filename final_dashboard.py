import os
from typing import Optional
from io import BytesIO

import json
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode

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

# 화면 표시용 컬럼만 분리
grid_df = sku_summary.drop(columns=["item_code", "기준 PLC", "올해 예상 매출 PLC"], errors="ignore").copy()


tooltip_js = JsCode("""
class CustomTooltip {
  init(params) {
    const eGui = document.createElement('div');
    eGui.style.background = 'white';
    eGui.style.border = '1px solid #d9d9d9';
    eGui.style.borderRadius = '8px';
    eGui.style.padding = '12px';
    eGui.style.boxShadow = '0 4px 12px rgba(0,0,0,0.15)';
    eGui.style.maxWidth = '520px';

    const makeSvgLine = (series, title) => {
      if (!series || series.length === 0) {
        return `<div style="margin-bottom:10px;"><div style="font-weight:600; margin-bottom:6px;">${title}</div><div>데이터 없음</div></div>`;
      }

      const w = 460;
      const h = 140;
      const p = 24;
      const values = series.map(x => Number(x.value || 0));
      const labels = series.map(x => x.label || '');
      const minV = Math.min(...values, 0);
      const maxV = Math.max(...values, 1);
      const range = maxV - minV || 1;

      const pts = values.map((v, i) => {
        const x = p + (i * (w - p * 2) / Math.max(series.length - 1, 1));
        const y = h - p - ((v - minV) / range) * (h - p * 2);
        return `${x},${y}`;
      }).join(' ');

      const circles = values.map((v, i) => {
        const x = p + (i * (w - p * 2) / Math.max(series.length - 1, 1));
        const y = h - p - ((v - minV) / range) * (h - p * 2);
        return `<circle cx="${x}" cy="${y}" r="3" fill="#2563eb">
                  <title>${labels[i]} : ${v}</title>
                </circle>`;
      }).join('');

      return `
        <div style="margin-bottom:12px;">
          <div style="font-weight:600; margin-bottom:6px;">${title}</div>
          <svg width="${w}" height="${h}">
            <line x1="${p}" y1="${h-p}" x2="${w-p}" y2="${h-p}" stroke="#999" />
            <line x1="${p}" y1="${p}" x2="${p}" y2="${h-p}" stroke="#999" />
            <polyline fill="none" stroke="#2563eb" stroke-width="2" points="${pts}" />
            ${circles}
          </svg>
        </div>
      `;
    };

    let baseSeries = [];
    let forecastMap = {};

    try { baseSeries = JSON.parse(params.data["기준 PLC"] || "[]"); } catch(e) {}
    try { forecastMap = JSON.parse(params.data["올해 예상 매출 PLC"] || "{}"); } catch(e) {}

    let html = `<div style="font-weight:700; margin-bottom:8px;">SKU: ${params.data.sku}</div>`;
    html += makeSvgLine(baseSeries, 'item_plc(기준 PLC)');

    const plants = Object.keys(forecastMap || {});
    if (plants.length === 0) {
      html += `<div><div style="font-weight:600; margin-bottom:6px;">올해 예상 매출 PLC</div><div>데이터 없음</div></div>`;
    } else {
      plants.forEach(plant => {
        html += makeSvgLine(forecastMap[plant], `올해 예상 매출 PLC (${plant})`);
      });
    }

    eGui.innerHTML = html;
    this.eGui = eGui;
  }

  getGui() {
    return this.eGui;
  }
}
""")

gb = GridOptionsBuilder.from_dataframe(grid_df)

gb.configure_default_column(
    resizable=True,
    sortable=True,
    filter=True,
    floatingFilter=True,
)

gb.configure_column(
    "sku",
    header_name="sku",
    tooltipField="기준 PLC",
    tooltipComponent=tooltip_js
)

grid_options = gb.build()

# tooltip에서 숨김 데이터 참조할 수 있게 rowData 재구성
row_data = sku_summary.to_dict("records")
grid_options["rowData"] = row_data
grid_options["tooltipShowDelay"] = 100
grid_options["tooltipMouseTrack"] = True

AgGrid(
    None,
    gridOptions=grid_options,
    allow_unsafe_jscode=True,
    update_mode="NO_UPDATE",
    data_return_mode="AS_INPUT",
    use_container_width=True,
    height=350,
    fit_columns_on_grid_load=False,
    theme="streamlit",
)

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
        "w1_reorder",
        "w2_reorder",
        "w3_reorder",
        "w4_reorder",
        
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
    "w1_reorder": "w+1 부족수량", 
    "w2_reorder": "w+2 부족수량", 
    "w3_reorder": "w+3 부족수량", 
    "w4_reorder": "w+4 부족수량", 
    "total_reorder": "총 리오더수량"
    
    
})

detail_df = detail_df.sort_values(["w+1 부족수량",  "w+2 부족수량",  "총 리오더수량" ], ascending=[False, False, False])

st.dataframe(detail_df, use_container_width=True, height=500)




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
