import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

try:
    from supabase import create_client as _create_supabase_client
except ImportError:
    _create_supabase_client = None


# -------------------------------------------------
# 페이지 기본 설정
# -------------------------------------------------
st.set_page_config(
    page_title="SKU Reorder Dashboard",
    page_icon="📦",
    layout="wide",
)


# -------------------------------------------------
# 공통 유틸
# -------------------------------------------------
def inject_custom_css() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(20,40,72,0.55), transparent 28%),
                radial-gradient(circle at top right, rgba(0,180,255,0.08), transparent 24%),
                linear-gradient(180deg, #06111f 0%, #071525 100%);
            color: #e9f3ff;
        }
        .block-container {
            padding-top: 1.2rem;
            padding-bottom: 1.0rem;
            max-width: 1440px;
        }
        .top-wrap {
            margin-bottom: 12px;
        }
        .top-tabs {
            display: flex;
            gap: 28px;
            margin: 14px 0 10px 0;
            border-bottom: 1px solid rgba(110, 160, 210, 0.18);
            padding-bottom: 8px;
        }
        .top-tab-active {
            color: #4ad4ff;
            font-weight: 700;
            border-bottom: 2px solid #37cfff;
            padding-bottom: 8px;
        }
        .top-tab {
            color: #9ab4d6;
            font-weight: 600;
            padding-bottom: 8px;
        }
        .metric-card {
            background: rgba(8, 21, 38, 0.86);
            border: 1px solid rgba(122, 172, 228, 0.14);
            border-radius: 14px;
            padding: 14px 16px;
            min-height: 88px;
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.02);
        }
        .metric-label {
            color: #8aa6c7;
            font-size: 12px;
            margin-bottom: 10px;
        }
        .metric-value {
            color: #f2f7ff;
            font-size: 30px;
            font-weight: 800;
            line-height: 1;
        }
        .metric-unit {
            color: #9fb6d2;
            font-size: 12px;
            margin-left: 4px;
        }
        .bar-card {
            background: rgba(8, 21, 38, 0.86);
            border: 1px solid rgba(122, 172, 228, 0.14);
            border-radius: 14px;
            padding: 14px 16px;
            min-height: 88px;
        }
        .bar-track {
            width: 100%;
            height: 6px;
            background: rgba(255,255,255,0.07);
            border-radius: 999px;
            overflow: hidden;
            margin-top: 12px;
        }
        .bar-fill {
            height: 100%;
            background: linear-gradient(90deg, #00c8ff 0%, #66e8ff 100%);
            border-radius: 999px;
        }
        .section-title {
            color: #e8f3ff;
            font-size: 26px;
            font-weight: 800;
            margin: 2px 0 4px 0;
        }
        .subtle {
            color: #8ea8c7;
            font-size: 13px;
            margin-bottom: 6px;
        }
        div[data-testid="stDataFrame"] {
            border: 1px solid rgba(122, 172, 228, 0.14);
            border-radius: 14px;
            overflow: hidden;
        }
        .sidebox {
            background: rgba(8, 21, 38, 0.92);
            border: 1px solid rgba(122, 172, 228, 0.14);
            border-radius: 14px;
            padding: 14px;
            margin-bottom: 12px;
        }
        .chip {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 700;
            margin-right: 6px;
            background: rgba(55, 207, 255, 0.12);
            color: #71deff;
            border: 1px solid rgba(55, 207, 255, 0.28);
        }
        .danger-chip {
            background: rgba(255, 92, 92, 0.12);
            color: #ff8080;
            border: 1px solid rgba(255, 92, 92, 0.28);
        }
        .select-hint {
            color: #7d95b3;
            font-size: 12px;
            margin-top: -2px;
            margin-bottom: 10px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


inject_custom_css()


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
def load_weekly_forecast() -> pd.DataFrame:
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
    df["created_at"] = pd.to_datetime(df.get("created_at"), errors="coerce")
    df["sale_end_date"] = pd.to_datetime(df.get("sale_end_date"), errors="coerce")

    numeric_cols = ["sale_qty", "last_year_ratio_pct", "BASE_STOCK_QTY", "loss", "IPGO_QTY", "week_no"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["style_code", "sku", "plant", "stage", "shape_type", "year_week"]:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("nan", "").str.strip()

    if "is_forecast" in df.columns:
        df["is_forecast"] = df["is_forecast"].apply(normalize_boolean)
    if "is_peak_week" in df.columns:
        df["is_peak_week"] = df["is_peak_week"].apply(normalize_boolean)

    df["year"] = df["year_week"].str[:4]
    return df


@st.cache_data(ttl=300)
def load_weekly_stock() -> pd.DataFrame:
    client = get_supabase_client()
    rows = fetch_supabase_table_all_rows(client, "weekly_stock")

    columns = [
        "id", "created_at", "year_week", "sku", "total_sale_qty", "total_base_stock_qty",
        "total_ipgo_qty", "total_loss", "cumulative_loss", "total_center_stock",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(rows)
    df["created_at"] = pd.to_datetime(df.get("created_at"), errors="coerce")
    for col in [
        "total_sale_qty", "total_base_stock_qty", "total_ipgo_qty", "total_loss",
        "cumulative_loss", "total_center_stock",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["sku", "year_week"]:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("nan", "").str.strip()

    df["year"] = df["year_week"].str[:4]
    df["week_no_num"] = pd.to_numeric(df["year_week"].str[-2:], errors="coerce")
    return df


# -------------------------------------------------
# 파생 계산
# -------------------------------------------------
def normalize_boolean(value) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ["true", "t", "1", "yes", "y"]
    return bool(value)


def safe_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


def get_latest_sku_snapshot(weekly_stock_df: pd.DataFrame) -> pd.DataFrame:
    if weekly_stock_df.empty:
        return pd.DataFrame(columns=[
            "sku", "year_week", "total_sale_qty", "total_base_stock_qty", "total_ipgo_qty",
            "total_loss", "cumulative_loss", "total_center_stock"
        ])

    ws = weekly_stock_df.copy()
    ws = ws.sort_values(["sku", "year_week", "created_at"], ascending=[True, False, False])
    latest = ws.drop_duplicates(subset=["sku"], keep="first").copy()
    return latest


def build_sku_master(forecast_df: pd.DataFrame, weekly_stock_df: pd.DataFrame) -> pd.DataFrame:
    if forecast_df.empty and weekly_stock_df.empty:
        return pd.DataFrame()

    base = forecast_df.copy()
    if not base.empty:
        base = base.sort_values(["sku", "created_at"], ascending=[True, False])
        sku_info = (
            base.groupby("sku", dropna=False)
            .agg(
                style_code=("style_code", "first"),
                stage=("stage", "first"),
                sale_end_date=("sale_end_date", "max"),
                latest_created_at=("created_at", "max"),
            )
            .reset_index()
        )
    else:
        sku_info = pd.DataFrame(columns=["sku", "style_code", "stage", "sale_end_date", "latest_created_at"])

    latest_stock = get_latest_sku_snapshot(weekly_stock_df)

    # 현재 이후 예측 기준 집계
    if not forecast_df.empty:
        current_week = datetime.today().isocalendar().week
        fw = forecast_df.copy()
        fw["week_no"] = pd.to_numeric(fw["week_no"], errors="coerce")
        future = fw[fw["week_no"].fillna(0) >= current_week].copy()

        future_summary = (
            future.groupby("sku", dropna=False)
            .agg(
                future_sale_qty=("sale_qty", "sum"),
                plant_count=("plant", lambda x: x.astype(str).replace("", pd.NA).dropna().nunique()),
                max_week_no=("week_no", "max"),
            )
            .reset_index()
        )

        plant_now = (
            fw.groupby(["sku", "plant"], dropna=False)
            .agg(
                plant_sale_qty=("sale_qty", "sum"),
                plant_base_stock_qty=("BASE_STOCK_QTY", "max"),
                plant_ipgo_qty=("IPGO_QTY", "sum"),
                plant_loss_qty=("loss", "max"),
                plant_peak_count=("is_peak_week", "sum"),
                latest_stage=("stage", "last"),
            )
            .reset_index()
        )
    else:
        future_summary = pd.DataFrame(columns=["sku", "future_sale_qty", "plant_count", "max_week_no"])
        plant_now = pd.DataFrame(columns=[
            "sku", "plant", "plant_sale_qty", "plant_base_stock_qty", "plant_ipgo_qty", "plant_loss_qty",
            "plant_peak_count", "latest_stage"
        ])

    sku_df = sku_info.merge(latest_stock, on="sku", how="outer")
    sku_df = sku_df.merge(future_summary, on="sku", how="left")

    for col in [
        "total_sale_qty", "total_base_stock_qty", "total_ipgo_qty", "total_loss",
        "cumulative_loss", "total_center_stock", "future_sale_qty", "plant_count"
    ]:
        if col in sku_df.columns:
            sku_df[col] = pd.to_numeric(sku_df[col], errors="coerce").fillna(0)

    sku_df["plant_count"] = sku_df["plant_count"].fillna(0).astype(int)
    sku_df["display_year"] = sku_df["year_week"].astype(str).str[:4]
    sku_df["risk_score"] = (
        sku_df["cumulative_loss"] * 3
        + sku_df["total_loss"] * 2
        + (sku_df["future_sale_qty"] - sku_df["total_base_stock_qty"] - sku_df["total_center_stock"]).clip(lower=0)
    )

    # 화면용 KPI
    sku_df["need_reorder_qty"] = (
        sku_df["future_sale_qty"] - sku_df["total_base_stock_qty"] - sku_df["total_center_stock"]
    ).clip(lower=0)
    sku_df["sell_through"] = (
        sku_df["total_sale_qty"] /
        (sku_df["total_sale_qty"] + sku_df["total_base_stock_qty"] + 1e-9)
    ).fillna(0)
    sku_df["inventory_coverage"] = (
        (sku_df["total_base_stock_qty"] + sku_df["total_center_stock"]) /
        (sku_df["future_sale_qty"] + 1e-9)
    ).fillna(0)

    def classify_status(row: pd.Series) -> str:
        if row["need_reorder_qty"] > 0 and row["cumulative_loss"] > 0:
            return "리오더 필요"
        if row["need_reorder_qty"] > 0:
            return "리오더 검토"
        if row["cumulative_loss"] > 0:
            return "관리 필요"
        return "안정"

    sku_df["status"] = sku_df.apply(classify_status, axis=1)

    # 메인 테이블 정렬
    sku_df = sku_df.sort_values(
        ["risk_score", "need_reorder_qty", "future_sale_qty", "sku"],
        ascending=[False, False, False, True],
        na_position="last",
    ).reset_index(drop=True)

    return sku_df, plant_now


# -------------------------------------------------
# 필터
# -------------------------------------------------
def apply_filters(sku_df: pd.DataFrame) -> pd.DataFrame:
    st.markdown('<div class="section-title">상세 내역</div>', unsafe_allow_html=True)

    c1, c2, c3, c4, c5 = st.columns([0.9, 0.9, 0.9, 2.0, 0.7])

    year_options = sorted([x for x in sku_df["display_year"].dropna().astype(str).unique().tolist() if x])
    year_selected = c1.selectbox("연도", options=["전체"] + year_options, index=0)

    stage_options = sorted([x for x in sku_df["stage"].dropna().astype(str).unique().tolist() if x])
    stage_selected = c2.selectbox("스테이지", options=["전체"] + stage_options, index=0)

    status_options = ["전체", "리오더 필요", "리오더 검토", "관리 필요", "안정"]
    status_selected = c3.selectbox("상태", options=status_options, index=0)

    keyword = c4.text_input("검색", placeholder="아이템 또는 스타일코드 검색")
    _ = c5.button("조회", use_container_width=True)

    filtered = sku_df.copy()

    if year_selected != "전체":
        filtered = filtered[filtered["display_year"] == year_selected]

    if stage_selected != "전체":
        filtered = filtered[filtered["stage"].astype(str) == stage_selected]

    if status_selected != "전체":
        filtered = filtered[filtered["status"] == status_selected]

    if keyword:
        keyword = keyword.strip()
        filtered = filtered[
            filtered["sku"].astype(str).str.contains(keyword, case=False, na=False)
            | filtered["style_code"].astype(str).str.contains(keyword, case=False, na=False)
        ]

    return filtered.reset_index(drop=True)


# -------------------------------------------------
# 상단 영역
# -------------------------------------------------
def render_top_summary(df: pd.DataFrame) -> None:
    reorder_style_cnt = df.loc[df["need_reorder_qty"] > 0, "style_code"].astype(str).nunique()
    reorder_qty = int(round(df["need_reorder_qty"].sum()))
    reorder_sku_cnt = int((df["need_reorder_qty"] > 0).sum())
    avg_sell_through = float(df["sell_through"].mean() * 100) if not df.empty else 0.0
    portfolio_ratio = float((df["status"] != "안정").mean() * 100) if not df.empty else 0.0

    st.markdown('<div class="top-wrap">', unsafe_allow_html=True)
    m1, m2, m3, m4, m5, m6 = st.columns([1.0, 1.0, 1.0, 1.0, 1.7, 0.7])

    with m1:
        st.markdown(
            f'''<div class="metric-card"><div class="metric-label">리오더 스타일 수</div><div class="metric-value">{reorder_style_cnt}</div></div>''',
            unsafe_allow_html=True,
        )
    with m2:
        st.markdown(
            f'''<div class="metric-card"><div class="metric-label">리오더 발주액</div><div class="metric-value">0<span class="metric-unit">원</span></div></div>''',
            unsafe_allow_html=True,
        )
    with m3:
        st.markdown(
            f'''<div class="metric-card"><div class="metric-label">리오더 발주량</div><div class="metric-value">{reorder_qty:,}<span class="metric-unit">pcs</span></div></div>''',
            unsafe_allow_html=True,
        )
    with m4:
        st.markdown(
            f'''<div class="metric-card"><div class="metric-label">리오더 위험률</div><div class="metric-value">{avg_sell_through:,.1f}<span class="metric-unit">%</span></div></div>''',
            unsafe_allow_html=True,
        )
    with m5:
        st.markdown(
            f'''
            <div class="bar-card">
                <div class="metric-label">생산지 포트폴리오</div>
                <div style="color:#dff6ff;font-size:14px;font-weight:700;">{portfolio_ratio:,.1f}%</div>
                <div class="bar-track"><div class="bar-fill" style="width:{min(max(portfolio_ratio, 0), 100)}%;"></div></div>
            </div>
            ''',
            unsafe_allow_html=True,
        )
    with m6:
        planner_options = ["전체"]
        st.selectbox("기획자", planner_options, index=0)

    st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('<div class="top-tabs"><div class="top-tab-active">상세 내역</div><div class="top-tab">리오더 확정</div></div>', unsafe_allow_html=True)


# -------------------------------------------------
# 메인 SKU 테이블
# -------------------------------------------------
def format_main_table(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["주판매율"] = (out["sell_through"] * 100).round(1).astype(str) + "%"
    out["달성율"] = ((out["inventory_coverage"] * 100).clip(upper=999)).round(1).astype(str) + "%"
    out["리오더수량"] = out["need_reorder_qty"].round(0).astype(int)
    out["주판매량"] = out["future_sale_qty"].round(0).astype(int)
    out["실적"] = out["total_sale_qty"].round(0).astype(int)
    out["원가율"] = out["cumulative_loss"].round(1)
    out["보정판매기간"] = out["stage"].replace({"": "-"})
    out["연도"] = out["display_year"].replace({"": "-"})
    out["시즌"] = "-"
    out["복종"] = "-"
    out["아이템"] = "-"
    out["기획자"] = "-"
    out["스타일코드"] = out["style_code"].replace({"": "-"})
    out["SKU"] = out["sku"].replace({"": "-"})
    out["상태"] = out["status"]
    out["센터재고"] = out["total_center_stock"].round(0).astype(int)
    out["가용재고"] = out["total_base_stock_qty"].round(0).astype(int)
    out["피크주수"] = out["plant_count"].astype(int)

    return out[[
        "연도", "시즌", "복종", "아이템", "기획자", "스타일코드", "SKU",
        "실적", "달성율", "주판매율", "주판매량", "원가율", "보정판매기간",
        "가용재고", "센터재고", "리오더수량", "피크주수", "상태"
    ]]


# -------------------------------------------------
# 사이드 상세 패널
# -------------------------------------------------
def get_selected_sku_from_table(filtered_df: pd.DataFrame) -> Optional[str]:
    options = filtered_df["sku"].astype(str).tolist()
    if not options:
        return None

    st.markdown('<div class="select-hint">SKU를 하나 선택하면 오른쪽에서 매장별 상세를 볼 수 있습니다.</div>', unsafe_allow_html=True)
    selected = st.selectbox("선택 SKU", options=options, label_visibility="collapsed")
    return selected


def render_side_detail(selected_sku: Optional[str], sku_df: pd.DataFrame, plant_df: pd.DataFrame) -> None:
    st.markdown('<div class="sidebox">', unsafe_allow_html=True)
    st.markdown("### SKU 상세")

    if not selected_sku:
        st.info("왼쪽 표에서 SKU를 선택하세요.")
        st.markdown('</div>', unsafe_allow_html=True)
        return

    sku_row = sku_df[sku_df["sku"].astype(str) == str(selected_sku)]
    if sku_row.empty:
        st.info("선택한 SKU 정보를 찾지 못했습니다.")
        st.markdown('</div>', unsafe_allow_html=True)
        return

    r = sku_row.iloc[0]

    chip_class = "danger-chip" if r["need_reorder_qty"] > 0 else "chip"
    st.markdown(
        f'<span class="chip">{r.get("style_code", "-")}</span><span class="chip {chip_class}">{r.get("status", "-")}</span>',
        unsafe_allow_html=True,
    )

    d1, d2 = st.columns(2)
    d1.metric("SKU", str(r.get("sku") or "-"))
    d2.metric("스테이지", str(r.get("stage") or "-"))

    d3, d4 = st.columns(2)
    d3.metric("총 판매", int(round(float(r.get("total_sale_qty") or 0))))
    d4.metric("리오더 필요", int(round(float(r.get("need_reorder_qty") or 0))))

    d5, d6 = st.columns(2)
    d5.metric("매장재고 합", int(round(float(r.get("total_base_stock_qty") or 0))))
    d6.metric("센터재고", int(round(float(r.get("total_center_stock") or 0))))

    st.markdown("#### 매장별 세부 내용")
    detail = plant_df[plant_df["sku"].astype(str) == str(selected_sku)].copy()
    if detail.empty:
        st.info("매장별 상세 데이터가 없습니다.")
        st.markdown('</div>', unsafe_allow_html=True)
        return

    detail["매장"] = detail["plant"].replace({"": "-"})
    detail["판매량"] = safe_num(detail["plant_sale_qty"]).round(0).astype(int)
    detail["재고"] = safe_num(detail["plant_base_stock_qty"]).round(0).astype(int)
    detail["입고"] = safe_num(detail["plant_ipgo_qty"]).round(0).astype(int)
    detail["loss"] = safe_num(detail["plant_loss_qty"]).round(0).astype(int)
    detail["피크주"] = safe_num(detail["plant_peak_count"]).round(0).astype(int)
    detail["stage"] = detail["latest_stage"].replace({"": "-"})

    detail = detail[["매장", "판매량", "재고", "입고", "loss", "피크주", "stage"]].sort_values(
        ["loss", "판매량", "재고", "매장"],
        ascending=[False, False, True, True],
    )

    st.dataframe(detail, use_container_width=True, hide_index=True, height=520)
    st.markdown('</div>', unsafe_allow_html=True)


# -------------------------------------------------
# 실행
# -------------------------------------------------
def main() -> None:
    st.markdown('<div class="section-title">SKU 리오더 대시보드</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtle">메인 목록은 SKU 단위, 오른쪽 패널은 클릭한 SKU의 매장별 세부 정보입니다.</div>', unsafe_allow_html=True)

    try:
        forecast_df = load_weekly_forecast()
        weekly_stock_df = load_weekly_stock()
    except Exception as e:
        st.error(f"데이터를 불러오지 못했습니다: {e}")
        st.stop()

    if forecast_df.empty and weekly_stock_df.empty:
        st.warning("sku_weekly_forecast_2, weekly_stock 테이블에 데이터가 없습니다.")
        st.stop()

    sku_df, plant_df = build_sku_master(forecast_df, weekly_stock_df)
    if sku_df.empty:
        st.warning("SKU 기준으로 묶을 데이터가 없습니다.")
        st.stop()

    render_top_summary(sku_df)
    filtered_df = apply_filters(sku_df)

    left, right = st.columns([3.8, 1.5], gap="large")

    with left:
        st.caption(f"{len(filtered_df):,}건")
        view_df = format_main_table(filtered_df)
        st.dataframe(view_df, use_container_width=True, hide_index=True, height=760)
        selected_sku = get_selected_sku_from_table(filtered_df)

    with right:
        render_side_detail(selected_sku, sku_df, plant_df)


if __name__ == "__main__":
    main()
