import math
from datetime import datetime
import pandas as pd
import streamlit as st
from supabase import create_client, Client
import re

# =========================
# 환경설정
# =========================
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

SOURCE_TABLE = "sku_weekly_forecast"
TARGET_TABLE = "sku_weekly_forecast_2"
ITEM_PLC_TABLE = "item_plc"

MAX_WEEK = 52


# =========================
# 공통 함수
# =========================
def fetch_all_rows(table_name: str, filters: dict = None, order_by: str = None):
    """
    Supabase 1000건 제한 때문에 페이지 단위로 전부 가져오기
    """
    all_rows = []
    page_size = 1000
    offset = 0

    while True:
        query = supabase.table(table_name).select("*")
        if filters:
            for col, val in filters.items():
                query = query.eq(col, val)
        if order_by:
            query = query.order(order_by)

        res = query.range(offset, offset + page_size - 1).execute()
        data = res.data or []

        if not data:
            break

        all_rows.extend(data)

        if len(data) < page_size:
            break

        offset += page_size

    return all_rows


def safe_numeric(x, default=0):
    if x is None:
        return default
    try:
        return float(x)
    except:
        return default


def safe_int(x, default=0):
    if x is None:
        return default
    try:
        return int(x)
    except:
        return default


def round_sale(x):
    """
    판매량 반올림 규칙
    """
    if pd.isna(x):
        return 0
    return int(round(float(x)))


# =========================
# item_plc 가공
# =========================
def load_item_plc():
    rows = fetch_all_rows(ITEM_PLC_TABLE, order_by="week_no")
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df["week_no"] = pd.to_numeric(df["week_no"], errors="coerce")
    df["last_year_ratio_pct"] = pd.to_numeric(df["last_year_ratio_pct"], errors="coerce").fillna(0)

    # item_code + week_no 기준으로 첫 행만 사용
    df = df.sort_values(["item_code", "week_no", "id"]).drop_duplicates(
        subset=["item_code", "week_no"], keep="first"
    )

    return df


def build_item_plc_map(item_plc_df: pd.DataFrame):
    """
    (item_code, week_no) -> plc 정보
    """
    plc_map = {}

    if item_plc_df.empty:
        return plc_map

    for _, row in item_plc_df.iterrows():
        key = (str(row["item_code"]), int(row["week_no"]))
        plc_map[key] = {
            "last_year_ratio_pct": safe_numeric(row.get("last_year_ratio_pct"), 0),
            "shape_type": row.get("shape_type"),
            "stage": row.get("stage"),
            "peak_week": row.get("peak_week"),
        }

    return plc_map


def get_plc_info(plc_map, item_code, week_no):
    if pd.isna(week_no):
        return {
            "last_year_ratio_pct": 0,
            "shape_type": None,
            "stage": None,
            "peak_week": None,
        }

    week_no = int(week_no)
    item_code = str(item_code).strip()

    # 1️⃣ 먼저 item_code로 찾기
    key = (item_code, week_no)
    if key in plc_map:
        return plc_map[key]

    # 2️⃣ 없으면 "평균"으로 fallback
    avg_key = ("평균", week_no)
    if avg_key in plc_map:
        return plc_map[avg_key]

    # 3️⃣ 그래도 없으면 0
    return {
        "last_year_ratio_pct": 0,
        "shape_type": None,
        "stage": None,
        "peak_week": None,
    }


# =========================
# 실제 데이터 로드
# =========================
def load_actual_style_data(style_code: str):
    rows = fetch_all_rows(SOURCE_TABLE, filters={"style_code": style_code}, order_by="week_no")
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df["week_no"] = pd.to_numeric(df["week_no"], errors="coerce")
    df["SALE_QTY"] = pd.to_numeric(df["SALE_QTY"], errors="coerce").fillna(0)
    df["BASE_STOCK_QTY"] = pd.to_numeric(df["BASE_STOCK_QTY"], errors="coerce").fillna(0).astype(int)
    df["IPGO_QTY"] = pd.to_numeric(df["IPGO_QTY"], errors="coerce").fillna(0).astype(int)

    # item_code 컬럼 비어있는 경우 문자열 처리
    df["item_code"] = df["item_code"].fillna("").astype(str)

    return df


# =========================
# 예측값 생성
# =========================
def build_forecast_rows(actual_df: pd.DataFrame, item_plc_df: pd.DataFrame):
    if actual_df.empty:
        return pd.DataFrame()

    plc_map = build_item_plc_map(item_plc_df)

    result_rows = []

    # 실제 데이터도 target 스키마로 맞춰서 먼저 넣기
    for _, row in actual_df.iterrows():
        plc_info = get_plc_info(plc_map, row["item_code"], row["week_no"])

        result_rows.append({
            "year_week": row["year_week"],
            "sale_qty": safe_numeric(row["SALE_QTY"], 0),
            "stage": plc_info["stage"],
            "style_code": row["style_code"],
            "sku": row["sku"],
            "is_peak_week": bool(plc_info["peak_week"] == safe_int(row["week_no"], 0)) if plc_info["peak_week"] is not None else False,
            "plant": row["plant"],
            "last_year_ratio_pct": safe_numeric(plc_info["last_year_ratio_pct"], 0),
            "BASE_STOCK_QTY": safe_int(row["BASE_STOCK_QTY"], 0),
            "is_forecast": False,
            "loss": 0,
            "IPGO_QTY": safe_int(row["IPGO_QTY"], 0),
            "shape_type": plc_info["shape_type"],
            "week_no": safe_numeric(row["week_no"], 0),
            "sale_end_date": None,
        })

    # plant + sku 단위로 미래 주차 생성
    group_cols = ["style_code", "sku", "plant", "item_code"]

    grouped = actual_df.groupby(group_cols, dropna=False)

    for (style_code, sku, plant, item_code), g in grouped:
        g = g.sort_values("week_no").copy()

        # 실제 주차만 사용
        actual_only = g[g["week_no"].notna()].copy()
        if actual_only.empty:
            continue

        last_actual_week = int(actual_only["week_no"].max())
        last_row = actual_only.sort_values("week_no").iloc[-1]
        
        last_base_stock = safe_int(last_row["BASE_STOCK_QTY"], 0)
        last_sale_qty = safe_int(last_row["SALE_QTY"], 0)
        last_ipgo_qty = safe_int(last_row["IPGO_QTY"], 0)
        
        prev_base_stock = max(0, last_base_stock + last_ipgo_qty - last_sale_qty)


        
        # 최근 2주 actual
        recent2 = actual_only.sort_values("week_no").tail(2).copy()

        recent_2w_avg_sale = recent2["SALE_QTY"].mean() if not recent2.empty else 0

        # 최근 2주의 ratio 평균
        recent_ratios = []
        for _, r in recent2.iterrows():
            info = get_plc_info(plc_map, item_code, r["week_no"])
            recent_ratios.append(safe_numeric(info["last_year_ratio_pct"], 0))

        recent_2w_avg_ratio = sum(recent_ratios) / len(recent_ratios) if recent_ratios else 0

        # 연간 총 판매량 추정
        if recent_2w_avg_ratio > 0:
            estimated_total_sales = recent_2w_avg_sale / (recent_2w_avg_ratio / 100.0)
        else:
            estimated_total_sales = 0

        # 미래 주차 생성
        for future_week in range(last_actual_week + 1, MAX_WEEK + 1):
            plc_info = get_plc_info(plc_map, item_code, future_week)
            future_ratio = safe_numeric(plc_info["last_year_ratio_pct"], 0)

            pred_sale = estimated_total_sales * (future_ratio / 100.0)
            pred_sale = round_sale(pred_sale)
            base_stock = max(0, prev_base_stock - pred_sale)
            loss = max(0, pred_sale - prev_base_stock)


            prev_base_stock = base_stock

            result_rows.append({
                "year_week": f"2026-{future_week:02d}",  # 필요하면 실제 연도로 바꾸기
                "sale_qty": pred_sale,
                "stage": plc_info["stage"],
                "style_code": style_code,
                "sku": sku,
                "is_peak_week": bool(plc_info["peak_week"] == future_week) if plc_info["peak_week"] is not None else False,
                "plant": plant,
                "last_year_ratio_pct": future_ratio,
                "BASE_STOCK_QTY": base_stock,
                "is_forecast": True,
                "loss": loss,
                "IPGO_QTY": 0,
                "shape_type": plc_info["shape_type"],
                "week_no": future_week,
                "sale_end_date": None,
            })

    result_df = pd.DataFrame(result_rows)

    # 중복 방지
    result_df = result_df.sort_values(
        by=["style_code", "sku", "plant", "week_no", "is_forecast"]
    ).drop_duplicates(
        subset=["style_code", "sku", "plant", "week_no"],
        keep="first"
    )

    return result_df


# =========================
# 적재
# =========================
def delete_target_style(style_code: str):
    supabase.table(TARGET_TABLE).delete().eq("style_code", style_code).execute()


def upsert_target_rows(df: pd.DataFrame):
    if df.empty:
        return 0

    rows = df.to_dict(orient="records")
    batch_size = 500
    affected = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        supabase.table(TARGET_TABLE).upsert(
            batch,
            on_conflict="style_code,sku,plant,week_no"
        ).execute()
        affected += len(batch)

    return affected


# =========================
# 화면
# =========================
st.set_page_config(page_title="SKU Weekly Forecast Builder", layout="wide")

st.title("sku_weekly_forecast_2 적재")
st.write("스타일코드를 입력하면 해당 스타일의 실제 데이터는 그대로 가져오고, 마지막 실제 주차 다음 주부터 52주차까지 예측해서 sku_weekly_forecast_2에 적재합니다.")


style_input = st.text_area("스타일코드 입력 (콤마 또는 줄바꿈)", value="")
style_codes = [s.strip() for s in re.split(r"[\n,]+", style_input) if s.strip()]

run_button = st.button("적재 실행")

if run_button:
    if not style_codes:
        st.error("스타일코드를 입력하세요.")
        st.stop()

    total_inserted = 0

    item_plc_df = load_item_plc()

    for style_code in style_codes:
        with st.spinner(f"{style_code} 처리 중..."):
    
            actual_df = load_actual_style_data(style_code)
            if actual_df.empty:
                st.warning(f"{style_code}: 데이터 없음")
                continue
    
            result_df = build_forecast_rows(actual_df, item_plc_df)
            inserted_count = upsert_target_rows(result_df)
    
            total_inserted += inserted_count
            st.success(f"{style_code}: {inserted_count}건 저장 완료")
