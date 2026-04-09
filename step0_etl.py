import os
import pandas as pd
import streamlit as st

try:
    from supabase import create_client
except ImportError:
    create_client = None


RAW_FILE_TABLE = "RAW FILE"
ITEM_PLC_TABLE = "item_plc"
SKU_WEEKLY_FORECAST_TABLE = "sku_weekly_forecast"

RAW_FILE_SELECT = """
id,
CALDAY,
PLANT,
sku,
style_code,
item_code,
STOCK_CHANGE_QTY,
SALE_QTY,
IPGO_QTY,
BASE_STOCK_QTY
""".replace("\n", "").replace(" ", "")

ITEM_PLC_SELECT = """
id,
item_code,
item_name,
year_week,
month,
sales,
last_year_ratio_pct,
shape_type,
stage,
peak_week,
peak_month
""".replace("\n", "").replace(" ", "")


def get_supabase_client():
    if create_client is None:
        raise ImportError("supabase 패키지가 없습니다. pip install supabase 필요")

    url = ""
    key = ""

    if "supabase" in st.secrets:
        url = str(st.secrets["supabase"].get("url", "")).strip()
        key = str(
            st.secrets["supabase"].get("service_role_key")
            or st.secrets["supabase"].get("key")
            or ""
        ).strip()

    if not url:
        url = os.getenv("SUPABASE_URL", "").strip()
    if not key:
        key = (
            os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
            or os.getenv("SUPABASE_KEY", "")
        ).strip()

    if not url or not key:
        raise ValueError("Supabase URL 또는 KEY가 없습니다.")

    return create_client(url, key)


def fetch_all_rows(client, table_name: str, select_sql: str, batch_size: int = 1000):
    rows = []
    offset = 0

    while True:
        resp = (
            client.table(table_name)
            .select(select_sql)
            .range(offset, offset + batch_size - 1)
            .execute()
        )
        data = resp.data or []

        if not data:
            break

        rows.extend(data)

        if len(data) < batch_size:
            break

        offset += batch_size

    return rows


def delete_all_rows(client, table_name: str, key_col: str):
    sentinel = "__never_match__"
    client.table(table_name).delete().neq(key_col, sentinel).execute()


def insert_in_chunks(client, table_name: str, rows: list, batch_size: int = 500):
    if not rows:
        return

    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        client.table(table_name).insert(chunk).execute()


def calday_to_year_week(calday_value):
    """
    예: 20260319 -> 2026-12
    """
    if pd.isna(calday_value):
        return None

    s = str(calday_value).strip().replace(".0", "")
    dt = pd.to_datetime(s, format="%Y%m%d", errors="coerce")

    if pd.isna(dt):
        return None

    iso = dt.isocalendar()
    return f"{int(iso.year)}-{int(iso.week):02d}"


def normalize_year_week(yw):
    """
    RAW / item_plc 간 주차 키를 동일하게 맞춤 (예: 2026-12, 2026-12.0 -> 2026-12).
    """
    if pd.isna(yw) or yw is None:
        return None
    s = str(yw).strip().replace(".0", "")
    if not s:
        return None
    if "-" in s:
        parts = s.split("-", 1)
        try:
            y = int(parts[0])
            w = int(parts[1])
            return f"{y}-{w:02d}"
        except (ValueError, IndexError):
            return s
    return s


def to_int(value, default=0):
    x = pd.to_numeric(value, errors="coerce")
    if pd.isna(x):
        return default
    return int(round(float(x)))


def load_raw_file_df(client) -> pd.DataFrame:
    rows = fetch_all_rows(client, RAW_FILE_TABLE, RAW_FILE_SELECT, batch_size=1000)
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df["CALDAY"] = df["CALDAY"].apply(lambda x: str(x).replace(".0", "") if pd.notna(x) else x)
    df["year_week"] = df["CALDAY"].apply(calday_to_year_week)

    for col in ["PLANT", "sku", "style_code", "item_code"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    for col in ["SALE_QTY", "IPGO_QTY", "STOCK_CHANGE_QTY", "BASE_STOCK_QTY"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df = df.dropna(subset=["year_week"])
    df["year_week"] = df["year_week"].apply(normalize_year_week)
    df = df.dropna(subset=["year_week"])
    df = df[df["sku"] != ""]

    return df


def load_item_plc_df(client) -> pd.DataFrame:
    rows = fetch_all_rows(client, ITEM_PLC_TABLE, ITEM_PLC_SELECT, batch_size=1000)
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    for col in ["item_code", "item_name", "year_week", "stage", "shape_type"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    if "peak_week" in df.columns:
        df["peak_week"] = pd.to_numeric(df["peak_week"], errors="coerce")

    if "year_week" in df.columns:
        df["year_week"] = df["year_week"].apply(normalize_year_week)

    return df


def build_forecast_rows(raw_df: pd.DataFrame, plc_df: pd.DataFrame) -> list:
    if raw_df.empty:
        return []

    if plc_df.empty:
        raise ValueError("item_plc 테이블이 비어 있습니다.")

    # 1) RAW FILE 주차 단위 집계
    grouped = (
        raw_df.groupby(
            ["year_week", "PLANT", "style_code", "sku", "item_code"],
            as_index=False
        )
        .agg({
            "SALE_QTY": "sum",
            "IPGO_QTY": "sum",
            "STOCK_CHANGE_QTY": "sum",
            "BASE_STOCK_QTY": "sum",
        })
    )

    # 2) item_code별 PLC — (item_code, year_week)당 한 행만 두고 stage 등 조인
    plc_specific = plc_df[plc_df["item_code"] != "평균"].copy()
    plc_specific = plc_specific.dropna(subset=["year_week"])
    if "id" in plc_specific.columns:
        plc_specific = plc_specific.sort_values("id")
    plc_specific = plc_specific.drop_duplicates(subset=["item_code", "year_week"], keep="last")

    # 3) 평균 PLC
    plc_avg = plc_df[plc_df["item_code"] == "평균"].copy()
    plc_avg = plc_avg.rename(columns={
        "stage": "avg_stage",
        "peak_week": "avg_peak_week",
        "item_name": "avg_item_name",
        "shape_type": "avg_shape_type",
    })

    # 4) 먼저 item_code + year_week로 매칭
    merged = grouped.merge(
        plc_specific[["item_code", "year_week", "stage", "peak_week", "item_name", "shape_type"]],
        on=["item_code", "year_week"],
        how="left"
    )

    # 5) 못 붙은 행은 평균값 붙이기
    merged = merged.merge(
        plc_avg[["year_week", "avg_stage", "avg_peak_week", "avg_item_name", "avg_shape_type"]],
        on="year_week",
        how="left"
    )

    # 6) specific 우선, 없으면 average 사용
    merged["final_stage"] = merged["stage"]
    merged.loc[merged["final_stage"].isna() | (merged["final_stage"].astype(str).str.strip() == ""), "final_stage"] = merged["avg_stage"]

    merged["final_shape_type"] = merged["shape_type"]
    merged.loc[
        merged["final_shape_type"].isna() | (merged["final_shape_type"].astype(str).str.strip() == ""),
        "final_shape_type",
    ] = merged["avg_shape_type"]

    merged["final_peak_week"] = merged["peak_week"]
    merged.loc[merged["final_peak_week"].isna(), "final_peak_week"] = merged["avg_peak_week"]

    merged["final_item_name"] = merged["item_name"]
    empty_name_mask = merged["final_item_name"].isna() | (merged["final_item_name"].astype(str).str.strip() == "")
    merged.loc[empty_name_mask, "final_item_name"] = merged["avg_item_name"]

    rows = []

    for _, r in merged.iterrows():
        year_week = str(r["year_week"]).strip()
        peak_week = r["final_peak_week"]

        is_peak_week = False
        try:
            if pd.notna(peak_week):
                week_no = int(year_week.split("-")[1])
                is_peak_week = week_no == int(peak_week)
        except Exception:
            is_peak_week = False

        sku = str(r["sku"]).strip()
        plant = str(r["PLANT"]).strip()
        style_code = str(r["style_code"]).strip()

        final_item_name = r.get("final_item_name")
        if pd.isna(final_item_name) or str(final_item_name).strip() == "":
            final_item_name = sku

        row = {
            "year_week": year_week,
            "sale_qty": to_int(r["SALE_QTY"], 0),
            "stage": None if pd.isna(r["final_stage"]) or str(r["final_stage"]).strip() == "" else str(r["final_stage"]).strip(),
            "shape_type": None if pd.isna(r["final_shape_type"]) or str(r["final_shape_type"]).strip() == "" else str(r["final_shape_type"]).strip(),
            "style_code": style_code,
            "sku": sku,
            "is_peak_week": is_peak_week,
            "plant": plant,
            "avg_discount_rate": None,
            "sku_name": str(final_item_name).strip(),
            "store_name": plant,
            "begin_stock": to_int(r["BASE_STOCK_QTY"], 0),
            "is_forecast": False,
            "loss": 0,
            "inbound_qty": to_int(r["IPGO_QTY"], 0),
            "outbound_qty": to_int(r["STOCK_CHANGE_QTY"], 0),
        }
        rows.append(row)

    return rows


def run_job():
    client = get_supabase_client()

    st.write("1. RAW FILE 불러오는 중...")
    raw_df = load_raw_file_df(client)
    st.write(f"RAW FILE rows: {len(raw_df):,}")

    st.write("2. item_plc 불러오는 중...")
    plc_df = load_item_plc_df(client)
    st.write(f"item_plc rows: {len(plc_df):,}")

    st.write("3. sku_weekly_forecast row 생성 중...")
    rows = build_forecast_rows(raw_df, plc_df)
    st.write(f"생성 rows: {len(rows):,}")

    st.write("4. 기존 sku_weekly_forecast 삭제 중...")
    delete_all_rows(client, SKU_WEEKLY_FORECAST_TABLE, key_col="sku")

    st.write("5. 새 데이터 insert 중...")
    insert_in_chunks(client, SKU_WEEKLY_FORECAST_TABLE, rows, batch_size=500)

    st.success(f"완료: {len(rows):,}건 적재")


st.set_page_config(page_title="sku_weekly_forecast 적재", layout="wide")
st.title("sku_weekly_forecast 단순 적재")
st.write("RAW FILE + item_plc -> sku_weekly_forecast")

if st.button("실행"):
    try:
        run_job()
    except Exception as e:
        st.error(f"실패: {e}")
