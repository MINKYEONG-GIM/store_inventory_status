import os
import pandas as pd
import streamlit as st

try:
    from supabase import create_client
except ImportError:
    create_client = None


RAW_FILE_TABLE = "RAW FILE"
SKU_WEEKLY_FORECAST_TABLE = "sku_weekly_forecast"

RAW_FILE_SELECT = """
id,
CALDAY,
PLANT,
sku,
style_code,
STOCK_CHANGE_QTY,
SALE_QTY,
IPGO_QTY,
item_code
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


def year_week_to_week_no(year_week: str):
    """
    예: 2026-12 -> 12
    """
    if not year_week or "-" not in str(year_week):
        return None

    try:
        return int(str(year_week).split("-", 1)[1])
    except (ValueError, IndexError):
        return None


def to_float_or_none(value):
    x = pd.to_numeric(value, errors="coerce")
    if pd.isna(x):
        return None
    return float(x)


def to_int_or_none(value):
    x = pd.to_numeric(value, errors="coerce")
    if pd.isna(x):
        return None
    return int(round(float(x)))


def extract_item_code(sku):
    if sku is None or pd.isna(sku):
        return None
    s = str(sku).strip()
    if len(s) < 4:
        return None
    return s[2:4]


def load_raw_file_df(client) -> pd.DataFrame:
    rows = fetch_all_rows(client, RAW_FILE_TABLE, RAW_FILE_SELECT, batch_size=1000)
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df["CALDAY"] = df["CALDAY"].apply(
        lambda x: str(x).replace(".0", "").strip() if pd.notna(x) else None
    )
    df["CALDAY_DT"] = pd.to_datetime(df["CALDAY"], format="%Y%m%d", errors="coerce")
    df["year_week"] = df["CALDAY"].apply(calday_to_year_week)
    df["week_no"] = df["year_week"].apply(year_week_to_week_no)

    for col in ["PLANT", "sku", "style_code", "item_code"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x).strip() if pd.notna(x) else None)

    for col in ["STOCK_CHANGE_QTY", "SALE_QTY", "IPGO_QTY"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df = df.dropna(subset=["CALDAY_DT", "year_week", "PLANT", "sku"])

    weekly_df = (
        df.groupby(
            ["PLANT", "sku", "style_code", "item_code", "year_week", "week_no"],
            dropna=False,
            as_index=False
        )
        .agg({
            "IPGO_QTY": "sum",
            "SALE_QTY": "sum"
        })
    )

    weekly_df = weekly_df.sort_values(
        ["PLANT", "sku", "year_week"]
    ).reset_index(drop=True)

    weekly_df["BASE_STOCK_QTY"] = (
        weekly_df.groupby(["PLANT", "sku"])["IPGO_QTY"].cumsum()
        - weekly_df.groupby(["PLANT", "sku"])["SALE_QTY"].cumsum()
    )

    return weekly_df

def build_forecast_rows(raw_df: pd.DataFrame) -> list:
    """
    RAW FILE 각 행을 거의 그대로 sku_weekly_forecast로 옮김.
    추가: year_week, week_no, sku에서 추출한 item_code.
    """
    if raw_df.empty:
        return []

    rows = []

    for _, r in raw_df.iterrows():
        plant = r.get("PLANT")
        sku = r.get("sku")
        item_code = r.get("item_code") or extract_item_code(sku)

        rows.append({
            "year_week": r.get("year_week"),
            "SALE_QTY": to_float_or_none(r.get("SALE_QTY")),
            "style_code": str(r.get("style_code")).strip() if pd.notna(r.get("style_code")) else None,
            "sku": str(sku).strip() if pd.notna(sku) else None,
            "plant": str(plant).strip() if pd.notna(plant) else None,
            "item_code": item_code,
            "BASE_STOCK_QTY": to_int_or_none(r.get("BASE_STOCK_QTY")),
            "IPGO_QTY": to_int_or_none(r.get("IPGO_QTY")),
            "week_no": to_int_or_none(r.get("week_no")),
        })

    return rows


def run_job():
    client = get_supabase_client()

    st.write("1. RAW FILE 불러오는 중...")
    raw_df = load_raw_file_df(client)
    st.write(f"RAW FILE rows: {len(raw_df):,}")

    st.write("2. sku_weekly_forecast row 생성 중...")
    rows = build_forecast_rows(raw_df)
    st.write(f"생성 rows: {len(rows):,}")

    st.write("3. 기존 데이터 유지 — 새 행만 추가(누적) 중...")
    insert_in_chunks(client, SKU_WEEKLY_FORECAST_TABLE, rows, batch_size=500)

    st.success(f"완료: {len(rows):,}건 추가 적재 (기존 행은 유지)")


st.set_page_config(page_title="sku_weekly_forecast 적재", layout="wide")
st.title("sku_weekly_forecast 단순 적재")
st.write("RAW FILE 그대로 사용 + CALDAY 기반 year_week, week_no만 추가")

if st.button("실행"):
    try:
        run_job()
    except Exception as e:
        st.error(f"실패: {e}")
