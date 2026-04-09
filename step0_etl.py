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
SALE_QTY,
IPGO_QTY,
BASE_STOCK_QTY
""".replace("\n", "").replace(" ", "")

ITEM_PLC_SELECT = """
id,
item_code,
item_name,
year_week
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

    for col in ["SALE_QTY", "IPGO_QTY", "BASE_STOCK_QTY"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["year_week"])
    df["year_week"] = df["year_week"].apply(normalize_year_week)
    df = df.dropna(subset=["year_week"])
    df = df[df["sku"] != ""]
    df = df[df["PLANT"] != ""]

    return df


def load_item_plc_df(client) -> pd.DataFrame:
    rows = fetch_all_rows(client, ITEM_PLC_TABLE, ITEM_PLC_SELECT, batch_size=1000)
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    for col in ["item_code", "item_name", "year_week"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    if "year_week" in df.columns:
        df["year_week"] = df["year_week"].apply(normalize_year_week)

    return df


def year_week_to_week_no(year_week: str):
    """예: 2026-12 -> 12"""
    if not year_week or "-" not in year_week:
        return None
    try:
        return int(str(year_week).split("-", 1)[1])
    except (ValueError, IndexError):
        return None


def build_forecast_rows(raw_df: pd.DataFrame, plc_df: pd.DataFrame) -> list:
    """
    키: year_week(YEAR_WEEK), PLANT, sku(SKU).
    SALE_QTY / BASE_STOCK_QTY / IPGO_QTY는 RAW FILE 해당 컬럼 값을 집계 없이 그대로 사용.
    동일 키가 여러 줄이면 id 기준 마지막 행을 사용.
    """
    if raw_df.empty:
        return []

    work = raw_df.copy()
    if "id" in work.columns:
        work = work.sort_values("id")
    work = work.drop_duplicates(
        subset=["year_week", "PLANT", "sku"],
        keep="last",
    )

    if not plc_df.empty:
        plc_specific = plc_df[plc_df["item_code"] != "평균"].copy()
        plc_specific = plc_specific.dropna(subset=["year_week"])
        if "id" in plc_specific.columns:
            plc_specific = plc_specific.sort_values("id")
        plc_specific = plc_specific.drop_duplicates(
            subset=["item_code", "year_week"], keep="last"
        )
        merged = work.merge(
            plc_specific[["item_code", "year_week", "item_name"]],
            on=["item_code", "year_week"],
            how="left",
        )
        plc_avg = plc_df[plc_df["item_code"] == "평균"].copy()
        plc_avg = plc_avg.rename(columns={"item_name": "avg_item_name"})
        merged = merged.merge(
            plc_avg[["year_week", "avg_item_name"]],
            on="year_week",
            how="left",
        )
        merged["final_item_name"] = merged["item_name"]
        empty_name_mask = merged["final_item_name"].isna() | (
            merged["final_item_name"].astype(str).str.strip() == ""
        )
        merged.loc[empty_name_mask, "final_item_name"] = merged["avg_item_name"]
    else:
        merged = work.copy()
        merged["final_item_name"] = None

    rows = []
    for _, r in merged.iterrows():
        year_week = str(r["year_week"]).strip()
        week_no = year_week_to_week_no(year_week)

        sku = str(r["sku"]).strip()
        plant = str(r["PLANT"]).strip()
        style_code = str(r["style_code"]).strip()

        final_item_name = r.get("final_item_name")
        if pd.isna(final_item_name) or str(final_item_name).strip() == "":
            final_item_name = sku

        rows.append({
            "year_week": year_week,
            "SALE_QTY": to_float_or_none(r["SALE_QTY"]),
            "style_code": style_code or None,
            "sku": sku or None,
            "plant": plant or None,
            "sku_name": str(final_item_name).strip() or None,
            "store_name": plant or None,
            "BASE_STOCK_QTY": to_int_or_none(r["BASE_STOCK_QTY"]),
            "IPGO_QTY": to_int_or_none(r["IPGO_QTY"]),
            "week_no": week_no,
        })

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
st.write("RAW FILE (YEAR_WEEK, PLANT, SKU) 수량 그대로 + item_plc -> sku_weekly_forecast")

if st.button("실행"):
    try:
        run_job()
    except Exception as e:
        st.error(f"실패: {e}")
