
import os
import json
import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import numpy as np
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

try:
    from supabase import create_client as _create_supabase_client
except ImportError:
    _create_supabase_client = None


def make_unique_headers(headers: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out: List[str] = []
    for h in headers:
        col = str(h).strip() or "unnamed"
        if col not in seen:
            seen[col] = 1
            out.append(col)
        else:
            seen[col] += 1
            out.append(f"{col}_{seen[col]}")
    return out


def clean_number(value: Any) -> float:
    if pd.isna(value):
        return np.nan
    s = str(value).strip()
    if not s:
        return np.nan
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return np.nan


def parse_yearweek_to_monday(yearweek: str) -> pd.Timestamp:
    s = str(yearweek).strip()
    if not re.match(r"^\d{4}-\d{1,2}$", s):
        return pd.NaT
    year_str, week_str = s.split("-")
    try:
        return pd.to_datetime(
            f"{int(year_str)}-W{int(week_str):02d}-1",
            format="%G-W%V-%u",
            errors="coerce",
        )
    except Exception:
        return pd.NaT


def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    if "gcp_service_account" in st.secrets:
        creds_dict = dict(st.secrets["gcp_service_account"])
        credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return gspread.authorize(credentials)

    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if service_account_json:
        creds_dict = json.loads(service_account_json)
        credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return gspread.authorize(credentials)

    raise ValueError("구글 서비스 계정 정보가 없습니다.")


def get_sheets_config() -> dict:
    if "sheets" not in st.secrets:
        raise ValueError("st.secrets['sheets'] 설정이 없습니다. secrets.toml에 [sheets] 섹션을 추가하세요.")
    return dict(st.secrets["sheets"])


def load_sheet_as_df(worksheet_name: str) -> pd.DataFrame:
    client = get_gspread_client()
    sheets_cfg = get_sheets_config()
    sheet_id = sheets_cfg.get("sheet_id")
    if not sheet_id:
        raise ValueError("secrets.toml의 [sheets].sheet_id 가 비어있습니다.")

    sh = client.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(worksheet_name)
    except Exception as e:
        available = [w.title for w in sh.worksheets()]
        raise ValueError(f"워크시트 '{worksheet_name}'를 찾지 못했습니다. 사용 가능한 워크시트: {available}") from e

    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    headers = make_unique_headers([str(h) for h in values[0]])
    rows = values[1:] if len(values) > 1 else []
    if not rows:
        return pd.DataFrame(columns=headers)

    max_cols = len(headers)
    normalized_rows = []
    for row in rows:
        row = list(row)
        if len(row) < max_cols:
            row = row + [""] * (max_cols - len(row))
        elif len(row) > max_cols:
            row = row[:max_cols]
        normalized_rows.append(row)

    return pd.DataFrame(normalized_rows, columns=headers)


def get_supabase_client():
    if _create_supabase_client is None:
        return None

    url = ""
    key = ""
    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            sec = dict(st.secrets["supabase"])
            url = str(sec.get("url") or "").strip()
            key = str(sec.get("service_role_key") or sec.get("key") or sec.get("anon_key") or "").strip()
    except Exception:
        pass

    if not url:
        url = (os.getenv("SUPABASE_URL") or "").strip()
    if not key:
        key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_ANON_KEY") or "").strip()

    if not url or not key:
        return None

    return _create_supabase_client(url, key)


def load_plc_db_df() -> pd.DataFrame:
    sheets_cfg = get_sheets_config()
    ws_name = str(sheets_cfg.get("plc_db") or "plc db").strip()
    return load_sheet_as_df(ws_name)


def build_item_plc_rows_from_plc_sheet(plc_df: pd.DataFrame) -> List[Dict[str, Any]]:
    if plc_df is None or plc_df.empty:
        return []

    required = ["아이템명", "아이템코드"]
    missing = [c for c in required if c not in plc_df.columns]
    if missing:
        raise ValueError(f"plc db 필수 컬럼이 없습니다: {missing}")

    week_cols = [c for c in plc_df.columns if re.match(r"^\d{4}-\d{1,2}$", str(c).strip())]
    if not week_cols:
        raise ValueError("plc db에 2025-01 형식의 주차 컬럼이 없습니다.")

    rows: List[Dict[str, Any]] = []
    for _, r in plc_df.iterrows():
        item_code = str(r.get("아이템코드", "")).strip()
        item_name = str(r.get("아이템명", "")).strip() or None
        if not item_code:
            continue

        for yw in week_cols:
            week_key = str(yw).strip()
            monday = parse_yearweek_to_monday(week_key)
            if pd.isna(monday):
                continue
            month = pd.Timestamp(monday).to_period("M").to_timestamp().date()
            sales = clean_number(r.get(yw))
            rows.append(
                {
                    "item_code": item_code,
                    "item_name": item_name,
                    "year_week": week_key,
                    "month": str(month),
                    "sales": None if pd.isna(sales) else float(sales),
                    "last_year_ratio_pct": None,
                    "shape_type": None,
                    "stage": None,
                    "peak_week": None,
                    "peak_month": None,
                }
            )

    return rows


def clear_item_plc_table(client) -> None:
    # PostgREST는 필터 없이 delete가 안 되므로, 절대 매칭되지 않을 값으로 neq 필터를 둡니다.
    client.table("item_plc").delete().neq("id", -1).execute()


def bulk_insert_item_plc_rows(client, rows: List[Dict[str, Any]], batch_size: int = 500) -> int:
    if not rows:
        return 0
    tbl = client.table("item_plc")
    inserted = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        tbl.insert(chunk).execute()
        inserted += len(chunk)
    return inserted


def sync_item_plc_from_sheet_to_supabase() -> Dict[str, Any]:
    if _create_supabase_client is None:
        raise RuntimeError("supabase 패키지가 필요합니다. (예: pip install supabase)")

    sb = get_supabase_client()
    if sb is None:
        raise RuntimeError("Supabase 연결 정보가 없습니다. Streamlit secrets [supabase] url·service_role_key를 설정하세요.")

    plc_df = load_plc_db_df()
    if plc_df.empty:
        raise RuntimeError("plc db 워크시트 데이터가 비어있습니다.")

    rows = build_item_plc_rows_from_plc_sheet(plc_df)
    if not rows:
        raise RuntimeError("적재할 행이 없습니다. (아이템코드/주차 컬럼을 확인하세요)")

    clear_item_plc_table(sb)
    inserted = bulk_insert_item_plc_rows(sb, rows, batch_size=500)

    return {"inserted": inserted, "items": int(plc_df["아이템코드"].astype(str).str.strip().replace("", np.nan).dropna().nunique())}


def main() -> None:
    st.set_page_config(page_title="item_plc 적재", layout="centered")
    if st.button("SUPABASE item_plc 채우기", type="primary"):
        with st.spinner("plc db → Supabase item_plc 적재 중…"):
            try:
                r = sync_item_plc_from_sheet_to_supabase()
                st.success(f"완료: item_plc {r['inserted']:,}행 적재 (아이템 {r['items']:,}개 기준)")
            except Exception as e:
                st.error(f"실패: {e}")


if __name__ == "__main__":
    main()
