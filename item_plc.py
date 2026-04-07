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


def _week_no_from_year_week(year_week: str) -> Optional[int]:
    s = str(year_week).strip()
    if not re.match(r"^\d{4}-\d{1,2}$", s):
        return None
    try:
        return int(s.split("-")[1])
    except Exception:
        return None


def smooth_series(values: np.ndarray, window: int = 2) -> np.ndarray:
    if len(values) < window:
        return values.copy()
    return (
        pd.Series(values)
        .rolling(window=window, center=True, min_periods=1)
        .mean()
        .values
    )


def find_significant_peaks(
    values: np.ndarray,
    min_peak_ratio: float = 0.35,
    min_prominence_ratio: float = 0.10,
    min_distance: int = 1,
) -> List[int]:
    if len(values) < 3:
        return []

    max_val = float(np.max(values))
    if max_val <= 0:
        return []

    candidate: List[int] = []
    for i in range(1, len(values) - 1):
        if values[i] > values[i - 1] and values[i] >= values[i + 1]:
            base_level = max(values[i - 1], values[i + 1])
            peak_ratio = float(values[i]) / max_val
            prom_ratio = float(values[i] - base_level) / max_val
            if peak_ratio >= min_peak_ratio and prom_ratio >= min_prominence_ratio:
                candidate.append(i)

    if not candidate:
        return []

    filtered: List[int] = []
    for idx in candidate:
        if not filtered:
            filtered.append(idx)
            continue
        prev = filtered[-1]
        if idx - prev <= min_distance:
            if values[idx] > values[prev]:
                filtered[-1] = idx
        else:
            filtered.append(idx)

    return filtered


def is_double_peak(values: np.ndarray) -> bool:
    peaks = find_significant_peaks(values, min_peak_ratio=0.25, min_prominence_ratio=0.05, min_distance=2)
    if len(peaks) < 2:
        return False

    mx = float(np.max(values))
    if mx <= 0:
        return False

    strong = [p for p in peaks if float(values[p]) >= mx * 0.60]
    if len(strong) < 2:
        return False

    strong = sorted(strong)
    for i in range(len(strong) - 1):
        p1, p2 = strong[i], strong[i + 1]
        if p2 - p1 < 6:
            continue
        valley = float(np.min(values[p1 : p2 + 1]))
        lower_peak = min(float(values[p1]), float(values[p2]))
        if lower_peak > 0 and valley / lower_peak <= 0.85:
            return True

    return False


def is_single_peak(values: np.ndarray) -> bool:
    peaks = find_significant_peaks(values, min_peak_ratio=0.30, min_prominence_ratio=0.08, min_distance=2)
    if len(peaks) == 1:
        return True

    mx = float(np.max(values)) if len(values) else 0.0
    if mx <= 0:
        return False

    strong = [p for p in peaks if float(values[p]) >= mx * 0.60]
    return len(strong) == 1


def is_all_season(values: np.ndarray) -> bool:
    if len(values) < 4:
        return False
    avg = float(np.mean(values))
    mx = float(np.max(values))
    if avg <= 0:
        return False
    if mx / avg > 2.0:
        return False
    low = values < avg * 0.5
    if int(np.sum(low)) > int(len(values) * 0.3):
        return False
    near_avg = (values >= avg * 0.7) & (values <= avg * 1.3)
    if int(np.sum(near_avg)) < int(len(values) * 0.7):
        return False
    return True


def classify_shape_type_from_monthly(monthly_sales: np.ndarray) -> str:
    if monthly_sales is None or len(monthly_sales) < 3:
        return "단봉형"

    y = np.asarray(monthly_sales, dtype=float)
    y = np.nan_to_num(y, nan=0.0)
    y_smooth = smooth_series(y, window=2)

    # 우선순위: 쌍봉형 -> 단봉형 -> 올시즌형
    if is_double_peak(y_smooth):
        return "쌍봉형"
    if is_single_peak(y_smooth):
        return "단봉형"
    if is_all_season(y_smooth):
        return "올시즌형"
    return "단봉형"


def classify_weekly_stage_by_shape(weekly_sales: np.ndarray, shape_type: str) -> List[str]:
    y = np.asarray(weekly_sales, dtype=float)
    y = np.nan_to_num(y, nan=0.0)
    n = len(y)
    if n == 0:
        return []

    # 기본값
    stage = ["성숙"] * n
    smooth = (
        pd.Series(y)
        .rolling(window=3, center=True, min_periods=1)
        .mean()
        .values
    )

    def safe_argmax(arr: np.ndarray) -> int:
        if len(arr) == 0:
            return 0
        return int(np.argmax(arr))

    if shape_type == "단봉형":
        peak_idx = int(np.argmax(y))
        intro_end = min(3, max(1, peak_idx // 3))
        growth_start = intro_end + 1
        growth_end = max(growth_start, peak_idx - 1)
        maturity_start = min(n - 1, peak_idx + 1)
        maturity_end = min(n - 1, maturity_start + 2)
        decline_start = min(n - 1, maturity_end + 1)

        for i in range(0, intro_end + 1):
            stage[i] = "도입"
        for i in range(growth_start, growth_end + 1):
            if 0 <= i < n:
                stage[i] = "성장"
        if 0 <= peak_idx < n:
            stage[peak_idx] = "피크"
        for i in range(maturity_start, maturity_end + 1):
            if 0 <= i < n:
                stage[i] = "성숙"
        for i in range(decline_start, n):
            stage[i] = "쇠퇴"
        return stage

    if shape_type == "쌍봉형":
        peaks = find_significant_peaks(smooth, min_peak_ratio=0.25, min_prominence_ratio=0.05, min_distance=2)
        if len(peaks) >= 2:
            peaks = sorted(peaks, key=lambda i: float(smooth[i]), reverse=True)[:2]
            peaks = sorted(peaks)
            peak1, peak2 = peaks[0], peaks[1]
        else:
            peak1 = safe_argmax(smooth[: max(1, n // 2)])
            peak2 = safe_argmax(smooth[max(peak1 + 1, 1) :]) + max(peak1 + 1, 1)
            peak2 = min(peak2, n - 1)

        if peak2 > peak1 + 1:
            valley_idx = peak1 + int(np.argmin(smooth[peak1 : peak2 + 1]))
        else:
            valley_idx = min(n - 1, peak1 + 1)

        intro_end = min(3, max(1, peak1 // 3))
        growth_start = intro_end + 1
        growth_end = max(growth_start, peak1 - 1)

        maturity1_start = min(n - 1, peak1 + 1)
        maturity1_end = min(n - 1, max(maturity1_start, valley_idx - 2))

        offseason_start = min(n - 1, max(maturity1_end + 1, valley_idx - 1))
        offseason_end = min(n - 1, valley_idx + 1)

        maturity2_start = min(n - 1, offseason_end + 1)
        maturity2_end = min(n - 1, max(maturity2_start, peak2 - 1))

        maturity3_start = min(n - 1, peak2 + 1)
        maturity3_end = min(n - 1, maturity3_start + 1)
        decline_start = min(n - 1, maturity3_end + 1)

        for i in range(0, intro_end + 1):
            stage[i] = "도입"
        for i in range(growth_start, growth_end + 1):
            if 0 <= i < n:
                stage[i] = "성장"
        if 0 <= peak1 < n:
            stage[peak1] = "피크"
        for i in range(maturity1_start, maturity1_end + 1):
            if 0 <= i < n:
                stage[i] = "성숙"
        for i in range(offseason_start, offseason_end + 1):
            if 0 <= i < n:
                stage[i] = "비시즌"
        for i in range(maturity2_start, maturity2_end + 1):
            if 0 <= i < n:
                stage[i] = "성숙"
        if 0 <= peak2 < n:
            stage[peak2] = "피크2"
        for i in range(maturity3_start, maturity3_end + 1):
            if 0 <= i < n:
                stage[i] = "성숙"
        for i in range(decline_start, n):
            stage[i] = "쇠퇴"
        return stage

    # 올시즌형: 도입 > 성장 > 성숙 > 쇠퇴 (단순)
    intro_end = min(2, n - 1)
    growth_end = min(max(intro_end + 2, n // 4), n - 1)
    decline_start = max(growth_end + 1, n - max(3, n // 5))

    for i in range(0, intro_end + 1):
        stage[i] = "도입"
    for i in range(intro_end + 1, growth_end + 1):
        if 0 <= i < n:
            stage[i] = "성장"
    for i in range(growth_end + 1, decline_start):
        if 0 <= i < n:
            stage[i] = "성숙"
    for i in range(decline_start, n):
        stage[i] = "쇠퇴"
    return stage


def normalize_stage_for_db(stage: str) -> str:
    s = str(stage or "").strip()
    if s in ("피크", "피크2"):
        return "성숙"
    if s in ("도입", "성장", "성숙", "쇠퇴", "비시즌"):
        return s
    return "성숙"


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


def build_item_weekly_df_from_plc_row(row: pd.Series, week_cols_sorted: List[str]) -> pd.DataFrame:
    series_records: List[Dict[str, Any]] = []
    for yw in week_cols_sorted:
        monday = parse_yearweek_to_monday(yw)
        if pd.isna(monday):
            continue
        sales = clean_number(row.get(yw))
        sales_v = 0.0 if pd.isna(sales) else float(sales)
        series_records.append(
            {
                "year_week": yw,
                "week_start": pd.Timestamp(monday),
                "month_ts": pd.Timestamp(monday).to_period("M").to_timestamp(),
                "sales": sales_v,
            }
        )

    if not series_records:
        return pd.DataFrame(columns=["year_week", "week_start", "month_ts", "sales"])

    dfw = pd.DataFrame(series_records).dropna(subset=["week_start"]).copy()
    if dfw.empty:
        return pd.DataFrame(columns=["year_week", "week_start", "month_ts", "sales"])

    return dfw.sort_values("week_start").reset_index(drop=True)


def compute_item_metrics_from_weekly_df(dfw: pd.DataFrame) -> Dict[str, Any]:
    if dfw is None or dfw.empty:
        return {
            "shape_type": None,
            "peak_week": None,
            "peak_month": None,
            "weekly_with_stage": dfw,
            "monthly": pd.DataFrame(columns=["month_ts", "sales"]),
        }

    dfw = dfw.copy()
    total_sales = float(pd.to_numeric(dfw["sales"], errors="coerce").fillna(0.0).sum())
    if total_sales > 0:
        dfw["last_year_ratio_pct"] = (dfw["sales"] / total_sales) * 100.0
    else:
        dfw["last_year_ratio_pct"] = 0.0

    monthly = (
        dfw.groupby("month_ts", as_index=False)["sales"]
        .sum()
        .sort_values("month_ts")
        .reset_index(drop=True)
    )
    shape_type = classify_shape_type_from_monthly(monthly["sales"].values.astype(float) if not monthly.empty else np.array([]))

    stages_raw = classify_weekly_stage_by_shape(dfw["sales"].values.astype(float), shape_type)
    if len(stages_raw) != len(dfw):
        stages_raw = ["성숙"] * len(dfw)
    dfw["stage_raw"] = stages_raw
    dfw["stage"] = [normalize_stage_for_db(s) for s in stages_raw]

    growth_maturity = dfw[dfw["stage"].isin(["성장", "성숙"])].copy()
    if growth_maturity.empty:
        peak_week: Optional[int] = None
        peak_month: Optional[int] = None
    else:
        idx = int(pd.to_numeric(growth_maturity["sales"], errors="coerce").fillna(0.0).values.argmax())
        peak_row = growth_maturity.iloc[idx]
        try:
            peak_week = int(pd.Timestamp(peak_row["week_start"]).isocalendar().week)
        except Exception:
            peak_week = None

        gm_monthly = (
            growth_maturity.groupby("month_ts", as_index=False)["sales"]
            .sum()
            .sort_values("month_ts")
            .reset_index(drop=True)
        )
        if gm_monthly.empty:
            peak_month = None
        else:
            midx = int(pd.to_numeric(gm_monthly["sales"], errors="coerce").fillna(0.0).values.argmax())
            peak_month = int(pd.Timestamp(gm_monthly.iloc[midx]["month_ts"]).month)

    return {
        "shape_type": shape_type,
        "peak_week": peak_week,
        "peak_month": peak_month,
        "weekly_with_stage": dfw,
        "monthly": monthly,
    }


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

    week_cols_sorted = sorted(
        [str(c).strip() for c in week_cols],
        key=lambda s: (
            int(s.split("-")[0]),
            int(s.split("-")[1]),
        ),
    )

    out_rows: List[Dict[str, Any]] = []

    for _, r in plc_df.iterrows():
        item_code = str(r.get("아이템코드", "")).strip()
        item_name = str(r.get("아이템명", "")).strip() or None
        # '평균' 행은 시트에서 아이템코드가 비어있는 경우가 많아 예외로 적재합니다.
        if (not item_code) and (item_name == "평균"):
            item_code = "평균"
        if not item_code:
            continue

        dfw = build_item_weekly_df_from_plc_row(r, week_cols_sorted)
        if dfw.empty:
            continue

        m = compute_item_metrics_from_weekly_df(dfw)
        shape_type = m["shape_type"]
        peak_week = m["peak_week"]
        peak_month = m["peak_month"]
        dfw = m["weekly_with_stage"]

        # 6) 행 생성 (아이템별 값은 모든 주차 행에 동일 저장)
        for _, rw in dfw.iterrows():
            month_date = pd.Timestamp(rw["month_ts"]).date()
            out_rows.append(
                {
                    "item_code": item_code,
                    "item_name": item_name,
                    "year_week": str(rw["year_week"]),
                    "month": str(month_date),
                    "sales": float(rw["sales"]),
                    "last_year_ratio_pct": float(rw["last_year_ratio_pct"]),
                    "shape_type": shape_type,
                    "stage": str(rw["stage"]),
                    "peak_week": peak_week,
                    "peak_month": peak_month,
                }
            )

    return out_rows


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
    st.set_page_config(page_title="item_plc 적재", layout="wide")

    plc_df = load_plc_db_df()
    if plc_df.empty:
        st.error("plc db 워크시트 데이터가 비어있습니다.")
        return

    required = ["아이템명", "아이템코드"]
    missing = [c for c in required if c not in plc_df.columns]
    if missing:
        st.error(f"plc db 필수 컬럼이 없습니다: {missing}")
        return

    week_cols = [c for c in plc_df.columns if re.match(r"^\d{4}-\d{1,2}$", str(c).strip())]
    if not week_cols:
        st.error("plc db에 2025-01 형식의 주차 컬럼이 없습니다.")
        return

    week_cols_sorted = sorted(
        [str(c).strip() for c in week_cols],
        key=lambda s: (int(s.split("-")[0]), int(s.split("-")[1])),
    )

    col_left, col_right = st.columns([1, 1])
    with col_left:
        do_sync = st.button("SUPABASE item_plc 채우기", type="primary")
    with col_right:
        st.caption("아래 그래프는 plc db(구글시트) 기준으로 표시됩니다.")

    if do_sync:
        with st.spinner("plc db → Supabase item_plc 적재 중…"):
            try:
                r = sync_item_plc_from_sheet_to_supabase()
                st.success(f"완료: item_plc {r['inserted']:,}행 적재 (아이템 {r['items']:,}개 기준)")
            except Exception as e:
                st.error(f"실패: {e}")

    # ---- 아이템별 그래프(전부) ----
    st.divider()
    st.subheader("아이템별 주차/월별 매출 그래프")

    plc_df = plc_df.copy()
    plc_df["아이템코드"] = plc_df["아이템코드"].astype(str).str.strip()
    plc_df["아이템명"] = plc_df["아이템명"].astype(str).str.strip()

    # 너무 많은 경우에도 UI가 버티도록, expander로 접어두고 렌더링합니다.
    for i, r in plc_df.iterrows():
        item_code = str(r.get("아이템코드", "")).strip()
        item_name = str(r.get("아이템명", "")).strip()
        if not item_code or item_code.lower() == "nan":
            continue

        dfw = build_item_weekly_df_from_plc_row(r, week_cols_sorted)
        if dfw.empty:
            continue

        m = compute_item_metrics_from_weekly_df(dfw)
        shape_type = m["shape_type"]
        peak_week = m["peak_week"]
        peak_month = m["peak_month"]
        dfw2 = m["weekly_with_stage"]
        monthly = m["monthly"]

        title = f"{item_name} ({item_code})"
        with st.expander(title, expanded=(i < 3)):
            c1, c2, c3 = st.columns([1, 1, 1])
            c1.metric("shape_type", shape_type or "-")
            c2.metric("peak_week(성장/성숙)", "-" if peak_week is None else str(peak_week))
            c3.metric("peak_month(성장/성숙)", "-" if peak_month is None else str(peak_month))

            w = dfw2[["week_start", "sales", "stage"]].copy()
            w = w.rename(columns={"week_start": "주차(월요일)", "sales": "매출"})
            st.line_chart(w.set_index("주차(월요일)")["매출"])

            if monthly is not None and not monthly.empty:
                mdf = monthly.rename(columns={"month_ts": "월", "sales": "매출"}).copy()
                st.bar_chart(mdf.set_index("월")["매출"])


if __name__ == "__main__":
    main()
