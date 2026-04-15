import os
import pandas as pd
import streamlit as st
from datetime import date

try:
    from supabase import create_client
except ImportError:
    create_client = None


SKU_WEEKLY_FORECAST_TABLE = "sku_weekly_forecast"
ITEM_PLC_TABLE = "item_plc"
SKU_WEEKLY_FORECAST_2_TABLE = "sku_weekly_forecast_2"

SKU_WEEKLY_FORECAST_SELECT = """
id,
created_at,
year_week,
SALE_QTY,
style_code,
sku,
plant,
BASE_STOCK_QTY,
IPGO_QTY,
week_no,
item_code
""".replace("\n", "").replace(" ", "")

ITEM_PLC_SELECT = """
id,
item_code,
item_name,
year_week,
week_no,
month,
sales,
last_year_ratio_pct,
shape_type,
stage,
peak_week,
peak_month,
created_at
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
    """
    Supabase(PostgREST)는 delete 시 필터가 필요해서,
    '항상 true'가 되는 조건으로 전체 삭제를 수행한다.
    """
    if key_col == "id":
        client.table(table_name).delete().neq(key_col, -1).execute()
        return

    sentinel = "__never_match__"
    client.table(table_name).delete().neq(key_col, sentinel).execute()


def insert_in_chunks(client, table_name: str, rows: list, batch_size: int = 500):
    if not rows:
        return

    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        client.table(table_name).insert(chunk).execute()


def normalize_year_week(yw):
    """
    예:
    2026-5 -> 2026-05
    2026-05 -> 2026-05
    202605 -> 2026-05
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
        except Exception:
            return None

    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) == 6:
        try:
            y = int(digits[:4])
            w = int(digits[4:])
            return f"{y}-{w:02d}"
        except Exception:
            return None

    return None


def year_week_to_parts(year_week: str):
    yw = normalize_year_week(year_week)
    if not yw:
        return None, None

    try:
        y, w = yw.split("-")
        return int(y), int(w)
    except Exception:
        return None, None


def get_current_year_week():
    today = pd.Timestamp.today()
    iso = today.isocalendar()
    return int(iso.year), int(iso.week)


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


def to_bool(value):
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    return s in ["true", "1", "y", "yes"]


def load_sku_weekly_forecast_df(client) -> pd.DataFrame:
    rows = fetch_all_rows(
        client,
        SKU_WEEKLY_FORECAST_TABLE,
        SKU_WEEKLY_FORECAST_SELECT,
        batch_size=1000
    )
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    for col in ["year_week", "style_code", "sku", "plant", "item_code"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x).strip() if pd.notna(x) else None)

    if "year_week" in df.columns:
        df["year_week"] = df["year_week"].apply(normalize_year_week)

    for col in ["SALE_QTY", "BASE_STOCK_QTY", "IPGO_QTY", "week_no"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # year_week 기준으로 year, week 분리
    parts = df["year_week"].apply(year_week_to_parts)
    df["year"] = parts.apply(lambda x: x[0])
    df["week"] = parts.apply(lambda x: x[1])

    # week_no 없으면 year_week에서 계산
    if "week_no" not in df.columns:
        df["week_no"] = df["week"]
    else:
        df["week_no"] = df["week_no"].fillna(df["week"])

    return df


def load_item_plc_df(client) -> pd.DataFrame:
    rows = fetch_all_rows(
        client,
        ITEM_PLC_TABLE,
        ITEM_PLC_SELECT,
        batch_size=1000
    )
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    for col in ["item_code", "item_name", "year_week", "shape_type", "stage"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x).strip() if pd.notna(x) else None)

    if "year_week" in df.columns:
        df["year_week"] = df["year_week"].apply(normalize_year_week)

    for col in ["last_year_ratio_pct", "peak_week", "peak_month", "sales", "week_no"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    parts = df["year_week"].apply(year_week_to_parts)
    df["year"] = parts.apply(lambda x: x[0])
    df["week"] = parts.apply(lambda x: x[1])

    # item_plc에 week_no가 비어 있으면 year_week에서 week 값으로 보정
    if "week_no" not in df.columns:
        df["week_no"] = df["week"]
    else:
        df["week_no"] = df["week_no"].fillna(df["week"])

    return df


def deduplicate_item_plc(plc_df: pd.DataFrame) -> pd.DataFrame:
    """
    item_code + week_no 기준으로 한 행만 남김.
    마지막 id 기준 keep last
    """
    if plc_df.empty:
        return plc_df

    work = plc_df.copy()
    work = work.dropna(subset=["item_code", "week_no"])

    if "id" in work.columns:
        work = work.sort_values("id")

    work = work.drop_duplicates(subset=["item_code", "week_no"], keep="last")
    return work


def attach_plc_fields_by_itemcode_weekno(base_df: pd.DataFrame, plc_df: pd.DataFrame) -> pd.DataFrame:
    """
    sku row마다 item_plc 값을 붙인다.

    우선순위
    1) 같은 item_code + 같은 week_no
    2) 없으면 item_code='평균' + 같은 week_no
    """
    if base_df.empty:
        return base_df.copy()

    result = base_df.copy()

    if plc_df.empty:
        result["last_year_ratio_pct"] = None
        result["shape_type"] = None
        result["stage"] = None
        result["peak_week"] = None
        return result

    plc_use = plc_df.copy()
    plc_use["item_code"] = plc_use["item_code"].apply(
        lambda x: str(x).strip() if pd.notna(x) else None
    )
    plc_use["week_no"] = pd.to_numeric(plc_use["week_no"], errors="coerce")

    plc_cols = ["item_code", "week_no", "last_year_ratio_pct", "shape_type", "stage", "peak_week"]
    plc_use = plc_use[[c for c in plc_cols if c in plc_use.columns]].copy()

    specific_plc = plc_use[plc_use["item_code"] != "평균"].copy()
    avg_plc = plc_use[plc_use["item_code"] == "평균"].copy()

    avg_plc = avg_plc.rename(columns={
        "last_year_ratio_pct": "avg_last_year_ratio_pct",
        "shape_type": "avg_shape_type",
        "stage": "avg_stage",
        "peak_week": "avg_peak_week",
    })

    result["item_code"] = result["item_code"].apply(
        lambda x: str(x).strip() if pd.notna(x) else None
    )
    result["week_no"] = pd.to_numeric(result["week_no"], errors="coerce")

    # 1차: item_code + week_no
    result = result.merge(
        specific_plc,
        on=["item_code", "week_no"],
        how="left"
    )

    # 2차: 평균 + week_no
    result = result.merge(
        avg_plc[[c for c in ["week_no", "avg_last_year_ratio_pct", "avg_shape_type", "avg_stage", "avg_peak_week"] if c in avg_plc.columns]],
        on="week_no",
        how="left"
    )

    # 실제 item_code 매칭이 있으면 그 값 사용, 없으면 평균값 사용
    if "avg_last_year_ratio_pct" in result.columns:
        result["last_year_ratio_pct"] = result["last_year_ratio_pct"].combine_first(result["avg_last_year_ratio_pct"])
    if "avg_shape_type" in result.columns:
        result["shape_type"] = result["shape_type"].combine_first(result["avg_shape_type"])
    if "avg_stage" in result.columns:
        result["stage"] = result["stage"].combine_first(result["avg_stage"])
    if "avg_peak_week" in result.columns:
        result["peak_week"] = result["peak_week"].combine_first(result["avg_peak_week"])

    drop_cols = ["avg_last_year_ratio_pct", "avg_shape_type", "avg_stage", "avg_peak_week"]
    result = result.drop(columns=[c for c in drop_cols if c in result.columns])

    return result


def make_full_weeks_for_base(base_df: pd.DataFrame, target_year: int) -> pd.DataFrame:
    """
    sku / plant / item_code 단위로 1~52주 전체 행 생성
    """
    if base_df.empty:
        return pd.DataFrame()

    keep_cols = [c for c in ["item_code", "style_code", "sku", "plant"] if c in base_df.columns]
    uniq = base_df[keep_cols].drop_duplicates().copy()
    uniq["key"] = 1

    weeks = pd.DataFrame({"week_no": list(range(1, 53))})
    weeks["year_week"] = weeks["week_no"].apply(lambda w: f"{target_year}-{int(w):02d}")
    weeks["key"] = 1

    full = uniq.merge(weeks, on="key", how="inner").drop(columns=["key"])
    return full


def apply_base_stock_and_loss(final_df: pd.DataFrame) -> pd.DataFrame:
    """
    규칙
    - is_forecast == False(실제):
        available_stock = (재고 시작값) + IPGO_QTY
        shortage = max(0, sale_qty - available_stock)  # 그 주에 못 판 수량(단일)
        BASE_STOCK_QTY = max(0, available_stock - sale_qty)  # 이월 재고
        loss는 누적하지 않음 (단일 shortage)
    - is_forecast == True(예측):
        available_stock = (전주 이월 재고) + IPGO_QTY(보통 0)
        shortage = max(0, sale_qty - available_stock)
        BASE_STOCK_QTY = max(0, available_stock - sale_qty)
        loss는 '누적 미래 손실'로 계산
        = 전주 loss + shortage
    """
    if final_df.empty:
        return final_df

    work = final_df.copy()
    work["week_no"] = pd.to_numeric(work["week_no"], errors="coerce")
    work["sale_qty"] = pd.to_numeric(work["sale_qty"], errors="coerce").fillna(0)
    work["BASE_STOCK_QTY"] = pd.to_numeric(work["BASE_STOCK_QTY"], errors="coerce")
    work["IPGO_QTY"] = pd.to_numeric(work["IPGO_QTY"], errors="coerce").fillna(0)
    if "loss" not in work.columns:
        work["loss"] = 0
    work["loss"] = pd.to_numeric(work["loss"], errors="coerce").fillna(0)
    work["is_forecast"] = work["is_forecast"].apply(to_bool)

    work = work.sort_values(["sku", "plant", "week_no"], na_position="last").reset_index(drop=True)

    new_base = work["BASE_STOCK_QTY"].tolist()
    new_loss = work["loss"].tolist()

    for _, g in work.groupby(["sku", "plant"], sort=False):
        g = g.sort_values("week_no", na_position="last")

        prev_base = None
        prev_loss = 0.0

        for idx, row in g.iterrows():
            is_forecast = to_bool(row.get("is_forecast"))
            current_base = pd.to_numeric(row.get("BASE_STOCK_QTY"), errors="coerce")
            sale_qty = pd.to_numeric(row.get("sale_qty"), errors="coerce")
            ipgo_qty = pd.to_numeric(row.get("IPGO_QTY"), errors="coerce")

            sale_qty = 0.0 if pd.isna(sale_qty) else float(sale_qty)
            ipgo_qty = 0.0 if pd.isna(ipgo_qty) else float(ipgo_qty)

            # 재고 시작값 결정:
            # - 첫 주차(또는 앞에서 계산된 값이 없을 때)는 current_base를 시작 재고로 사용
            # - 이후부터는 prev_base(전주 이월 재고)를 사용
            if prev_base is None:
                prev_base = 0.0 if pd.isna(current_base) else float(current_base)

            available_stock = prev_base + ipgo_qty
            shortage = max(0.0, sale_qty - available_stock)
            remain = max(0.0, available_stock - sale_qty)

            if is_forecast:
                curr_loss = prev_loss + shortage  # 누적
            else:
                curr_loss = shortage  # 단일(누적 X)

            new_base[idx] = int(round(remain))
            new_loss[idx] = int(round(curr_loss))

            prev_base = remain
            prev_loss = curr_loss

    work["BASE_STOCK_QTY"] = new_base
    work["loss"] = new_loss

    return work


def build_actual_rows(sku_df: pd.DataFrame, plc_df: pd.DataFrame, target_year: int) -> pd.DataFrame:
    """
    target_year 실제값 row 생성
    """
    if sku_df.empty:
        return pd.DataFrame()

    work = sku_df.copy()
    work = work[work["year"] == target_year].copy()

    if work.empty:
        return pd.DataFrame()

    merged = attach_plc_fields_by_itemcode_weekno(work, plc_df)

    merged["is_peak_week"] = (
        merged["peak_week"].notna() &
        (pd.to_numeric(merged["week_no"], errors="coerce") == pd.to_numeric(merged["peak_week"], errors="coerce"))
    )

    rows = []
    for _, r in merged.iterrows():
        rows.append({
            "year_week": r.get("year_week"),
            "sale_qty": to_int_or_none(r.get("SALE_QTY")),
            "stage": None if pd.isna(r.get("stage")) or str(r.get("stage")).strip() == "" else str(r.get("stage")).strip(),
            "style_code": None if pd.isna(r.get("style_code")) else str(r.get("style_code")).strip(),
            "sku": None if pd.isna(r.get("sku")) else str(r.get("sku")).strip(),
            "is_peak_week": bool(r.get("is_peak_week")),
            "plant": None if pd.isna(r.get("plant")) else str(r.get("plant")).strip(),
            "last_year_ratio_pct": to_float_or_none(r.get("last_year_ratio_pct")),
            "BASE_STOCK_QTY": to_int_or_none(r.get("BASE_STOCK_QTY")),
            "is_forecast": False,
            "loss": None,
            "IPGO_QTY": to_int_or_none(r.get("IPGO_QTY")),
            "shape_type": None if pd.isna(r.get("shape_type")) or str(r.get("shape_type")).strip() == "" else str(r.get("shape_type")).strip(),
            "week_no": to_int_or_none(r.get("week_no")),
        })

    return pd.DataFrame(rows)


def build_forecast_rows(sku_df: pd.DataFrame, plc_df: pd.DataFrame, target_year: int, curr_week: int) -> pd.DataFrame:
    """
    sku / plant 별로 1~52주 전체를 만든 뒤,
    실제값이 없는 미래 주차를 예측으로 채운다.
    """
    if sku_df.empty or plc_df.empty:
        return pd.DataFrame()

    sku_year = sku_df[sku_df["year"] == target_year].copy()
    plc_year = plc_df.copy()

    if sku_year.empty:
        return pd.DataFrame()

    # 기준 sku/plant/item_code 목록
    base_keys = sku_year[["item_code", "style_code", "sku", "plant"]].drop_duplicates().copy()

    # 1~52 전체 주차 생성
    full_weeks = make_full_weeks_for_base(base_keys, target_year)

    # 실제 존재 주차
    actual_keys = sku_year[["item_code", "style_code", "sku", "plant", "week_no"]].copy()
    actual_keys["week_no"] = pd.to_numeric(actual_keys["week_no"], errors="coerce")
    full_weeks["week_no"] = pd.to_numeric(full_weeks["week_no"], errors="coerce")

    merged_full = full_weeks.merge(
        actual_keys.assign(actual_exists=1),
        on=["item_code", "style_code", "sku", "plant", "week_no"],
        how="left"
    )

    # 실제 없는 주차만 남김
    missing_weeks = merged_full[merged_full["actual_exists"].isna()].copy()

    # 미래 주차만 예측 대상으로 제한
    missing_weeks = missing_weeks[missing_weeks["week_no"] > curr_week].copy()

    if missing_weeks.empty:
        return pd.DataFrame()

    # 가장 최근 실제 주차의 재고/입고
    latest_actual = (
        sku_year[sku_year["week_no"] <= curr_week]
        .sort_values(["item_code", "sku", "plant", "week_no"])
        .groupby(["item_code", "style_code", "sku", "plant"], as_index=False)
        .tail(1)
        .copy()
    )

    latest_actual = latest_actual[[
        "item_code", "style_code", "sku", "plant",
        "BASE_STOCK_QTY", "IPGO_QTY"
    ]].copy()

    # 예측 대상 주차에 최신 재고 붙이기
    expanded = missing_weeks.merge(
        latest_actual,
        on=["item_code", "style_code", "sku", "plant"],
        how="left"
    )

    # 미래 주차별 item_plc 비중 붙이기
    expanded = attach_plc_fields_by_itemcode_weekno(expanded, plc_year)

    # 누적 판매량이 0일 때 사용할 "다다음주" 기준 주차
    fallback_week_no = curr_week + 2
    if fallback_week_no > 52:
        fallback_week_no = 52
    expanded["fallback_week_no"] = fallback_week_no

    # 현재까지 실제 판매량
    actual_summary = (
        sku_year[sku_year["week_no"] <= curr_week]
        .groupby(["item_code", "style_code", "sku", "plant"], as_index=False)
        .agg(actual_sale_cum=("SALE_QTY", "sum"))
    )

    expanded = expanded.merge(
        actual_summary,
        on=["item_code", "style_code", "sku", "plant"],
        how="left"
    )

    # 현재까지(실제 주차 기준) ratio 합계 계산용
    ratio_base = make_full_weeks_for_base(base_keys, target_year)
    ratio_base["week_no"] = pd.to_numeric(ratio_base["week_no"], errors="coerce")
    ratio_base = attach_plc_fields_by_itemcode_weekno(ratio_base, plc_year)

    # 전체 52주 ratio 합계
    total_ratio_df = (
        ratio_base.groupby(["item_code", "style_code", "sku", "plant"], as_index=False)
        .agg(total_ratio_sum=("last_year_ratio_pct", "sum"))
    )

    # 현재 주차까지 ratio 합계
    elapsed_ratio_df = (
        ratio_base[ratio_base["week_no"] <= curr_week]
        .groupby(["item_code", "style_code", "sku", "plant"], as_index=False)
        .agg(elapsed_ratio_sum=("last_year_ratio_pct", "sum"))
    )

    # 다다음주 ratio
    fallback_ratio_df = ratio_base.rename(columns={
        "week_no": "fallback_week_no",
        "last_year_ratio_pct": "fallback_week_ratio"
    })[["item_code", "style_code", "sku", "plant", "fallback_week_no", "fallback_week_ratio"]].copy()

    expanded = expanded.merge(
        total_ratio_df,
        on=["item_code", "style_code", "sku", "plant"],
        how="left"
    )

    expanded = expanded.merge(
        elapsed_ratio_df,
        on=["item_code", "style_code", "sku", "plant"],
        how="left"
    )

    expanded = expanded.merge(
        fallback_ratio_df,
        on=["item_code", "style_code", "sku", "plant", "fallback_week_no"],
        how="left"
    )

    def calc_forecast_sale(row):
        actual_sale_cum = pd.to_numeric(row.get("actual_sale_cum"), errors="coerce")
        elapsed_ratio_sum = pd.to_numeric(row.get("elapsed_ratio_sum"), errors="coerce")
        total_ratio_sum = pd.to_numeric(row.get("total_ratio_sum"), errors="coerce")
        week_ratio = pd.to_numeric(row.get("last_year_ratio_pct"), errors="coerce")
        fallback_week_no = pd.to_numeric(row.get("fallback_week_no"), errors="coerce")
        fallback_week_ratio = pd.to_numeric(row.get("fallback_week_ratio"), errors="coerce")
        week_no = pd.to_numeric(row.get("week_no"), errors="coerce")

        if pd.isna(actual_sale_cum):
            actual_sale_cum = 0.0

        if pd.isna(week_ratio):
            return 0

        # 1) 누적 판매량이 0보다 크면 정상 로직
        if actual_sale_cum > 0:
            if pd.isna(elapsed_ratio_sum) or pd.isna(total_ratio_sum):
                return 0
            if elapsed_ratio_sum <= 0 or total_ratio_sum <= 0:
                return 0

            # 현재까지 실제 판매량을 기준으로 올해 총판매량 추정
            estimated_total_sale = float(actual_sale_cum) / (
                float(elapsed_ratio_sum) / float(total_ratio_sum)
            )

            # 해당 미래 주차 판매량 = 추정 연간 총판매량 * 해당 주차 비중
            raw = estimated_total_sale * (float(week_ratio) / float(total_ratio_sum))

            if raw < 0:
                raw = 0.0

            return int(round(raw))

        # 2) 누적 판매량이 0이면 fallback 로직
        if pd.isna(fallback_week_no) or pd.isna(week_no):
            return 0

        fallback_week_no = int(fallback_week_no)

        # 다음주는 0 유지
        if week_no < fallback_week_no:
            return 0

        # 다다음주는 1장 강제
        if week_no == fallback_week_no:
            return 1

        # 그 이후는 다다음주 비중 대비 배수 계산
        if pd.isna(fallback_week_ratio) or fallback_week_ratio <= 0:
            return 1

        raw = float(week_ratio) / float(fallback_week_ratio)
        if raw < 1:
            raw = 1

        return int(round(raw))

    expanded["sale_qty"] = expanded.apply(calc_forecast_sale, axis=1)
    expanded["is_peak_week"] = (
        expanded["peak_week"].notna() &
        (pd.to_numeric(expanded["week_no"], errors="coerce") == pd.to_numeric(expanded["peak_week"], errors="coerce"))
    )

    rows = []
    for _, r in expanded.iterrows():
        rows.append({
            "year_week": r.get("year_week"),
            "sale_qty": to_int_or_none(r.get("sale_qty")),
            "stage": None if pd.isna(r.get("stage")) or str(r.get("stage")).strip() == "" else str(r.get("stage")).strip(),
            "style_code": None if pd.isna(r.get("style_code")) else str(r.get("style_code")).strip(),
            "sku": None if pd.isna(r.get("sku")) else str(r.get("sku")).strip(),
            "is_peak_week": bool(r.get("is_peak_week")),
            "plant": None if pd.isna(r.get("plant")) else str(r.get("plant")).strip(),
            "last_year_ratio_pct": to_float_or_none(r.get("last_year_ratio_pct")),
            "BASE_STOCK_QTY": to_int_or_none(r.get("BASE_STOCK_QTY")),  # 시작값만 들고 감
            "is_forecast": True,
            "loss": 0,
            "IPGO_QTY": 0,
            "shape_type": None if pd.isna(r.get("shape_type")) or str(r.get("shape_type")).strip() == "" else str(r.get("shape_type")).strip(),
            "week_no": to_int_or_none(r.get("week_no")),
        })

    return pd.DataFrame(rows)


def build_sku_weekly_forecast_2_rows(sku_df: pd.DataFrame, plc_df: pd.DataFrame) -> list:
    curr_year, curr_week = get_current_year_week()

    plc_df = deduplicate_item_plc(plc_df)

    actual_df = build_actual_rows(sku_df, plc_df, curr_year)
    forecast_df = build_forecast_rows(sku_df, plc_df, curr_year, curr_week)

    final_df = pd.concat([actual_df, forecast_df], ignore_index=True)

    if final_df.empty:
        return []

    final_df = final_df.sort_values(["sku", "plant", "week_no"], na_position="last").reset_index(drop=True)

    # 전체 행 기준으로 다시 누적 계산
    final_df = apply_base_stock_and_loss(final_df)

    return final_df.to_dict(orient="records")


def run_job(style_codes_text: str):
    client = get_supabase_client()

    st.write("1. sku_weekly_forecast 불러오는 중...")
    sku_df = load_sku_weekly_forecast_df(client)
    st.write(f"sku_weekly_forecast rows: {len(sku_df):,}")

    # style_code 입력값 파싱
    style_codes = [
        x.strip()
        for x in str(style_codes_text).replace("\n", ",").split(",")
        if str(x).strip()
    ]

    # style_code 필터
    if style_codes:
        sku_df = sku_df[sku_df["style_code"].isin(style_codes)].copy()
        st.write(f"style_code 필터 적용 후 rows: {len(sku_df):,}")
        st.write(f"선택된 style_code: {style_codes}")

    if sku_df.empty:
        raise ValueError("입력한 style_code에 해당하는 sku_weekly_forecast 데이터가 없습니다.")

    st.write("2. item_plc 불러오는 중...")
    plc_df = load_item_plc_df(client)
    st.write(f"item_plc rows: {len(plc_df):,}")

    st.write("3. sku_weekly_forecast_2 row 생성 중...")
    rows = build_sku_weekly_forecast_2_rows(sku_df, plc_df)
    st.write(f"생성 rows: {len(rows):,}")

    if not rows:
        raise ValueError("생성할 데이터가 없습니다. 원본 테이블과 item_plc 데이터를 확인하세요.")

    preview_df = pd.DataFrame(rows)
    st.write("미리보기")
    st.dataframe(preview_df.head(20), use_container_width=True)

    st.write("4. 새 데이터 insert 중...")
    insert_in_chunks(client, SKU_WEEKLY_FORECAST_2_TABLE, rows, batch_size=500)

    actual_count = int((preview_df["is_forecast"] == False).sum())
    forecast_count = int((preview_df["is_forecast"] == True).sum())

    st.success(
        f"완료: 총 {len(rows):,}건 누적 적재 / 실제 {actual_count:,}건 / 예측 {forecast_count:,}건"
    )


st.set_page_config(page_title="sku_weekly_forecast_2 적재", layout="wide")
st.title("sku_weekly_forecast_2 적재")
st.write("sku_weekly_forecast 실제값 + item_plc 비중으로 미래 주차 예측")

style_codes_text = st.text_area(
    "적재할 style_code 입력",
    placeholder="예:\nSPPPG25U18\nSPRPG24G51\nSPRPG24C62\n또는 SPPPG25U18, SPRPG24G51, SPRPG24C62",
    height=120,
)

if st.button("쌓기"):
    try:
        run_job(style_codes_text)
    except Exception as e:
        st.error(f"실패: {e}")
