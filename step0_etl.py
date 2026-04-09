"""
RAW FILE + item_plc → sku_weekly_forecast 단순 적재 스크립트.
환경변수 DATABASE_URL (또는 PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE) 로 연결합니다.

  pip install psycopg2-binary pandas

실행: python "import math.py"
"""

from __future__ import annotations

import os
import sys
from datetime import date
from typing import Any, Dict, List, Optional

import pandas as pd

try:
    import psycopg2
    from psycopg2.extras import execute_batch
except ImportError:
    print("psycopg2-binary 가 필요합니다: pip install psycopg2-binary", file=sys.stderr)
    raise


def get_conn():
    url = os.getenv("DATABASE_URL", "").strip()
    if url:
        return psycopg2.connect(url)
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
        dbname=os.getenv("PGDATABASE", "postgres"),
    )


def item_code_from_sku(sku: str) -> str:
    s = str(sku).strip()
    if len(s) >= 4:
        return s[2:4]
    return s


def calday_to_year_week(calday: Any) -> Optional[str]:
    try:
        d = int(float(str(calday).replace(".0", "")))
        ts = pd.to_datetime(str(d), format="%Y%m%d", errors="coerce")
        if pd.isna(ts):
            return None
        y, w, _ = ts.isocalendar()
        return f"{int(y)}-{int(w)}"
    except Exception:
        return None


def load_raw_file(conn) -> pd.DataFrame:
    q = """
    SELECT "CALDAY", "PLANT", sku, style_code,
           "SALE_QTY", "IPGO_QTY", "BASE_STOCK_QTY", "STOCK_CHANGE_QTY"
    FROM public."RAW FILE"
    """
    df = pd.read_sql_query(q, conn)
    if df.empty:
        return df
    df = df.rename(columns={c: str(c) for c in df.columns})
    df["year_week"] = df["CALDAY"].map(calday_to_year_week)
    df = df.dropna(subset=["year_week", "sku"])
    df["sku"] = df["sku"].astype(str).str.strip()
    df["PLANT"] = df.get("PLANT", "").astype(str).str.strip().replace("", "전체")
    for col in ("SALE_QTY", "IPGO_QTY", "BASE_STOCK_QTY", "STOCK_CHANGE_QTY"):
        if col not in df.columns:
            df[col] = 0.0
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    agg = (
        df.groupby(["PLANT", "sku", "year_week"], as_index=False)
        .agg(
            style_code=("style_code", lambda s: s.dropna().astype(str).str.strip().iloc[0] if len(s.dropna()) else ""),
            SALE_QTY=("SALE_QTY", "sum"),
            IPGO_QTY=("IPGO_QTY", "sum"),
            BASE_STOCK_QTY=("BASE_STOCK_QTY", "mean"),
            STOCK_CHANGE_QTY=("STOCK_CHANGE_QTY", "sum"),
        )
    )
    agg["item_code"] = agg["sku"].map(item_code_from_sku)
    return agg


def load_item_plc(conn) -> pd.DataFrame:
    q = """
    SELECT item_code, item_name, year_week, month, sales, stage,
           peak_week, peak_month, last_year_ratio_pct
    FROM public.item_plc
    """
    df = pd.read_sql_query(q, conn)
    if df.empty:
        return df
    df["item_code"] = df["item_code"].astype(str).str.strip()
    df["year_week"] = df["year_week"].astype(str).str.strip()
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce")
    return df


PLC_FALLBACK_ITEM_CODE = "평균"


def split_item_plc(plc: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """일반 행 vs item_code='평균' fallback. 평균은 year_week당 한 행만 쓴다."""
    if plc.empty:
        return plc, plc.iloc[0:0].copy()
    is_avg = plc["item_code"] == PLC_FALLBACK_ITEM_CODE
    plc_specific = plc[~is_avg].copy()
    plc_avg = plc[is_avg].copy()
    if not plc_avg.empty:
        plc_avg = plc_avg.drop_duplicates(subset=["year_week"], keep="first")
    if not plc_specific.empty:
        plc_specific = plc_specific.drop_duplicates(subset=["item_code", "year_week"], keep="first")
    return plc_specific, plc_avg


def year_week_to_iso_week(yw: str) -> Optional[int]:
    try:
        parts = str(yw).strip().split("-")
        if len(parts) >= 2:
            return int(parts[1])
    except Exception:
        pass
    return None


def _plc_columns_for_join(plc_sp: pd.DataFrame) -> List[str]:
    base = {"item_code", "year_week"}
    return [c for c in plc_sp.columns if c not in base]


def attach_plc_with_fallback(raw_agg: pd.DataFrame, plc: pd.DataFrame) -> pd.DataFrame:
    """
    1) item_plc: RAW의 item_code + year_week 와 동일한 행으로 매칭
    2) 없으면 item_plc.item_code == '평균' 인 행 중 같은 year_week 사용
    """
    if raw_agg.empty:
        return raw_agg
    if plc.empty:
        out = raw_agg.copy()
        for c in ("item_name", "sales", "stage", "peak_week", "peak_month", "month", "last_year_ratio_pct"):
            out[c] = pd.NA
        return out

    plc_sp, plc_avg = split_item_plc(plc)
    plc_cols = _plc_columns_for_join(plc_sp) if not plc_sp.empty else _plc_columns_for_join(plc)

    if plc_sp.empty:
        m = raw_agg.copy()
        m["_merge"] = "left_only"
    else:
        m = raw_agg.merge(
            plc_sp,
            on=["item_code", "year_week"],
            how="left",
            indicator=True,
        )

    if not plc_avg.empty:
        fb = plc_avg.drop(columns=["item_code"], errors="ignore").copy()
        rename_fb = {c: f"{c}_fb" for c in fb.columns if c != "year_week"}
        fb = fb.rename(columns=rename_fb)
        m = m.merge(fb, on="year_week", how="left")

    for c in plc_cols:
        c_fb = f"{c}_fb"
        if c_fb not in m.columns:
            continue
        if c in m.columns:
            m[c] = m[c].where(m["_merge"] == "both", m[c_fb])
        else:
            m[c] = m[c_fb]

    drop_cols = ["_merge"] + [c for c in m.columns if c.endswith("_fb")]
    m = m.drop(columns=[c for c in drop_cols if c in m.columns], errors="ignore")
    return m


def build_forecast_rows(raw_agg: pd.DataFrame, plc: pd.DataFrame) -> List[Dict[str, Any]]:
    if raw_agg.empty:
        return []

    today_week = date.today().isocalendar()[1]
    merged = attach_plc_with_fallback(raw_agg, plc)

    rows: List[Dict[str, Any]] = []

    for _, r in merged.iterrows():
        plant = str(r.get("PLANT", "전체") or "전체").strip() or "전체"
        sku = str(r.get("sku", "") or r.get("item_code", "")).strip()
        yw = str(r.get("year_week", "")).strip()
        if not yw:
            continue

        sale_raw = r.get("SALE_QTY")
        sale_plc = r.get("sales")
        if pd.notna(sale_raw) and float(sale_raw) != 0:
            sale_qty = float(sale_raw)
        elif pd.notna(sale_plc):
            sale_qty = float(sale_plc)
        else:
            sale_qty = 0.0

        stage = r.get("stage")
        stage_s = "" if pd.isna(stage) else str(stage).strip()

        pw = r.get("peak_week")
        iso_w = year_week_to_iso_week(yw)
        is_peak = False
        if pd.notna(pw) and iso_w is not None:
            try:
                is_peak = int(pw) == int(iso_w)
            except Exception:
                is_peak = False

        is_forecast = iso_w is not None and iso_w > today_week

        style = r.get("style_code")
        style_s = "" if pd.isna(style) else str(style).strip()

        name = r.get("item_name")
        sku_name = "" if pd.isna(name) else str(name).strip()

        base = r.get("BASE_STOCK_QTY")
        begin_stock = None if pd.isna(base) else int(round(float(base)))

        rows.append(
            {
                "year_week": yw,
                "sale_qty": sale_qty,
                "stage": stage_s or None,
                "style_code": style_s or None,
                "sku": sku or None,
                "is_peak_week": is_peak,
                "plant": plant,
                "avg_discount_rate": None,
                "sku_name": sku_name or None,
                "store_name": plant,
                "begin_stock": begin_stock,
                "is_forecast": is_forecast,
                "loss": None,
                "inbound_qty": int(round(float(r.get("IPGO_QTY") or 0))),
                "outbound_qty": int(round(float(r.get("STOCK_CHANGE_QTY") or 0))),
            }
        )

    return rows


def clear_and_insert(conn, rows: List[Dict[str, Any]], batch_size: int = 500) -> int:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM public.sku_weekly_forecast")
    conn.commit()

    if not rows:
        return 0

    cols = [
        "year_week",
        "sale_qty",
        "stage",
        "style_code",
        "sku",
        "is_peak_week",
        "plant",
        "avg_discount_rate",
        "sku_name",
        "store_name",
        "begin_stock",
        "is_forecast",
        "loss",
        "inbound_qty",
        "outbound_qty",
    ]
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"""
    INSERT INTO public.sku_weekly_forecast (
        {", ".join(cols)}
    ) VALUES ({placeholders})
    """

    tuples = [tuple(row.get(c) for c in cols) for row in rows]

    with conn.cursor() as cur:
        execute_batch(cur, sql, tuples, page_size=batch_size)
    conn.commit()
    return len(rows)


def main() -> None:
    conn = get_conn()
    try:
        raw_agg = load_raw_file(conn)
        plc = load_item_plc(conn)
        rows = build_forecast_rows(raw_agg, plc)
        n = clear_and_insert(conn, rows)
        print(f"완료: sku_weekly_forecast {n}행 적재 (RAW 집계 {len(raw_agg)}행, item_plc {len(plc)}행)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
