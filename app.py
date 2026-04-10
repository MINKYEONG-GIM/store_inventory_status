"""
sku_weekly_forecast_2 전체를 읽어 매장(SKU×plant)별 지표를 계산한 뒤
store_inventory_status_step1 에 적재합니다.
"""
import math
import os
import traceback
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

try:
    from supabase import create_client as _create_supabase_client
except ImportError:
    _create_supabase_client = None

st.set_page_config(page_title="데이터 쌓기", layout="centered")

DEFAULT_LEAD_TIME_DAYS = 21
DEFAULT_INVENTORY_SAFETY_WEEKS = 2.0


def show_detailed_exception(err: BaseException, title: str = "오류가 발생했습니다") -> None:
    st.error(title)
    st.markdown(f"**예외 종류:** `{type(err).__name__}`")
    st.code(str(err) if str(err) else "(메시지 없음)", language="text")
    tb = traceback.format_exc()
    with st.expander("전체 스택 트레이스", expanded=True):
        st.code(tb, language="text")
    print(f"[{type(err).__name__}] {err}\n{tb}", flush=True)


def get_supabase_client():
    if _create_supabase_client is None:
        return None
    url = ""
    key = ""
    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            sec = dict(st.secrets["supabase"])
            url = str(sec.get("url") or "").strip()
            key = str(
                sec.get("service_role_key")
                or sec.get("key")
                or sec.get("anon_key")
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
        return None
    return _create_supabase_client(url, key)


def get_store_inventory_status_step1_table_name() -> str:
    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            v = st.secrets["supabase"].get("store_inventory_status_step1_table")
            if v is not None and str(v).strip():
                return str(v).strip()
    except Exception:
        pass
    return (
        os.getenv("SUPABASE_STORE_INVENTORY_STATUS_STEP1_TABLE")
        or "store_inventory_status_step1"
    ).strip()


def get_sku_weekly_forecast_table_name() -> str:
    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            v = st.secrets["supabase"].get("sku_weekly_forecast_table")
            if v is not None and str(v).strip():
                return str(v).strip()
    except Exception:
        pass
    return (os.getenv("SUPABASE_SKU_WEEKLY_FORECAST_TABLE") or "sku_weekly_forecast_2").strip()


def get_inventory_safety_weeks() -> float:
    safety = DEFAULT_INVENTORY_SAFETY_WEEKS
    try:
        if hasattr(st, "secrets") and "inventory_policy" in st.secrets:
            sec = dict(st.secrets["inventory_policy"])
            if sec.get("safety_weeks") is not None and str(sec.get("safety_weeks")).strip() != "":
                safety = float(sec["safety_weeks"])
    except Exception:
        pass
    env_s = (os.getenv("INVENTORY_SAFETY_WEEKS") or "").strip()
    if env_s:
        try:
            safety = float(env_s)
        except ValueError:
            pass
    return max(0.0, safety)


def get_lead_time_days() -> int:
    d = DEFAULT_LEAD_TIME_DAYS
    try:
        if hasattr(st, "secrets") and "inventory_policy" in st.secrets:
            sec = dict(st.secrets["inventory_policy"])
            if sec.get("lead_time_days") is not None and str(sec.get("lead_time_days")).strip() != "":
                d = int(float(sec["lead_time_days"]))
    except Exception:
        pass
    env_d = (os.getenv("LEAD_TIME_DAYS") or "").strip()
    if env_d:
        try:
            d = int(float(env_d))
        except ValueError:
            pass
    return max(1, d)


def fetch_supabase_table_all_rows(
    client,
    table_name: str,
    batch_size: int = 1000,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    off = 0
    while True:
        try:
            resp = (
                client.table(table_name)
                .select("*")
                .limit(batch_size)
                .offset(off)
                .execute()
            )
        except Exception:
            resp = (
                client.table(table_name)
                .select("*")
                .range(off, off + batch_size - 1)
                .execute()
            )
        chunk = resp.data if resp.data else []
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < batch_size:
            break
        off += batch_size
    return rows


def _to_float_qty(v: Any) -> float:
    x = pd.to_numeric(v, errors="coerce")
    if pd.isna(x):
        return 0.0
    return float(x)


def _col(df: pd.DataFrame, *names: str) -> Optional[str]:
    for n in names:
        if n in df.columns:
            return n
    lower = {str(c).lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None


def pick_base_stock_for_iso_week(df_plant: pd.DataFrame, cw: int) -> float:
    wcol = _col(df_plant, "week_no")
    bcol = _col(df_plant, "BASE_STOCK_QTY", "base_stock_qty")
    if wcol is None or bcol is None:
        return 0.0
    d = df_plant.copy()
    d["_wn"] = pd.to_numeric(d[wcol], errors="coerce")
    d = d.dropna(subset=["_wn"])
    if d.empty:
        return 0.0
    d["_wn"] = d["_wn"].astype(int)
    exact = d[d["_wn"] == int(cw)]
    if not exact.empty:
        return max(0.0, _to_float_qty(exact.iloc[0][bcol]))
    le = d[d["_wn"] <= int(cw)]
    if not le.empty:
        le = le.sort_values("_wn")
        return max(0.0, _to_float_qty(le.iloc[-1][bcol]))
    d = d.sort_values("_wn")
    return max(0.0, _to_float_qty(d.iloc[0][bcol]))


def simulate_inventory_runway_weeks(
    start_stock: float,
    weekly_sales: List[Tuple[int, float]],
) -> Tuple[float, float]:
    rem = max(0.0, float(start_stock))
    weeks_cover = 0.0
    last_pos_q = 0.0
    for _wn, q_raw in weekly_sales:
        q = max(0.0, float(q_raw))
        if q > 0:
            last_pos_q = q
        if rem <= 1e-12:
            break
        if q <= 1e-12:
            weeks_cover += 1.0
            continue
        if rem <= q:
            weeks_cover += rem / q
            rem = 0.0
            break
        rem -= q
        weeks_cover += 1.0
    extra_weeks_from_tail = 0.0
    if rem > 1e-6 and last_pos_q > 1e-12:
        extra_weeks_from_tail = rem / last_pos_q
        rem = 0.0
    total = weeks_cover + extra_weeks_from_tail
    return total, rem


def classify_band(inv_w: float, lead_weeks: float, safety_w: float) -> str:
    low = lead_weeks + safety_w
    if math.isinf(inv_w):
        return "여유 매장"
    if inv_w <= low + 1e-9:
        return "부족 매장"
    return "여유 매장"


def compute_step1_rows_from_forecast_df(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """sku_weekly_forecast_2 DataFrame → store_inventory_status_step1 적재용 레코드."""
    if df is None or df.empty:
        return []

    sale_c = _col(df, "sale_qty")
    plant_c = _col(df, "plant")
    sku_c = _col(df, "sku")
    wn_c = _col(df, "week_no")
    style_c = _col(df, "style_code")

    if not all([sale_c, plant_c, sku_c, wn_c]):
        raise ValueError(
            f"sku_weekly_forecast_2에 필요한 컬럼이 없습니다. "
            f"있는 컬럼: {list(df.columns)}"
        )

    lead_days = get_lead_time_days()
    lead_weeks = max(0.0, float(lead_days) / 7.0)
    safety_w = get_inventory_safety_weeks()
    low_th = lead_weeks + safety_w

    cw = int(pd.Timestamp.today().isocalendar().week)

    df = df.copy()
    df["_sku"] = df[sku_c].astype(str).str.strip()
    df["_plant"] = df[plant_c].fillna("").astype(str).str.strip().replace("", "전체")

    out: List[Dict[str, Any]] = []

    for (_sku, _plant), g in df.groupby(["_sku", "_plant"], dropna=False):
        sku_s = str(_sku).strip()
        plant_s = str(_plant).strip() if _plant is not None else "전체"
        if not sku_s:
            continue

        g2 = g.copy()
        g2["_wn"] = pd.to_numeric(g2[wn_c], errors="coerce")
        g2 = g2.dropna(subset=["_wn"])
        if g2.empty:
            continue
        g2["_wn"] = g2["_wn"].astype(int)
        g2 = g2.sort_values("_wn", kind="mergesort")
        agg_sale = g2.groupby("_wn", as_index=False)[sale_c].sum()

        weekly_list: List[Tuple[int, float]] = []
        for _, r in agg_sale.iterrows():
            wn = int(r["_wn"])
            if wn < cw:
                continue
            weekly_list.append((wn, _to_float_qty(r[sale_c])))

        g_for_base = g2.copy()
        g_for_base["week_no"] = g_for_base["_wn"]
        if wn_c in g_for_base.columns and wn_c != "week_no":
            g_for_base = g_for_base.drop(columns=[wn_c])
        start_stock = pick_base_stock_for_iso_week(g_for_base, cw)

        sty = ""
        if style_c is not None:
            try:
                sty = str(g2.iloc[0][style_c]).strip()
            except Exception:
                sty = ""

        if not weekly_list:
            all_q = agg_sale[sale_c].map(_to_float_qty)
            avg_q = float(all_q.mean()) if not all_q.empty else 0.0
            if avg_q > 1e-12:
                inv_w = float(start_stock) / avg_q
                tail_rem = 0.0
            else:
                inv_w = float("inf") if start_stock > 1e-6 else 0.0
                tail_rem = max(0.0, float(start_stock))
        else:
            inv_w, tail_rem = simulate_inventory_runway_weeks(start_stock, weekly_list)

        band = classify_band(inv_w, lead_weeks, safety_w)

        fwd_vals = [q for _wn, q in weekly_list if q > 0]
        avg_pos = float(pd.Series(fwd_vals).mean()) if fwd_vals else 0.0
        if avg_pos <= 0 and weekly_list:
            avg_pos = float(pd.Series([q for _, q in weekly_list]).mean()) or 0.0
        if avg_pos <= 0 and not weekly_list:
            aq = agg_sale[sale_c].map(_to_float_qty)
            avg_pos = float(aq.mean()) if not aq.empty else 0.0

        shortfall_weeks = (
            max(0.0, low_th - inv_w) if band == "부족 매장" and not math.isinf(inv_w) else 0.0
        )
        surplus_weeks = (
            max(0.0, inv_w - low_th) if band == "여유 매장" and not math.isinf(inv_w) else 0.0
        )

        est_short = int(round(shortfall_weeks * avg_pos)) if shortfall_weeks > 0 else 0
        est_surplus = int(round(surplus_weeks * avg_pos)) if surplus_weeks > 0 else 0

        inv_display = 9999.99 if math.isinf(inv_w) else round(float(inv_w), 4)

        out.append(
            {
                "style_code": sty or None,
                "sku": sku_s,
                "plant": plant_s,
                "store_classification": band,
                "lead_time": float(lead_days),
                "current_qty": int(round(start_stock)),
                "stock_weeks": float(inv_display),
                "shortage_qty": est_short if est_short > 0 else None,
                "surplus_qty": est_surplus if est_surplus > 0 else None,
            }
        )

    return out


def clear_step1_table(client) -> None:
    tbl = get_store_inventory_status_step1_table_name()
    sentinel = "\uffff\uffff__never_match_sku__\uffff\uffff"
    client.table(tbl).delete().neq("sku", sentinel).execute()


def bulk_insert_step1(client, rows: List[Dict[str, Any]], batch_size: int = 200) -> int:
    if not rows:
        return 0
    tbl = client.table(get_store_inventory_status_step1_table_name())
    n = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        tbl.insert(chunk).execute()
        n += len(chunk)
    return n


def run_stack_data(client) -> Dict[str, Any]:
    wf_tbl = get_sku_weekly_forecast_table_name()
    raw = fetch_supabase_table_all_rows(client, wf_tbl)
    if not raw:
        return {"forecast_rows": 0, "step1_rows": 0, "message": "sku_weekly_forecast_2 가 비어 있습니다."}

    df = pd.DataFrame(raw)
    step1_rows = compute_step1_rows_from_forecast_df(df)
    clear_step1_table(client)
    inserted = bulk_insert_step1(client, step1_rows)
    return {
        "forecast_rows": len(raw),
        "step1_rows": inserted,
        "groups": len(step1_rows),
    }


def main() -> None:
    if st.button("데이터 쌓기", type="primary"):
        sb = get_supabase_client()
        if sb is None:
            st.error("Supabase 연결 불가. secrets [supabase] url·service_role_key 를 설정하세요.")
            return
        with st.spinner(
            f"{get_sku_weekly_forecast_table_name()} 전체 로드 후 "
            f"{get_store_inventory_status_step1_table_name()} 에 적재 중…"
        ):
            try:
                r = run_stack_data(sb)
                st.success(
                    f"완료: 예측 테이블 {r['forecast_rows']:,}행 기준 → "
                    f"매장 조합 {r.get('groups', 0):,}건 → "
                    f"{get_store_inventory_status_step1_table_name()} {r['step1_rows']:,}행 저장."
                )
                if r.get("message"):
                    st.info(r["message"])
            except Exception as e:
                show_detailed_exception(e, title="데이터 쌓기 실패")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        show_detailed_exception(e, title="앱 실행 중 오류")
