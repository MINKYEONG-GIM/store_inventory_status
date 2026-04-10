import math
import os
import traceback
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

try:
    from supabase import create_client as _create_supabase_client
except ImportError:
    _create_supabase_client = None


# -----------------------------
# 공통 오류 표시
# -----------------------------
def show_detailed_exception(err: BaseException, title: str = "오류가 발생했습니다") -> None:
    st.error(title)
    st.markdown(f"**예외 종류:** `{type(err).__name__}`")
    st.code(str(err) if str(err) else "(메시지 없음)", language="text")
    tb = traceback.format_exc()
    with st.expander("전체 스택 트레이스", expanded=True):
        st.code(tb, language="text")
    print(f"[{type(err).__name__}] {err}\n{tb}", flush=True)


# -----------------------------
# Supabase 연결
# 예시 파일 방식과 맞춤
# -----------------------------
def get_supabase_client():
    if _create_supabase_client is None:
        return None

    url = ""
    key = ""

    try:
        # 1) 중첩 secrets 방식
        # [supabase]
        # url="..."
        # key="..."
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

    try:
        # 2) 네가 지금 넣은 평면 secrets 방식
        # SUPABASE_URL="..."
        # SUPABASE_KEY="..."
        if not url:
            url = str(st.secrets.get("SUPABASE_URL") or "").strip()
        if not key:
            key = str(
                st.secrets.get("SUPABASE_SERVICE_ROLE_KEY")
                or st.secrets.get("SUPABASE_KEY")
                or st.secrets.get("SUPABASE_ANON_KEY")
                or ""
            ).strip()
    except Exception:
        pass

    # 3) 환경변수 fallback
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


# -----------------------------
# 테이블명 설정
# -----------------------------
def get_step1_table_name() -> str:
    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            v = st.secrets["supabase"].get("store_inventory_status_step1_table")
            if v is not None and str(v).strip():
                return str(v).strip()
    except Exception:
        pass
    return (os.getenv("SUPABASE_STORE_INVENTORY_STATUS_STEP1_TABLE") or "store_inventory_status_step1").strip()


def get_center_stock_table_name() -> str:
    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            v = st.secrets["supabase"].get("center_stock_table")
            if v is not None and str(v).strip():
                return str(v).strip()
    except Exception:
        pass
    return (os.getenv("SUPABASE_CENTER_STOCK_TABLE") or "center_stock").strip()


def get_step2_table_name() -> str:
    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            v = st.secrets["supabase"].get("store_inventory_status_step2_table")
            if v is not None and str(v).strip():
                return str(v).strip()
    except Exception:
        pass
    return (os.getenv("SUPABASE_STORE_INVENTORY_STATUS_STEP2_TABLE") or "store_inventory_status_step2").strip()


# -----------------------------
# 유틸
# -----------------------------
def _to_float(v: Any) -> float:
    x = pd.to_numeric(v, errors="coerce")
    if pd.isna(x):
        return 0.0
    return float(x)


def fetch_supabase_table_all_rows(client, table_name: str, batch_size: int = 1000) -> List[Dict[str, Any]]:
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


def clear_table_all_rows(client, table_name: str, key_col: str = "id") -> None:
    # Supabase delete는 조건이 필요해서 절대 매칭되는 조건 사용
    client.table(table_name).delete().gte(key_col, 0).execute()


def bulk_insert_rows(client, table_name: str, rows: List[Dict[str, Any]], batch_size: int = 200) -> int:
    if not rows:
        return 0

    total = 0
    tbl = client.table(table_name)

    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        tbl.insert(chunk).execute()
        total += len(chunk)

    return total


# -----------------------------
# step2 계산
# -----------------------------
def build_step2_rows(step1_rows: List[Dict[str, Any]], center_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not step1_rows:
        return []

    step1_df = pd.DataFrame(step1_rows)
    center_df = pd.DataFrame(center_rows) if center_rows else pd.DataFrame()

    def _first_existing_col(df: pd.DataFrame, candidates: List[str]) -> str:
        for c in candidates:
            if c in df.columns:
                return c
        return ""

    # step1 필수 컬럼 보정
    sku_col = _first_existing_col(step1_df, ["sku", "SKU"])
    style_col = _first_existing_col(step1_df, ["style_code", "style", "stylecd", "STYLE_CODE"])
    shortage_col = _first_existing_col(step1_df, ["shortage_qty", "shortage", "short_qty", "SHORTAGE_QTY"])
    surplus_col = _first_existing_col(step1_df, ["surplus_qty", "surplus", "surp_qty", "SURPLUS_QTY"])
    lead_time_col = _first_existing_col(step1_df, ["lead_time", "leadtime", "lt", "LEAD_TIME"])
    # shortage_start_date 계산(또는 사용)용 후보 컬럼들
    shortage_start_col = _first_existing_col(
        step1_df,
        [
            "shortage_start_date",
            "shortage_start",
            "shortage_start_dt",
            "부족시작일",
            "부족_시작일",
            "부족발생일",
            "부족_발생일",
        ],
    )
    week_start_col = _first_existing_col(
        step1_df,
        [
            "week_start",
            "week_start_date",
            "week_start_dt",
            "sales_week_start",
            "주차시작일",
            "주차_시작일",
            "판매주차시작일",
            "판매_주차_시작일",
        ],
    )
    sales_qty_col = _first_existing_col(
        step1_df,
        [
            "sales_qty",
            "sale_qty",
            "qty_sales",
            "SALES_QTY",
            "판매수량",
            "판매_수량",
        ],
    )
    stock_qty_col = _first_existing_col(
        step1_df,
        [
            "stock_qty",
            "store_stock_qty",
            "onhand_qty",
            "on_hand_qty",
            "STOCK_QTY",
            "재고",
            "재고수량",
            "재고_수량",
        ],
    )

    if not sku_col:
        sku_col = "sku"
        step1_df[sku_col] = None
    if not style_col:
        style_col = "style_code"
        step1_df[style_col] = None
    if not shortage_col:
        shortage_col = "shortage_qty"
        step1_df[shortage_col] = None
    if not surplus_col:
        surplus_col = "surplus_qty"
        step1_df[surplus_col] = None
    if not lead_time_col:
        lead_time_col = "lead_time"
        step1_df[lead_time_col] = None
    if not shortage_start_col:
        shortage_start_col = "shortage_start_date"
        step1_df[shortage_start_col] = None

    step1_df["sku_norm"] = step1_df[sku_col].fillna("").astype(str).str.strip()
    step1_df = step1_df[step1_df["sku_norm"] != ""].copy()

    step1_df["style_code_norm"] = step1_df[style_col].fillna("").astype(str).str.strip()
    step1_df["shortage_qty_num"] = step1_df[shortage_col].apply(_to_float)
    step1_df["surplus_qty_num"] = step1_df[surplus_col].apply(_to_float)
    step1_df["lead_time_num"] = step1_df[lead_time_col].apply(_to_float)

    # 부족 시작일(shortage_start_date) 파생
    # - 우선순위: (1) step1에 명시된 shortage_start_date (2) 주차별 stock/sales로 계산 (3) today
    today = pd.Timestamp.today().normalize()
    step1_df["shortage_start_date_parsed"] = pd.to_datetime(step1_df[shortage_start_col], errors="coerce")

    computed_shortage_start = pd.DataFrame({"sku_norm": [], "computed_shortage_start_date": []})
    if week_start_col and sales_qty_col and stock_qty_col:
        tmp = step1_df.copy()
        tmp["week_start_date_parsed"] = pd.to_datetime(tmp[week_start_col], errors="coerce")
        tmp["sales_qty_num"] = tmp[sales_qty_col].apply(_to_float)
        tmp["stock_qty_num"] = tmp[stock_qty_col].apply(_to_float)

        wk = (
            tmp.dropna(subset=["week_start_date_parsed"])
            .groupby(["sku_norm", "week_start_date_parsed"], as_index=False)
            .agg(stock_qty=("stock_qty_num", "sum"), sales_qty=("sales_qty_num", "sum"))
        )

        # center_stock는 SKU별 상수로 시작재고에 포함(주차별 동일 가정)
        if center_df.empty:
            center_by_sku = pd.DataFrame({"sku_norm": [], "center_stock_qty": []})
        else:
            center_sku_col2 = _first_existing_col(center_df, ["sku", "SKU"])
            center_stock_col2 = _first_existing_col(center_df, ["stock_qty", "qty", "stock", "STOCK_QTY"])
            if not center_sku_col2:
                center_sku_col2 = "sku"
                center_df[center_sku_col2] = None
            if not center_stock_col2:
                center_stock_col2 = "stock_qty"
                center_df[center_stock_col2] = 0

            ctmp = center_df.copy()
            ctmp["sku_norm"] = ctmp[center_sku_col2].fillna("").astype(str).str.strip()
            ctmp = ctmp[ctmp["sku_norm"] != ""].copy()
            ctmp["center_stock_qty_num"] = ctmp[center_stock_col2].apply(_to_float)
            center_by_sku = ctmp.groupby("sku_norm", as_index=False).agg(center_stock_qty=("center_stock_qty_num", "sum"))

        wk = wk.merge(center_by_sku, how="left", on="sku_norm")
        wk["center_stock_qty"] = wk["center_stock_qty"].fillna(0.0)
        wk = wk.sort_values(["sku_norm", "week_start_date_parsed"]).reset_index(drop=True)

        def _calc_shortage_start_for_sku(g: pd.DataFrame):
            if g.empty:
                return pd.NaT
            start_stock = float(_to_float(g.iloc[0]["stock_qty"]) + _to_float(g.iloc[0]["center_stock_qty"]))
            balance = start_stock - g["sales_qty"].cumsum()
            neg = balance[balance < 0]
            if neg.empty:
                return pd.NaT
            first_idx = int(neg.index[0])
            return g.loc[first_idx, "week_start_date_parsed"]

        computed_shortage_start = (
            wk.groupby("sku_norm", as_index=False)
            .apply(lambda g: pd.Series({"computed_shortage_start_date": _calc_shortage_start_for_sku(g)}))
            .reset_index(drop=True)
        )

    step1_df = step1_df.merge(computed_shortage_start, how="left", on="sku_norm")
    step1_df["shortage_start_date_parsed"] = pd.to_datetime(step1_df["shortage_start_date_parsed"], errors="coerce")
    step1_df["computed_shortage_start_date"] = pd.to_datetime(step1_df["computed_shortage_start_date"], errors="coerce")
    step1_df["shortage_start_final"] = step1_df["shortage_start_date_parsed"].combine_first(
        step1_df["computed_shortage_start_date"]
    )
    step1_df["shortage_start_final"] = step1_df["shortage_start_final"].fillna(today)

    step1_agg = (
        step1_df.groupby("sku_norm", as_index=False)
        .agg(
            style_code=("style_code_norm", lambda s: next((x for x in s if str(x).strip()), "")),
            sum_shortage_qty=("shortage_qty_num", "sum"),
            sum_surplus_qty=("surplus_qty_num", "sum"),
            shortage_store_count=("shortage_qty_num", lambda s: int((s > 0).sum())),
            max_lead_time=("lead_time_num", "max"),
            shortage_start_date=("shortage_start_final", "min"),
        )
    )
    step1_agg = step1_agg.rename(columns={"sku_norm": "sku"})

    # center_stock 집계
    if center_df.empty:
        center_agg = pd.DataFrame(columns=["sku", "center_stock_qty"])
    else:
        center_sku_col = _first_existing_col(center_df, ["sku", "SKU"])
        center_stock_col = _first_existing_col(center_df, ["stock_qty", "qty", "stock", "STOCK_QTY"])
        if not center_sku_col:
            center_sku_col = "sku"
            center_df[center_sku_col] = None
        if not center_stock_col:
            center_stock_col = "stock_qty"
            center_df[center_stock_col] = 0

        center_df["sku_norm"] = center_df[center_sku_col].fillna("").astype(str).str.strip()
        center_df = center_df[center_df["sku_norm"] != ""].copy()
        center_df["stock_qty_num"] = center_df[center_stock_col].apply(_to_float)

        center_agg = (
            center_df.groupby("sku_norm", as_index=False)
            .agg(center_stock_qty=("stock_qty_num", "sum"))
        )
        center_agg = center_agg.rename(columns={"sku_norm": "sku"})

    merged = step1_agg.merge(center_agg, how="left", on="sku")
    merged["center_stock_qty"] = merged["center_stock_qty"].fillna(0.0)

    out: List[Dict[str, Any]] = []
    today = pd.Timestamp.today().normalize()

    for _, r in merged.iterrows():
        sum_shortage_qty = _to_float(r["sum_shortage_qty"])
        sum_surplus_qty = _to_float(r["sum_surplus_qty"])
        center_stock_qty = _to_float(r["center_stock_qty"])
        max_lead_time = _to_float(r["max_lead_time"])
        shortage_start_date = pd.to_datetime(r.get("shortage_start_date"), errors="coerce")
        if pd.isna(shortage_start_date):
            shortage_start_date = today

        remain_qty = sum_shortage_qty - sum_surplus_qty - center_stock_qty
        # NOT NULL 컬럼 대응: 음수/None 방지 및 타입 고정
        total_shortage_qty = max(0, int(math.ceil(remain_qty)))

        reorder_needed = remain_qty > 0

        if (sum_shortage_qty - sum_surplus_qty) <= 0:
            reorder_urgency = "불필요"
        elif remain_qty <= 0:
            reorder_urgency = "센터출고"
        elif max_lead_time > 14:
            reorder_urgency = "긴급"
        elif max_lead_time > 7:
            reorder_urgency = "주의"
        else:
            reorder_urgency = "일반"

        # order_due_date = shortage_start_date - lead_time_days
        lt_days = int(math.ceil(max(0.0, max_lead_time)))
        order_due_date = (shortage_start_date - pd.Timedelta(days=lt_days)).date().isoformat()

        out.append(
            {
                # NOT NULL 컬럼 대응: 빈 값은 ""로 저장
                "style_code": (str(r["style_code"]).strip() if str(r["style_code"]).strip() else ""),
                "sku": str(r["sku"]).strip() or "",
                # 요청 컬럼: sku 기준 합계값들
                "center_stock_qty": float(center_stock_qty),
                "surplus_qty": float(sum_surplus_qty),
                "shortage_qty": float(sum_shortage_qty),
                "total_shortage_qty": int(total_shortage_qty),
                "shortage_store_count": int(r["shortage_store_count"]),
                "lead_time": float(max(0.0, max_lead_time)),
                "reorder_needed": bool(reorder_needed),
                "reorder_urgency": str(reorder_urgency or "").strip() or "불필요",
                "shortage_start_date": shortage_start_date.date().isoformat(),
                "order_due_date": order_due_date,
            }
        )

    out.sort(key=lambda x: (x.get("sku") or ""))
    return out


def load_step2() -> Dict[str, Any]:
    client = get_supabase_client()
    if client is None:
        raise RuntimeError("Supabase 연결 불가: SUPABASE_URL / SUPABASE_KEY 설정을 확인하세요.")

    step1_table = get_step1_table_name()
    center_table = get_center_stock_table_name()
    step2_table = get_step2_table_name()

    step1_rows = fetch_supabase_table_all_rows(client, step1_table)
    center_rows = fetch_supabase_table_all_rows(client, center_table)

    result_rows = build_step2_rows(step1_rows, center_rows)

    clear_table_all_rows(client, step2_table)
    inserted = bulk_insert_rows(client, step2_table, result_rows)

    sample = []
    try:
        resp = (
            client.table(step2_table)
            .select("sku, shortage_start_date, order_due_date, center_stock_qty, surplus_qty, shortage_qty")
            .limit(10)
            .execute()
        )
        sample = resp.data if resp and getattr(resp, "data", None) else []
    except Exception:
        sample = []

    return {
        "step1_rows": len(step1_rows),
        "center_rows": len(center_rows),
        "inserted_rows": inserted,
        "sample_rows": sample,
    }


def main():
    st.set_page_config(page_title="step2 loader", layout="centered")

    st.markdown(
        """
        <style>
        header, footer, [data-testid="stToolbar"], [data-testid="stDecoration"] {
            display: none !important;
        }
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
            max-width: 320px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if st.button("데이터 쌓기", use_container_width=True):
        try:
            with st.spinner("적재 중..."):
                r = load_step2()
            st.success(
                f"완료: step1 {r['step1_rows']:,}행, "
                f"center {r['center_rows']:,}행 기준, "
                f"step2 {r['inserted_rows']:,}행 저장"
            )
            if r.get("sample_rows"):
                st.markdown("**적재 결과 샘플(최대 10행)**")
                st.dataframe(pd.DataFrame(r["sample_rows"]), use_container_width=True)
        except Exception as e:
            show_detailed_exception(e, title="적재 실패")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        show_detailed_exception(e, title="앱 실행 중 오류")
