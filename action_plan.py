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

    # step1 필수 컬럼 보정
    for col in ["sku", "style_code", "shortage_qty", "surplus_qty", "lead_time"]:
        if col not in step1_df.columns:
            step1_df[col] = None

    step1_df["sku"] = step1_df["sku"].fillna("").astype(str).str.strip()
    step1_df = step1_df[step1_df["sku"] != ""].copy()

    step1_df["style_code"] = step1_df["style_code"].fillna("").astype(str).str.strip()
    step1_df["shortage_qty_num"] = step1_df["shortage_qty"].apply(_to_float)
    step1_df["surplus_qty_num"] = step1_df["surplus_qty"].apply(_to_float)
    step1_df["lead_time_num"] = step1_df["lead_time"].apply(_to_float)

    step1_agg = (
        step1_df.groupby("sku", as_index=False)
        .agg(
            style_code=("style_code", lambda s: next((x for x in s if str(x).strip()), "")),
            sum_shortage_qty=("shortage_qty_num", "sum"),
            sum_surplus_qty=("surplus_qty_num", "sum"),
            shortage_store_count=("shortage_qty_num", lambda s: int((s > 0).sum())),
            max_lead_time=("lead_time_num", "max"),
        )
    )

    # center_stock 집계
    if center_df.empty:
        center_agg = pd.DataFrame(columns=["sku", "center_stock_qty"])
    else:
        if "sku" not in center_df.columns:
            center_df["sku"] = None
        if "stock_qty" not in center_df.columns:
            center_df["stock_qty"] = 0

        center_df["sku"] = center_df["sku"].fillna("").astype(str).str.strip()
        center_df = center_df[center_df["sku"] != ""].copy()
        center_df["stock_qty_num"] = center_df["stock_qty"].apply(_to_float)

        center_agg = (
            center_df.groupby("sku", as_index=False)
            .agg(center_stock_qty=("stock_qty_num", "sum"))
        )

    merged = step1_agg.merge(center_agg, how="left", on="sku")
    merged["center_stock_qty"] = merged["center_stock_qty"].fillna(0.0)

    out: List[Dict[str, Any]] = []

    today = pd.Timestamp.today().date()

    for _, r in merged.iterrows():
        sum_shortage_qty = _to_float(r["sum_shortage_qty"])
        sum_surplus_qty = _to_float(r["sum_surplus_qty"])
        center_stock_qty = _to_float(r["center_stock_qty"])
        max_lead_time = _to_float(r["max_lead_time"])

        remain_qty = sum_shortage_qty - sum_surplus_qty - center_stock_qty
        total_shortage_qty = int(math.ceil(remain_qty))

        reorder_needed = remain_qty > 0

        if (sum_shortage_qty - sum_surplus_qty) <= 0:
            reorder_urgency = "불필요"
        elif remain_qty <= 0:
            reorder_urgency = "센터출고"
        elif max_lead_time <= 7:
            reorder_urgency = "긴급"
        elif max_lead_time <= 14:
            reorder_urgency = "주의"
        else:
            reorder_urgency = "일반"

        order_due_date = None
        if reorder_needed:
            order_due_date = (today + pd.Timedelta(days=int(math.ceil(max_lead_time)))).isoformat()

        out.append(
            {
                "style_code": (str(r["style_code"]).strip() or None),
                "sku": str(r["sku"]).strip(),
                "total_shortage_qty": total_shortage_qty,
                "shortage_store_count": int(r["shortage_store_count"]),
                "lead_time": float(max_lead_time),
                "reorder_needed": bool(reorder_needed),
                "reorder_urgency": reorder_urgency,
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

    return {
        "step1_rows": len(step1_rows),
        "center_rows": len(center_rows),
        "inserted_rows": inserted,
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
        except Exception as e:
            show_detailed_exception(e, title="적재 실패")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        show_detailed_exception(e, title="앱 실행 중 오류")
