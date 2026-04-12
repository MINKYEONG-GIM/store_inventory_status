import os
from datetime import date
from typing import Optional

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text


# -------------------------------------------------
# 기본 설정
# -------------------------------------------------
st.set_page_config(
    page_title="SKU Reorder Dashboard",
    page_icon="📦",
    layout="wide",
)

st.title("SKU 발주 현황 대시보드")
st.caption("store_inventory_status_step2 기준 발주 우선순위 조회 화면")


# -------------------------------------------------
# DB 연결
# -------------------------------------------------
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL")

    if db_url:
        return create_engine(db_url, pool_pre_ping=True)

    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT", "5432")
    dbname = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")

    if not all([host, dbname, user, password]):
        raise ValueError(
            "DB 접속 정보가 없습니다. DATABASE_URL 또는 PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD를 설정하세요."
        )

    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}?sslmode=require"
    return create_engine(conn_str, pool_pre_ping=True)


# -------------------------------------------------
# 데이터 로드
# -------------------------------------------------
@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    query = text(
        """
        SELECT
            id,
            created_at,
            style_code,
            sku,
            total_shortage_qty,
            shortage_store_count,
            lead_time,
            reorder_needed,
            reorder_urgency,
            order_due_date,
            center_stock_qty,
            surplus_qty,
            shortage_qty,
            shortage_start_week
        FROM public.store_inventory_status_step2
        ORDER BY created_at DESC, style_code, sku
        """
    )

    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    return df


# -------------------------------------------------
# 분류 로직
# -------------------------------------------------
def normalize_boolean(value) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ["true", "t", "1", "yes", "y"]
    return bool(value)



def compute_days_left(order_due_date: Optional[pd.Timestamp]) -> Optional[int]:
    if pd.isna(order_due_date):
        return None
    today = date.today()
    return (pd.to_datetime(order_due_date).date() - today).days



def classify_row(row: pd.Series) -> str:
    reorder_needed = normalize_boolean(row.get("reorder_needed"))
    urgency = str(row.get("reorder_urgency") or "").strip()
    days_left = row.get("days_left")
    shortage_qty = row.get("shortage_qty")
    total_shortage_qty = row.get("total_shortage_qty")

    shortage_qty = 0 if pd.isna(shortage_qty) else float(shortage_qty)
    total_shortage_qty = 0 if pd.isna(total_shortage_qty) else float(total_shortage_qty)

    # 1. 영원히 발주 안 해도 되는 후보
    # 실무에서는 완전 확정 개념은 아니므로 장기 불필요로 표시
    if (not reorder_needed) and total_shortage_qty <= 0 and shortage_qty <= 0:
        if pd.isna(row.get("order_due_date")):
            return "장기 불필요"

    # 2. 즉시 발주
    if reorder_needed:
        if urgency in ["긴급", "urgent", "critical"]:
            return "즉시 발주"
        if days_left is not None and days_left < 0:
            return "즉시 발주"
        if days_left is not None and days_left <= 3:
            return "즉시 발주"

    # 3. 곧 발주
    if reorder_needed:
        if urgency in ["주의", "보통", "normal", "warning"]:
            return "곧 발주"
        if days_left is not None and 4 <= days_left <= 14:
            return "곧 발주"
        return "발주 검토"

    # 4. 발주 불필요
    return "발주 불필요"



def priority_score(row: pd.Series) -> int:
    category = row.get("action_category")
    days_left = row.get("days_left")

    if category == "즉시 발주":
        base = 400
    elif category == "곧 발주":
        base = 300
    elif category == "발주 검토":
        base = 200
    elif category == "발주 불필요":
        base = 100
    else:
        base = 0

    if days_left is None:
        return base

    # 날짜가 더 급할수록 점수 높게
    return base + max(0, 30 - days_left)



def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    date_cols = ["created_at", "order_due_date"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    numeric_cols = [
        "total_shortage_qty",
        "shortage_store_count",
        "lead_time",
        "center_stock_qty",
        "surplus_qty",
        "shortage_qty",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["days_left"] = df["order_due_date"].apply(compute_days_left)
    df["action_category"] = df.apply(classify_row, axis=1)
    df["priority_score"] = df.apply(priority_score, axis=1)

    # 화면 표시용 상태 배지 텍스트
    def badge_text(category: str) -> str:
        mapping = {
            "즉시 발주": "🔴 즉시 발주",
            "곧 발주": "🟠 곧 발주",
            "발주 검토": "🟡 발주 검토",
            "발주 불필요": "🟢 발주 불필요",
            "장기 불필요": "⚪ 장기 불필요",
        }
        return mapping.get(category, category)

    df["status_badge"] = df["action_category"].apply(badge_text)
    return df


# -------------------------------------------------
# 사이드바 필터
# -------------------------------------------------
def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("조회 조건")

    st.sidebar.write("### 검색")
    style_keyword = st.sidebar.text_input("스타일코드 검색")
    sku_keyword = st.sidebar.text_input("SKU 검색")

    st.sidebar.write("### 상태 필터")
    category_options = ["즉시 발주", "곧 발주", "발주 검토", "발주 불필요", "장기 불필요"]
    selected_categories = st.sidebar.multiselect(
        "발주 상태",
        options=category_options,
        default=["즉시 발주", "곧 발주", "발주 검토", "발주 불필요", "장기 불필요"],
    )

    urgency_options = sorted([x for x in df["reorder_urgency"].dropna().astype(str).unique().tolist() if x.strip()])
    selected_urgencies = st.sidebar.multiselect(
        "reorder_urgency",
        options=urgency_options,
        default=urgency_options,
    )

    st.sidebar.write("### 수치 조건")
    only_reorder_needed = st.sidebar.checkbox("reorder_needed = true만 보기", value=False)
    only_overdue = st.sidebar.checkbox("발주기한 지난 것만 보기", value=False)
    only_shortage = st.sidebar.checkbox("부족수량 있는 것만 보기", value=False)

    min_shortage_store_count = st.sidebar.number_input(
        "최소 부족 매장 수",
        min_value=0,
        value=0,
        step=1,
    )

    filtered = df.copy()

    if style_keyword:
        filtered = filtered[
            filtered["style_code"].astype(str).str.contains(style_keyword, case=False, na=False)
        ]

    if sku_keyword:
        filtered = filtered[
            filtered["sku"].astype(str).str.contains(sku_keyword, case=False, na=False)
        ]

    if selected_categories:
        filtered = filtered[filtered["action_category"].isin(selected_categories)]

    if selected_urgencies:
        filtered = filtered[
            filtered["reorder_urgency"].astype(str).isin(selected_urgencies)
            | filtered["reorder_urgency"].isna()
        ]

    if only_reorder_needed:
        filtered = filtered[filtered["reorder_needed"].apply(normalize_boolean)]

    if only_overdue:
        filtered = filtered[filtered["days_left"].notna() & (filtered["days_left"] < 0)]

    if only_shortage:
        filtered = filtered[
            filtered["shortage_qty"].fillna(0) > 0
        ]

    filtered = filtered[
        filtered["shortage_store_count"].fillna(0) >= min_shortage_store_count
    ]

    filtered = filtered.sort_values(
        by=["priority_score", "days_left", "total_shortage_qty"],
        ascending=[False, True, False],
        na_position="last",
    )

    return filtered


# -------------------------------------------------
# KPI
# -------------------------------------------------
def render_kpis(df: pd.DataFrame):
    immediate_count = (df["action_category"] == "즉시 발주").sum()
    soon_count = (df["action_category"] == "곧 발주").sum()
    overdue_count = ((df["days_left"].notna()) & (df["days_left"] < 0)).sum()
    reorder_count = df["reorder_needed"].apply(normalize_boolean).sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("즉시 발주", int(immediate_count))
    c2.metric("곧 발주", int(soon_count))
    c3.metric("기한 초과", int(overdue_count))
    c4.metric("발주 필요", int(reorder_count))


# -------------------------------------------------
# 메인 화면
# -------------------------------------------------
def render_main_table(df: pd.DataFrame):
    view_df = df.copy()

    view_df["order_due_date"] = view_df["order_due_date"].dt.strftime("%Y-%m-%d")
    view_df["created_at"] = view_df["created_at"].dt.strftime("%Y-%m-%d %H:%M:%S")

    display_columns = {
        "status_badge": "발주상태",
        "style_code": "스타일코드",
        "sku": "SKU",
        "reorder_urgency": "긴급도",
        "reorder_needed": "발주필요",
        "days_left": "D-day",
        "order_due_date": "발주기한",
        "total_shortage_qty": "총부족수량",
        "shortage_qty": "부족수량",
        "surplus_qty": "여유수량",
        "center_stock_qty": "센터재고",
        "shortage_store_count": "부족매장수",
        "lead_time": "리드타임",
        "shortage_start_week": "부족시작주차",
        "created_at": "생성시각",
    }

    view_df = view_df[list(display_columns.keys())].rename(columns=display_columns)

    st.subheader("발주 대상 목록")
    st.dataframe(
        view_df,
        use_container_width=True,
        hide_index=True,
    )



def render_download(df: pd.DataFrame):
    csv = df.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        label="CSV 다운로드",
        data=csv,
        file_name="store_inventory_status_step2_dashboard.csv",
        mime="text/csv",
    )



def render_detail_panel(df: pd.DataFrame):
    st.subheader("SKU 상세 보기")

    sku_list = df["sku"].dropna().astype(str).unique().tolist()
    if not sku_list:
        st.info("현재 조건에 맞는 SKU가 없습니다.")
        return

    selected_sku = st.selectbox("상세 확인할 SKU 선택", sku_list)
    detail = df[df["sku"].astype(str) == selected_sku].iloc[0]

    col1, col2, col3 = st.columns(3)
    col1.metric("발주상태", detail["action_category"])
    col2.metric("발주기한", str(detail["order_due_date"].date()) if pd.notna(detail["order_due_date"]) else "없음")
    col3.metric("D-day", int(detail["days_left"]) if pd.notna(detail["days_left"]) else "없음")

    st.write("### 상세 정보")
    detail_table = pd.DataFrame(
        {
            "항목": [
                "style_code",
                "sku",
                "total_shortage_qty",
                "shortage_qty",
                "surplus_qty",
                "center_stock_qty",
                "shortage_store_count",
                "lead_time",
                "reorder_needed",
                "reorder_urgency",
                "order_due_date",
                "shortage_start_week",
                "created_at",
            ],
            "값": [
                detail.get("style_code"),
                detail.get("sku"),
                detail.get("total_shortage_qty"),
                detail.get("shortage_qty"),
                detail.get("surplus_qty"),
                detail.get("center_stock_qty"),
                detail.get("shortage_store_count"),
                detail.get("lead_time"),
                detail.get("reorder_needed"),
                detail.get("reorder_urgency"),
                detail.get("order_due_date"),
                detail.get("shortage_start_week"),
                detail.get("created_at"),
            ],
        }
    )
    st.dataframe(detail_table, use_container_width=True, hide_index=True)


# -------------------------------------------------
# 실행
# -------------------------------------------------
def main():
    try:
        raw_df = load_data()
    except Exception as e:
        st.error(f"데이터를 불러오지 못했습니다: {e}")
        st.stop()

    if raw_df.empty:
        st.warning("store_inventory_status_step2 테이블에 데이터가 없습니다.")
        st.stop()

    df = prepare_dataframe(raw_df)
    filtered_df = apply_filters(df)

    render_kpis(filtered_df)
    st.divider()

    tab1, tab2 = st.tabs(["전체 목록", "상세 보기"])

    with tab1:
        render_main_table(filtered_df)
        render_download(filtered_df)

    with tab2:
        render_detail_panel(filtered_df)

    with st.expander("판정 기준 설명"):
        st.markdown(
            """
            - **즉시 발주**
              - `reorder_needed = true` 이고
              - `reorder_urgency = 긴급` 이거나
              - `order_due_date`가 이미 지났거나 3일 이내인 경우

            - **곧 발주**
              - `reorder_needed = true` 이고
              - 발주기한이 4~14일 남았거나 `reorder_urgency`가 주의/보통인 경우

            - **발주 검토**
              - 발주는 필요하지만 즉시/곧 발주까지는 아닌 경우

            - **발주 불필요**
              - 현재 기준으로 발주 필요가 없는 경우

            - **장기 불필요**
              - 부족수량이 없고, 총부족수량도 없고, 발주기한도 없는 경우
              - 실무적으로는 '당분간 발주 필요 없음' 의미로 해석하는 것이 안전합니다.
            """
        )


if __name__ == "__main__":
    main()
