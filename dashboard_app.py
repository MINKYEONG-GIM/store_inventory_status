import os
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

try:
    from supabase import create_client as _create_supabase_client
except ImportError:
    _create_supabase_client = None


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
# Supabase 연결
# -------------------------------------------------
def get_supabase_client():
    if _create_supabase_client is None:
        raise ImportError(
            "supabase 패키지가 없습니다. requirements.txt에 supabase를 추가하세요."
        )

    url = ""
    key = ""

    try:
        if hasattr(st, "secrets"):
            url = str(st.secrets.get("SUPABASE_URL") or "").strip()
            key = str(st.secrets.get("SUPABASE_KEY") or "").strip()

            # 네가 올린 예시 파일처럼 [supabase] 블록도 같이 지원
            if (not url or not key) and "supabase" in st.secrets:
                sec = dict(st.secrets["supabase"])
                url = str(sec.get("url") or url or "").strip()
                key = str(
                    sec.get("service_role_key")
                    or sec.get("key")
                    or sec.get("anon_key")
                    or key
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
        raise ValueError(
            "Supabase 접속 정보가 없습니다. SUPABASE_URL, SUPABASE_KEY를 secrets 또는 환경변수에 설정하세요."
        )

    return _create_supabase_client(url, key)



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


# -------------------------------------------------
# 데이터 로드
# -------------------------------------------------
@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    client = get_supabase_client()
    rows = fetch_supabase_table_all_rows(client, "store_inventory_status_step2")

    if not rows:
        return pd.DataFrame(
            columns=[
                "id",
                "created_at",
                "style_code",
                "sku",
                "current_shortage_qty",
                "shortage_store_count",
                "lead_time",
                "reorder_needed",
                "reorder_urgency",
                "order_due_date",
                "center_stock_qty",
                "surplus_qty",
                "shortage_qty",
                "shortage_start_week",
                "total_reorder_amount",
                "due_date_reorder_amount",
            ]
        )

    df = pd.DataFrame(rows)

    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        df = df.sort_values(
            by=["created_at", "style_code", "sku"],
            ascending=[False, True, True],
            na_position="last",
        )

    return df


@st.cache_data(ttl=300)
def load_forecast_data() -> pd.DataFrame:
    client = get_supabase_client()
    rows = fetch_supabase_table_all_rows(client, "sku_weekly_forecast_2")

    if not rows:
        return pd.DataFrame(
            columns=["year_week", "sale_qty", "stage", "style_code", "sku", "plant", "week_no"]
        )

    df = pd.DataFrame(rows)
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
    current_shortage_qty = row.get("current_shortage_qty")

    shortage_qty = 0 if pd.isna(shortage_qty) else float(shortage_qty)
    current_shortage_qty = 0 if pd.isna(current_shortage_qty) else float(current_shortage_qty)

    # 발주기한이 내일(오늘+1일) 미만이면 발주 시점 경과 (날짜 기준 오늘 포함)
    if days_left is not None and days_left < 1:
        return "발주시점 지남"

    # 1. 영원히 발주 안 해도 되는 후보
    # 실무에서는 완전 확정 개념은 아니므로 장기 불필요로 표시
    if (not reorder_needed) and current_shortage_qty <= 0 and shortage_qty <= 0:
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

    if category == "발주시점 지남":
        base = 500
    elif category == "즉시 발주":
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



def _to_float(value: Any) -> float:
    x = pd.to_numeric(value, errors="coerce")
    if pd.isna(x):
        return 0.0
    return float(x)



def _current_iso_week() -> int:
    return int(datetime.today().isocalendar().week)



def compute_sku_metrics_from_forecast(forecast_df: pd.DataFrame, dashboard_df: pd.DataFrame) -> pd.DataFrame:
    if forecast_df.empty:
        return pd.DataFrame(columns=[
            "sku",
            "season_remaining_qty_until_maturity",
            "recommended_order_qty_now",
        ])

    work = forecast_df.copy()
    for col in ["sku", "style_code", "stage", "plant"]:
        if col in work.columns:
            work[col] = work[col].astype(str).str.strip()

    if "sale_qty" in work.columns:
        work["sale_qty"] = pd.to_numeric(work["sale_qty"], errors="coerce").fillna(0)
    else:
        work["sale_qty"] = 0

    if "week_no" in work.columns:
        work["week_no_num"] = pd.to_numeric(work["week_no"], errors="coerce")
    else:
        work["week_no_num"] = pd.NA

    current_week = _current_iso_week()

    step2_by_sku = dashboard_df.copy()
    if not step2_by_sku.empty:
        for col in ["lead_time", "current_shortage_qty", "shortage_qty"]:
            if col in step2_by_sku.columns:
                step2_by_sku[col] = pd.to_numeric(step2_by_sku[col], errors="coerce")
        step2_by_sku = step2_by_sku.sort_values("created_at", ascending=False, na_position="last")
        step2_by_sku = step2_by_sku.drop_duplicates(subset=["sku"], keep="first")
    else:
        step2_by_sku = pd.DataFrame(columns=["sku", "lead_time", "current_shortage_qty", "shortage_qty"])

    result_rows = []

    for sku, g in work.groupby("sku", dropna=False):
        sku = str(sku).strip()
        if not sku:
            continue

        g = g.copy()
        g = g[g["week_no_num"].notna()]
        if g.empty:
            season_remaining_qty_until_maturity = 0.0
            recommended_order_qty_now = 0.0
        else:
            g["week_no_num"] = g["week_no_num"].astype(int)
            g = g.sort_values(["week_no_num", "stage"], na_position="last")
            g_future = g[g["week_no_num"] >= current_week].copy()

            # 성숙기까지 남은 판매량
            # 현재 주차부터 시작해서 stage가 '쇠퇴기'로 바뀌기 전까지의 sale_qty 합
            season_remaining_qty_until_maturity = 0.0
            if not g_future.empty:
                weekly_stage = (
                    g_future.groupby("week_no_num", as_index=False)
                    .agg(
                        sale_qty=("sale_qty", "sum"),
                        stage=("stage", lambda x: str(next((v for v in x if str(v).strip()), "")).strip()),
                    )
                    .sort_values("week_no_num")
                )

                for _, row in weekly_stage.iterrows():
                    stage = str(row.get("stage") or "").strip()
                    if stage == "쇠퇴기":
                        break
                    season_remaining_qty_until_maturity += _to_float(row.get("sale_qty"))

            # 당장 발주량
            # 정의: 전체 매장이 lead time + 4주를 버틸 만큼 필요한 발주량
            # step2의 current_shortage_qty를 우선 사용하고, 없으면 forecast로 근사 계산
            step2_row = step2_by_sku[step2_by_sku["sku"].astype(str) == sku]
            recommended_order_qty_now = 0.0

            if not step2_row.empty:
                r = step2_row.iloc[0]
                current_shortage_qty = _to_float(r.get("current_shortage_qty"))
                shortage_qty = _to_float(r.get("shortage_qty"))
                recommended_order_qty_now = max(current_shortage_qty, shortage_qty, 0.0)

                if recommended_order_qty_now <= 0:
                    lead_time_days = _to_float(r.get("lead_time"))
                    lead_time_weeks = max(0, int((lead_time_days + 6) // 7))
                    target_weeks = lead_time_weeks + 4

                    if not g_future.empty:
                        weekly_sales = (
                            g_future.groupby("week_no_num", as_index=False)["sale_qty"]
                            .sum()
                            .sort_values("week_no_num")
                        )
                        recommended_order_qty_now = float(weekly_sales.head(max(target_weeks, 1))["sale_qty"].sum())
            else:
                weekly_sales = (
                    g_future.groupby("week_no_num", as_index=False)["sale_qty"]
                    .sum()
                    .sort_values("week_no_num")
                ) if not g_future.empty else pd.DataFrame(columns=["week_no_num", "sale_qty"])
                recommended_order_qty_now = float(weekly_sales.head(4)["sale_qty"].sum()) if not weekly_sales.empty else 0.0

        result_rows.append(
            {
                "sku": sku,
                "season_remaining_qty_until_maturity": int(round(max(season_remaining_qty_until_maturity, 0.0))),
                "recommended_order_qty_now": int(round(max(recommended_order_qty_now, 0.0))),
            }
        )

    return pd.DataFrame(result_rows)



def prepare_dataframe(df: pd.DataFrame, forecast_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    if df.empty:
        return df

    date_cols = ["created_at", "order_due_date"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    numeric_cols = [
        "current_shortage_qty",
        "shortage_store_count",
        "lead_time",
        "center_stock_qty",
        "surplus_qty",
        "shortage_qty",
        "total_reorder_amount",
        "due_date_reorder_amount",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["days_left"] = df["order_due_date"].apply(compute_days_left)
    df["action_category"] = df.apply(classify_row, axis=1)
    df["priority_score"] = df.apply(priority_score, axis=1)

    def badge_text(category: str) -> str:
        mapping = {
            "발주시점 지남": "⏱ 발주시점 지남",
            "즉시 발주": "🔴 즉시 발주",
            "곧 발주": "🟠 곧 발주",
            "발주 검토": "🟡 발주 검토",
            "발주 불필요": "🟢 발주 불필요",
            "장기 불필요": "⚪ 장기 불필요",
        }
        return mapping.get(category, category)

    df["status_badge"] = df["action_category"].apply(badge_text)

    if forecast_df is not None and not forecast_df.empty:
        sku_metrics = compute_sku_metrics_from_forecast(forecast_df, df)
        if not sku_metrics.empty:
            df = df.merge(sku_metrics, on="sku", how="left")
        else:
            df["season_remaining_qty_until_maturity"] = None
            df["recommended_order_qty_now"] = None
    else:
        df["season_remaining_qty_until_maturity"] = None
        df["recommended_order_qty_now"] = None

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
    category_options = ["발주시점 지남", "즉시 발주", "곧 발주", "발주 검토", "발주 불필요", "장기 불필요"]
    selected_categories = st.sidebar.multiselect(
        "발주 상태",
        options=category_options,
        default=category_options,
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
        filtered = filtered[filtered["days_left"].notna() & (filtered["days_left"] < 1)]

    if only_shortage:
        filtered = filtered[
            filtered["shortage_qty"].fillna(0) > 0
        ]

    filtered = filtered[
        filtered["shortage_store_count"].fillna(0) >= min_shortage_store_count
    ]

    filtered = filtered.sort_values(
        by=["priority_score", "days_left", "current_shortage_qty"],
        ascending=[False, True, False],
        na_position="last",
    )

    return filtered


# -------------------------------------------------
# KPI
# -------------------------------------------------
def render_kpis(df: pd.DataFrame):
    past_order_point_count = (df["action_category"] == "발주시점 지남").sum()
    immediate_count = (df["action_category"] == "즉시 발주").sum()
    soon_count = (df["action_category"] == "곧 발주").sum()
    reorder_count = df["reorder_needed"].apply(normalize_boolean).sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("발주시점 지남", int(past_order_point_count))
    c2.metric("즉시 발주", int(immediate_count))
    c3.metric("곧 발주", int(soon_count))
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
        "season_remaining_qty_until_maturity": "성숙기까지 예상판매수량",
        "recommended_order_qty_now": "권장 발주량(지금)",
        "reorder_urgency": "긴급도",
        "reorder_needed": "발주필요",
        "days_left": "D-day",
        "order_due_date": "발주기한",
        "current_shortage_qty": "현재 부족수량",
        "shortage_qty": "부족수량",
        "surplus_qty": "여유수량",
        "center_stock_qty": "센터재고",
        "shortage_store_count": "부족매장수",
        "lead_time": "리드타임",
        "total_reorder_amount": "총 발주금액",
        "due_date_reorder_amount": "기한 기준 발주금액",
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
                "season_remaining_qty_until_maturity",
                "recommended_order_qty_now",
                "current_shortage_qty",
                "shortage_qty",
                "surplus_qty",
                "center_stock_qty",
                "shortage_store_count",
                "lead_time",
                "total_reorder_amount",
                "due_date_reorder_amount",
                "reorder_needed",
                "reorder_urgency",
                "order_due_date",
                "shortage_start_week",
                "created_at",
            ],
            "값": [
                detail.get("style_code"),
                detail.get("sku"),
                detail.get("season_remaining_qty_until_maturity"),
                detail.get("recommended_order_qty_now"),
                detail.get("current_shortage_qty"),
                detail.get("shortage_qty"),
                detail.get("surplus_qty"),
                detail.get("center_stock_qty"),
                detail.get("shortage_store_count"),
                detail.get("lead_time"),
                detail.get("total_reorder_amount"),
                detail.get("due_date_reorder_amount"),
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
        forecast_df = load_forecast_data()
    except Exception as e:
        st.error(f"데이터를 불러오지 못했습니다: {e}")
        st.stop()

    if raw_df.empty:
        st.warning("store_inventory_status_step2 테이블에 데이터가 없습니다.")
        st.stop()

    df = prepare_dataframe(raw_df, forecast_df)
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
            - **성숙기까지 예상판매수량**
              - `sku_weekly_forecast_2`에서 현재 주차부터 시작
              - `stage = 쇠퇴기`가 나오기 전까지의 주차별 `sale_qty` 합
              - 즉, 성숙기 종료 전까지 더 팔릴 것으로 예상되는 수량

            - **권장 발주량(지금)**
              - 우선 `store_inventory_status_step2.current_shortage_qty`를 사용
              - 이 값은 실무적으로 전체 매장이 `lead time + 4주`를 버티도록 채워야 하는 부족분으로 보는 것이 가장 적절함
              - 값이 비어 있거나 0이면, 보조 계산으로 `sku_weekly_forecast_2`의 향후 `lead time + 4주` 판매예측 합을 사용

            - **발주시점 지남**
              - `order_due_date`가 내일(오늘+1일) 미만인 경우 (날짜 기준 당일 포함)

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
              - 부족수량이 없고, 현재 부족수량도 없고, 발주기한도 없는 경우
              - 실무적으로는 '당분간 발주 필요 없음' 의미로 해석하는 것이 안전합니다.
            """
        )


if __name__ == "__main__":
    main()
