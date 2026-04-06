import math
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
import streamlit as st
from supabase import create_client, Client


# =========================
# 기본 설정
# =========================
st.set_page_config(
    page_title="Inventory Management Dashboard",
    layout="wide"
)

ACTION_TABLE = "inventory_action_plan_step2"
ROTATION_TABLE = "stock_rotation_plan_step2"
STATUS_TABLE = "store_inventory_status_step1"
WEEKLY_TABLE = "sku_weekly_forecast"


# =========================
# Supabase 연결
# =========================
@st.cache_resource
def get_supabase_client() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)


# =========================
# 공통 유틸
# =========================
def to_float(v: Any, default: float = 0.0) -> float:
    x = pd.to_numeric(v, errors="coerce")
    if pd.isna(x):
        return default
    return float(x)


def to_int(v: Any, default: int = 0) -> int:
    x = pd.to_numeric(v, errors="coerce")
    if pd.isna(x):
        return default
    return int(round(float(x)))


def parse_year_week_sort_key(year_week: str) -> Tuple[int, int]:
    try:
        s = str(year_week).strip()
        y, w = s.split("-")
        return int(y), int(w)
    except Exception:
        return (0, 0)


def year_week_to_ts(year_week: str) -> pd.Timestamp:
    try:
        y, w = parse_year_week_sort_key(year_week)
        return pd.to_datetime(f"{y}-W{w:02d}-1", format="%G-W%V-%u", errors="coerce")
    except Exception:
        return pd.NaT


def current_year_week() -> str:
    iso = pd.Timestamp.today().isocalendar()
    return f"{int(iso.year)}-{int(iso.week):02d}"


def diff_weeks_from_now(year_week: str) -> Optional[int]:
    if not year_week or year_week == "NOW":
        return 0 if year_week == "NOW" else None

    target = year_week_to_ts(year_week)
    now = year_week_to_ts(current_year_week())

    if pd.isna(target) or pd.isna(now):
        return None

    return int((target - now).days // 7)


def fetch_all_rows(
    client: Client,
    table_name: str,
    select_cols: str = "*",
    page_size: int = 5000
) -> pd.DataFrame:
    all_rows: List[Dict[str, Any]] = []
    start = 0

    while True:
        end = start + page_size - 1
        resp = (
            client.table(table_name)
            .select(select_cols)
            .range(start, end)
            .execute()
        )
        rows = resp.data or []

        if not rows:
            break

        all_rows.extend(rows)

        if len(rows) < page_size:
            break

        start += page_size

    return pd.DataFrame(all_rows)


def fetch_filtered_rows(
    client: Client,
    table_name: str,
    select_cols: str = "*",
    filters: Optional[List[Tuple[str, str, Any]]] = None,
    page_size: int = 5000,
) -> pd.DataFrame:
    all_rows: List[Dict[str, Any]] = []
    start = 0

    while True:
        end = start + page_size - 1
        query = client.table(table_name).select(select_cols)

        if filters:
            for col, op, val in filters:
                if op == "eq":
                    query = query.eq(col, val)
                elif op == "in":
                    query = query.in_(col, val)

        resp = query.range(start, end).execute()
        rows = resp.data or []

        if not rows:
            break

        all_rows.extend(rows)

        if len(rows) < page_size:
            break

        start += page_size

    return pd.DataFrame(all_rows)


# =========================
# 메인 대시보드용 데이터 로딩
# raw 100만행은 여기서 안 불러옴
# =========================
@st.cache_data(ttl=180)
def load_dashboard_base_tables() -> Dict[str, pd.DataFrame]:
    client = get_supabase_client()

    action_df = fetch_all_rows(
        client,
        ACTION_TABLE,
        "sty,sku,plant,lead_time,current_qty_after_rotation,rotation_in_qty,rotation_out_qty,shortage_start_year_week,shortage_qty_after_rotation,center_alloc_qty,reorder_qty,reorder_action_year_week,final_action,priority_rank,reason"
    )

    rotation_df = fetch_all_rows(
        client,
        ROTATION_TABLE,
        "*"
    )

    status_df = fetch_all_rows(
        client,
        STATUS_TABLE,
        "sty,sku,plant,store_classification,lead_time,current_qty,stock_weeks,shortage_qty,surplus_qty"
    )

    return {
        "action": action_df,
        "rotation": rotation_df,
        "status": status_df,
    }


# =========================
# SKU 상세용 raw weekly 조회
# 여기서만 부분 조회
# =========================
@st.cache_data(ttl=180)
def load_weekly_by_sku(sku: str) -> pd.DataFrame:
    client = get_supabase_client()

    df = fetch_filtered_rows(
        client,
        WEEKLY_TABLE,
        "sty,sku,plant,store_name,year_week,sale_qty,is_forecast,begin_stock",
        filters=[("sku", "eq", sku)]
    )

    if df.empty:
        return df

    for col in ["sty", "sku", "plant", "store_name", "year_week"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].astype(str).str.strip()

    for col in ["sale_qty", "begin_stock"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if "is_forecast" not in df.columns:
        df["is_forecast"] = False

    df["sort_key"] = df["year_week"].apply(parse_year_week_sort_key)
    df = df.sort_values(["plant", "sort_key"]).reset_index(drop=True)
    return df


# =========================
# 요약 계산
# =========================
def build_dashboard_summary(action_df: pd.DataFrame, rotation_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    if action_df.empty:
        return pd.DataFrame(), {}

    df = action_df.copy()

    for col in ["sty", "sku", "plant", "final_action", "reorder_action_year_week", "shortage_start_year_week"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].astype(str).str.strip()

    for col in ["center_alloc_qty", "reorder_qty", "shortage_qty_after_rotation"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["reorder_due_weeks"] = df["reorder_action_year_week"].apply(diff_weeks_from_now)
    df["shortage_due_weeks"] = df["shortage_start_year_week"].apply(diff_weeks_from_now)

    rotation_sku_df = pd.DataFrame()
    if not rotation_df.empty:
        tmp = rotation_df.copy()
        for col in ["sty", "sku"]:
            if col not in tmp.columns:
                tmp[col] = ""
            tmp[col] = tmp[col].astype(str).str.strip()

        if "transfer_qty" not in tmp.columns:
            tmp["transfer_qty"] = 0
        tmp["transfer_qty"] = pd.to_numeric(tmp["transfer_qty"], errors="coerce").fillna(0)

        rotation_sku_df = (
            tmp.groupby(["sty", "sku"], dropna=False, as_index=False)["transfer_qty"]
            .sum()
            .rename(columns={"transfer_qty": "rotation_transfer_qty"})
        )

    sku_summary = (
        df.groupby(["sty", "sku"], as_index=False)
        .agg(
            plant_cnt=("plant", "nunique"),
            shortage_qty_after_rotation=("shortage_qty_after_rotation", "sum"),
            center_alloc_qty=("center_alloc_qty", "sum"),
            reorder_qty=("reorder_qty", "sum"),
            action_cnt=("final_action", "count"),
            now_reorder_cnt=("reorder_action_year_week", lambda s: (s == "NOW").sum()),
            center_only_cnt=("final_action", lambda s: (s == "CENTER_ONLY").sum()),
            wait_inbound_cnt=("final_action", lambda s: (s == "WAIT_INBOUND").sum()),
            center_and_reorder_cnt=("final_action", lambda s: (s == "CENTER_AND_REORDER").sum()),
            reorder_only_cnt=("final_action", lambda s: (s == "REORDER_ONLY").sum()),
        )
    )

    if not rotation_sku_df.empty:
        sku_summary = sku_summary.merge(rotation_sku_df, on=["sty", "sku"], how="left")
    else:
        sku_summary["rotation_transfer_qty"] = 0

    style_summary = (
        sku_summary.groupby("sty", as_index=False)
        .agg(
            sku_cnt=("sku", "nunique"),
            plant_cnt=("plant_cnt", "sum"),
            total_shortage_qty=("shortage_qty_after_rotation", "sum"),
            total_center_alloc_qty=("center_alloc_qty", "sum"),
            total_reorder_qty=("reorder_qty", "sum"),
            total_rotation_qty=("rotation_transfer_qty", "sum"),
            now_reorder_sku_cnt=("now_reorder_cnt", lambda s: (s > 0).sum()),
            center_only_sku_cnt=("center_only_cnt", lambda s: (s > 0).sum()),
            wait_inbound_sku_cnt=("wait_inbound_cnt", lambda s: (s > 0).sum()),
            center_and_reorder_sku_cnt=("center_and_reorder_cnt", lambda s: (s > 0).sum()),
            reorder_only_sku_cnt=("reorder_only_cnt", lambda s: (s > 0).sum()),
        )
        .sort_values(
            ["total_reorder_qty", "total_center_alloc_qty", "total_rotation_qty"],
            ascending=[False, False, False]
        )
        .reset_index(drop=True)
    )

    kpis = {
        "now_reorder_sku_cnt": int((sku_summary["now_reorder_cnt"] > 0).sum()),
        "center_only_sku_cnt": int((sku_summary["center_only_cnt"] > 0).sum()),
        "wait_inbound_sku_cnt": int((sku_summary["wait_inbound_cnt"] > 0).sum()),
        "rotation_sku_cnt": int((sku_summary["rotation_transfer_qty"] > 0).sum()),
        "total_reorder_qty": int(sku_summary["reorder_qty"].sum()),
        "total_center_alloc_qty": int(sku_summary["center_alloc_qty"].sum()),
    }

    return style_summary, kpis


def build_sku_detail_summary(
    sty: str,
    action_df: pd.DataFrame,
    rotation_df: pd.DataFrame,
    status_df: pd.DataFrame
) -> pd.DataFrame:
    act = action_df[action_df["sty"].astype(str).str.strip() == sty].copy()
    stat = status_df[status_df["sty"].astype(str).str.strip() == sty].copy()

    if act.empty and stat.empty:
        return pd.DataFrame()

    act_sku = (
        act.groupby(["sty", "sku"], as_index=False)
        .agg(
            plant_cnt=("plant", "nunique"),
            total_center_alloc_qty=("center_alloc_qty", "sum"),
            total_reorder_qty=("reorder_qty", "sum"),
            total_shortage_qty=("shortage_qty_after_rotation", "sum"),
            any_reorder_now=("reorder_action_year_week", lambda s: (s == "NOW").any()),
            final_action_sample=("final_action", "first"),
        )
    )

    stat_sku = (
        stat.groupby(["sty", "sku"], as_index=False)
        .agg(
            shortage_store_cnt=("store_classification", lambda s: (s == "부족매장").sum()),
            surplus_store_cnt=("store_classification", lambda s: (s == "여유매장").sum()),
            keep_store_cnt=("store_classification", lambda s: (s == "유지매장").sum()),
        )
    )

    out = act_sku.merge(stat_sku, on=["sty", "sku"], how="outer")

    if not rotation_df.empty:
        rot = rotation_df[rotation_df["sty"].astype(str).str.strip() == sty].copy()
        if "transfer_qty" not in rot.columns:
            rot["transfer_qty"] = 0
        rot["transfer_qty"] = pd.to_numeric(rot["transfer_qty"], errors="coerce").fillna(0)

        rot_sku = (
            rot.groupby(["sty", "sku"], as_index=False)["transfer_qty"]
            .sum()
            .rename(columns={"transfer_qty": "rotation_transfer_qty"})
        )
        out = out.merge(rot_sku, on=["sty", "sku"], how="left")
    else:
        out["rotation_transfer_qty"] = 0

    out = out.fillna(0)
    return out.sort_values(
        ["total_reorder_qty", "total_center_alloc_qty", "rotation_transfer_qty"],
        ascending=[False, False, False]
    ).reset_index(drop=True)


def build_related_sku_recommendation(selected_sty: str, selected_sku: str, action_df: pd.DataFrame) -> pd.DataFrame:
    df = action_df.copy()
    df = df[df["sty"].astype(str).str.strip() == selected_sty].copy()
    df = df[df["sku"].astype(str).str.strip() != selected_sku].copy()

    if df.empty:
        return pd.DataFrame()

    out = (
        df.groupby(["sty", "sku"], as_index=False)
        .agg(
            total_reorder_qty=("reorder_qty", "sum"),
            total_center_alloc_qty=("center_alloc_qty", "sum"),
            reorder_action_year_week=("reorder_action_year_week", "first"),
            final_action=("final_action", "first"),
        )
    )

    out["reorder_due_weeks"] = out["reorder_action_year_week"].apply(diff_weeks_from_now)

    # 같이 발주 추천 규칙
    # reorder_qty > 0 이고, NOW 또는 1주 이내면 우선
    out["bundle_score"] = out.apply(
        lambda r: (
            100 if r["reorder_action_year_week"] == "NOW" else
            80 if (r["reorder_due_weeks"] is not None and r["reorder_due_weeks"] <= 1) else
            50 if to_int(r["total_reorder_qty"], 0) > 0 else
            10
        ),
        axis=1
    )

    out = out.sort_values(
        ["bundle_score", "total_reorder_qty", "total_center_alloc_qty"],
        ascending=[False, False, False]
    ).reset_index(drop=True)

    return out


# =========================
# 화면
# =========================
def main():
    st.title("재고 운영 통합 대시보드")

    base = load_dashboard_base_tables()
    action_df = base["action"]
    rotation_df = base["rotation"]
    status_df = base["status"]

    if action_df.empty:
        st.error("inventory_action_plan_step2 테이블에 데이터가 없습니다.")
        return

    style_summary, kpis = build_dashboard_summary(action_df, rotation_df)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("즉시 리오더 SKU", f"{kpis.get('now_reorder_sku_cnt', 0):,}")
    c2.metric("센터 즉시 배분 SKU", f"{kpis.get('center_only_sku_cnt', 0):,}")
    c3.metric("입고 대기 SKU", f"{kpis.get('wait_inbound_sku_cnt', 0):,}")
    c4.metric("회전 발생 SKU", f"{kpis.get('rotation_sku_cnt', 0):,}")
    c5.metric("총 리오더 수량", f"{kpis.get('total_reorder_qty', 0):,}")
    c6.metric("총 센터 배분 수량", f"{kpis.get('total_center_alloc_qty', 0):,}")

    tab1, tab2, tab3 = st.tabs(["전체 운영판", "스타일 상세", "SKU 상세"])

    with tab1:
        st.subheader("스타일별 운영 요약")

        sty_keyword = st.text_input("스타일 검색", "")
        view_df = style_summary.copy()

        if sty_keyword.strip():
            view_df = view_df[
                view_df["sty"].astype(str).str.contains(sty_keyword.strip(), case=False, na=False)
            ].copy()

        st.dataframe(view_df, use_container_width=True, hide_index=True)

        st.subheader("긴급 SKU 리스트")
        urgent_df = action_df.copy()
        urgent_df = urgent_df[
            (urgent_df["reorder_action_year_week"].astype(str) == "NOW") |
            (urgent_df["final_action"].astype(str).isin(["CENTER_AND_REORDER", "REORDER_ONLY"]))
        ].copy()

        urgent_df = urgent_df.sort_values(
            ["reorder_qty", "center_alloc_qty"],
            ascending=[False, False]
        )

        show_cols = [
            "sty", "sku", "plant", "final_action",
            "shortage_start_year_week", "reorder_action_year_week",
            "center_alloc_qty", "reorder_qty", "reason"
        ]
        show_cols = [c for c in show_cols if c in urgent_df.columns]
        st.dataframe(urgent_df[show_cols], use_container_width=True, hide_index=True)

    with tab2:
        st.subheader("스타일 상세")

        sty_options = sorted([s for s in action_df["sty"].dropna().astype(str).str.strip().unique().tolist() if s])
        if not sty_options:
            st.info("스타일 데이터가 없습니다.")
        else:
            selected_sty = st.selectbox("스타일 선택", sty_options)

            sku_summary_df = build_sku_detail_summary(
                sty=selected_sty,
                action_df=action_df,
                rotation_df=rotation_df,
                status_df=status_df
            )

            st.write(f"선택 스타일: {selected_sty}")
            st.dataframe(sku_summary_df, use_container_width=True, hide_index=True)

    with tab3:
        st.subheader("SKU 상세")

        sku_options = sorted([s for s in action_df["sku"].dropna().astype(str).str.strip().unique().tolist() if s])
        if not sku_options:
            st.info("SKU 데이터가 없습니다.")
        else:
            selected_sku = st.selectbox("SKU 선택", sku_options)

            sku_action_df = action_df[action_df["sku"].astype(str).str.strip() == selected_sku].copy()
            sku_status_df = status_df[status_df["sku"].astype(str).str.strip() == selected_sku].copy()

            if sku_action_df.empty:
                st.info("선택한 SKU의 액션 데이터가 없습니다.")
            else:
                selected_sty = str(sku_action_df["sty"].dropna().astype(str).iloc[0]).strip() if len(sku_action_df) > 0 else ""

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("총 센터 배분", f"{int(pd.to_numeric(sku_action_df['center_alloc_qty'], errors='coerce').fillna(0).sum()):,}")
                m2.metric("총 리오더", f"{int(pd.to_numeric(sku_action_df['reorder_qty'], errors='coerce').fillna(0).sum()):,}")
                m3.metric("회전 IN", f"{int(pd.to_numeric(sku_action_df['rotation_in_qty'], errors='coerce').fillna(0).sum()):,}")
                m4.metric("회전 OUT", f"{int(pd.to_numeric(sku_action_df['rotation_out_qty'], errors='coerce').fillna(0).sum()):,}")

                st.markdown("### 매장별 최종 액션")
                action_show_cols = [
                    "sty", "sku", "plant", "lead_time",
                    "current_qty_after_rotation",
                    "rotation_in_qty", "rotation_out_qty",
                    "shortage_start_year_week", "shortage_qty_after_rotation",
                    "center_alloc_qty", "reorder_qty",
                    "reorder_action_year_week", "final_action", "reason"
                ]
                action_show_cols = [c for c in action_show_cols if c in sku_action_df.columns]
                st.dataframe(sku_action_df[action_show_cols], use_container_width=True, hide_index=True)

                st.markdown("### 매장별 상태(step1)")
                status_show_cols = [
                    "sty", "sku", "plant", "store_classification",
                    "lead_time", "current_qty", "stock_weeks",
                    "shortage_qty", "surplus_qty"
                ]
                status_show_cols = [c for c in status_show_cols if c in sku_status_df.columns]
                st.dataframe(sku_status_df[status_show_cols], use_container_width=True, hide_index=True)

                st.markdown("### 주차별 판매 / 예측")
                weekly_df = load_weekly_by_sku(selected_sku)

                if weekly_df.empty:
                    st.info("주차별 데이터가 없습니다.")
                else:
                    plant_options = ["전체"] + sorted([p for p in weekly_df["plant"].dropna().astype(str).str.strip().unique().tolist() if p])
                    selected_plant = st.selectbox("매장 선택", plant_options)

                    if selected_plant != "전체":
                        weekly_view = weekly_df[weekly_df["plant"].astype(str).str.strip() == selected_plant].copy()
                    else:
                        weekly_view = (
                            weekly_df.groupby(["sty", "sku", "year_week", "is_forecast", "sort_key"], as_index=False)
                            .agg(
                                sale_qty=("sale_qty", "sum"),
                                begin_stock=("begin_stock", "sum")
                            )
                            .sort_values("sort_key")
                        )

                    weekly_view["label"] = weekly_view["year_week"].astype(str)

                    chart_df = weekly_view[["label", "sale_qty"]].copy()
                    chart_df = chart_df.set_index("label")

                    st.line_chart(chart_df)

                    st.dataframe(
                        weekly_view[["year_week", "is_forecast", "sale_qty", "begin_stock"]],
                        use_container_width=True,
                        hide_index=True
                    )

                st.markdown("### 같은 스타일의 다른 SKU 추천")
                related_df = build_related_sku_recommendation(
                    selected_sty=selected_sty,
                    selected_sku=selected_sku,
                    action_df=action_df
                )

                if related_df.empty:
                    st.info("같이 볼 다른 SKU가 없습니다.")
                else:
                    related_show_cols = [
                        "sty", "sku", "final_action",
                        "total_reorder_qty", "total_center_alloc_qty",
                        "reorder_action_year_week", "reorder_due_weeks", "bundle_score"
                    ]
                    related_show_cols = [c for c in related_show_cols if c in related_df.columns]
                    st.dataframe(related_df[related_show_cols], use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
