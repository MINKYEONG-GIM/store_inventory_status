import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from supabase import create_client, Client


# =========================
# 기본 설정
# =========================
st.set_page_config(
    page_title="재고 운영 대시보드",
    layout="wide",
    initial_sidebar_state="expanded",
)

ACTION_TABLE = "inventory_action_plan_step2"
ROTATION_TABLE = "stock_rotation_plan_step2"
STATUS_TABLE = "store_inventory_status_step1"
WEEKLY_TABLE = "sku_weekly_forecast"


# =========================
# 스타일
# =========================
st.markdown("""
<style>
.block-container {
    padding-top: 1.2rem;
    padding-bottom: 1.5rem;
}
.card {
    background: #0f172a;
    border: 1px solid #1e293b;
    border-radius: 16px;
    padding: 18px 18px 14px 18px;
    margin-bottom: 12px;
}
.card-title {
    font-size: 0.9rem;
    color: #94a3b8;
    margin-bottom: 6px;
}
.card-value {
    font-size: 1.8rem;
    font-weight: 700;
    color: #f8fafc;
}
.badge {
    display: inline-block;
    padding: 4px 10px;
    border-radius: 999px;
    font-size: 0.78rem;
    font-weight: 700;
}
.badge-red { background: rgba(239,68,68,0.16); color: #f87171; }
.badge-yellow { background: rgba(245,158,11,0.16); color: #fbbf24; }
.badge-blue { background: rgba(59,130,246,0.16); color: #60a5fa; }
.badge-green { background: rgba(34,197,94,0.16); color: #4ade80; }
.section-title {
    font-size: 1.05rem;
    font-weight: 700;
    margin: 6px 0 12px 0;
}
.small-muted {
    color: #94a3b8;
    font-size: 0.85rem;
}
</style>
""", unsafe_allow_html=True)


# =========================
# Supabase 연결
# =========================
@st.cache_resource
def get_supabase_client() -> Client:
    url = ""
    key = ""

    try:
        if "SUPABASE_URL" in st.secrets:
            url = str(st.secrets["SUPABASE_URL"]).strip()
        if "SUPABASE_KEY" in st.secrets:
            key = str(st.secrets["SUPABASE_KEY"]).strip()
    except Exception:
        pass

    if not url:
        url = os.getenv("SUPABASE_URL", "").strip()
    if not key:
        key = os.getenv("SUPABASE_KEY", "").strip()

    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL 또는 SUPABASE_KEY 설정이 없습니다. "
            "Streamlit secrets 또는 환경변수에 넣어주세요."
        )

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


def year_week_to_timestamp(year_week: str) -> pd.Timestamp:
    try:
        y, w = parse_year_week_sort_key(year_week)
        return pd.to_datetime(f"{y}-W{w:02d}-1", format="%G-W%V-%u", errors="coerce")
    except Exception:
        return pd.NaT


def current_year_week() -> str:
    iso = pd.Timestamp.today().isocalendar()
    return f"{int(iso.year)}-{int(iso.week):02d}"


def diff_weeks_from_now(year_week: str) -> Optional[int]:
    if not year_week:
        return None
    if year_week == "NOW":
        return 0

    now_ts = year_week_to_timestamp(current_year_week())
    target_ts = year_week_to_timestamp(year_week)

    if pd.isna(now_ts) or pd.isna(target_ts):
        return None

    return int((target_ts - now_ts).days // 7)


def urgency_label_from_week(week_value: str) -> str:
    diff = diff_weeks_from_now(str(week_value).strip())
    if diff is None:
        return "여유"
    if diff <= 0:
        return "긴급"
    if diff <= 1:
        return "1주 이내"
    if diff <= 2:
        return "2주 이내"
    return "여유"


def urgency_rank(label: str) -> int:
    mapping = {
        "긴급": 0,
        "1주 이내": 1,
        "2주 이내": 2,
        "여유": 3,
    }
    return mapping.get(label, 9)


def urgency_badge(label: str) -> str:
    if label == "긴급":
        return '<span class="badge badge-red">긴급</span>'
    if label == "1주 이내":
        return '<span class="badge badge-yellow">1주 이내</span>'
    if label == "2주 이내":
        return '<span class="badge badge-blue">2주 이내</span>'
    return '<span class="badge badge-green">여유</span>'


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
# 데이터 로딩
# =========================
@st.cache_data(ttl=180)
def load_base_tables() -> Dict[str, pd.DataFrame]:
    client = get_supabase_client()

    action_df = fetch_all_rows(
        client,
        ACTION_TABLE,
        "style_code,sku,plant,lead_time,current_qty_after_rotation,rotation_in_qty,rotation_out_qty,shortage_start_year_week,shortage_qty_after_rotation,center_alloc_qty,reorder_qty,reorder_action_year_week,final_action,priority_rank,reason"
    )

    rotation_df = fetch_all_rows(
        client,
        ROTATION_TABLE,
        "*"
    )

    status_df = fetch_all_rows(
        client,
        STATUS_TABLE,
        "style_code,sku,plant,store_classification,lead_time,current_qty,stock_weeks,shortage_qty,surplus_qty"
    )

    return {
        "action": action_df,
        "rotation": rotation_df,
        "status": status_df,
    }


@st.cache_data(ttl=180)
def load_weekly_by_sty(sty: str) -> pd.DataFrame:
    client = get_supabase_client()

    df = fetch_filtered_rows(
        client,
        WEEKLY_TABLE,
        "style_code,sku,plant,store_name,year_week,sale_qty,is_forecast,begin_stock",
        filters=[("style_code", "eq", sty)]
    )

    if df.empty:
        return df

    for col in ["style_code", "sku", "plant", "store_name", "year_week"]:
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
    return df.sort_values(["sku", "plant", "sort_key"]).reset_index(drop=True)


# =========================
# 요약 계산
# =========================
def prepare_action_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "style_code", "sku", "plant", "shortage_start_year_week",
            "reorder_action_year_week", "final_action", "reason",
            "lead_time", "current_qty_after_rotation",
            "rotation_in_qty", "rotation_out_qty",
            "shortage_qty_after_rotation", "center_alloc_qty",
            "reorder_qty", "priority_rank",
        ])

    out = df.copy()

    for col in [
        "style_code", "sku", "plant", "shortage_start_year_week",
        "reorder_action_year_week", "final_action", "reason"
    ]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].astype(str).str.strip()

    for col in [
        "lead_time", "current_qty_after_rotation",
        "rotation_in_qty", "rotation_out_qty",
        "shortage_qty_after_rotation", "center_alloc_qty",
        "reorder_qty", "priority_rank"
    ]:
        if col not in out.columns:
            out[col] = 0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)

    return out


def prepare_status_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "style_code", "sku", "plant", "store_classification",
            "lead_time", "current_qty", "stock_weeks", "shortage_qty", "surplus_qty",
        ])

    out = df.copy()

    for col in ["style_code", "sku", "plant", "store_classification"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].astype(str).str.strip()

    for col in ["lead_time", "current_qty", "stock_weeks", "shortage_qty", "surplus_qty"]:
        if col not in out.columns:
            out[col] = 0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)

    return out


def prepare_rotation_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "style_code", "sku", "from_plant", "to_plant", "reason",
            "transfer_qty", "priority_rank",
        ])

    out = df.copy()

    for col in ["style_code", "sku", "from_plant", "to_plant", "reason"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].astype(str).str.strip()

    for col in ["transfer_qty", "priority_rank"]:
        if col not in out.columns:
            out[col] = 0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)

    return out


def classify_style_action(style_action_df: pd.DataFrame, style_rotation_df: pd.DataFrame) -> str:
    reorder_needed = pd.to_numeric(style_action_df.get("reorder_qty", 0), errors="coerce").fillna(0).sum() > 0
    rotation_needed = False

    if not style_rotation_df.empty and "transfer_qty" in style_rotation_df.columns:
        rotation_needed = pd.to_numeric(style_rotation_df["transfer_qty"], errors="coerce").fillna(0).sum() > 0

    if reorder_needed and rotation_needed:
        return "리오더+회전필요"
    if reorder_needed:
        return "리오더필요"
    if rotation_needed:
        return "회전필요"
    return "관리 불필요"


def style_urgency(style_action_df: pd.DataFrame) -> str:
    if style_action_df.empty:
        return "여유"

    reorder_candidates = style_action_df[
        pd.to_numeric(style_action_df["reorder_qty"], errors="coerce").fillna(0) > 0
    ].copy()

    if not reorder_candidates.empty:
        labels = reorder_candidates["reorder_action_year_week"].apply(urgency_label_from_week).tolist()
        return sorted(labels, key=urgency_rank)[0]

    shortage_candidates = style_action_df[
        pd.to_numeric(style_action_df["shortage_qty_after_rotation"], errors="coerce").fillna(0) > 0
    ].copy()

    if not shortage_candidates.empty:
        labels = shortage_candidates["shortage_start_year_week"].apply(urgency_label_from_week).tolist()
        return sorted(labels, key=urgency_rank)[0]

    return "여유"


def build_style_board(
    action_df: pd.DataFrame,
    rotation_df: pd.DataFrame,
    status_df: pd.DataFrame
) -> pd.DataFrame:
    def _style_set(d: pd.DataFrame) -> set:
        if d.empty or "style_code" not in d.columns:
            return set()
        return set(d["style_code"].dropna().astype(str).str.strip())

    styles = sorted(_style_set(action_df) | _style_set(status_df) | _style_set(rotation_df))
    rows = []

    for sty in styles:
        if not sty:
            continue

        a = action_df[action_df["style_code"] == sty].copy()
        r = rotation_df[rotation_df["style_code"] == sty].copy() if "style_code" in rotation_df.columns else pd.DataFrame()
        s = status_df[status_df["style_code"] == sty].copy()

        action_type = classify_style_action(a, r)
        urgency = style_urgency(a)

        rows.append({
            "style_code": sty,
            "action_type": action_type,
            "urgency": urgency,
            "sku_cnt": a["sku"].nunique() if not a.empty else s["sku"].nunique(),
            "plant_cnt": a["plant"].nunique() if not a.empty else s["plant"].nunique(),
            "total_reorder_qty": int(pd.to_numeric(a.get("reorder_qty", 0), errors="coerce").fillna(0).sum()),
            "total_center_alloc_qty": int(pd.to_numeric(a.get("center_alloc_qty", 0), errors="coerce").fillna(0).sum()),
            "total_rotation_qty": int(pd.to_numeric(r.get("transfer_qty", 0), errors="coerce").fillna(0).sum()) if not r.empty else 0,
            "shortage_store_cnt": int((s.get("store_classification", "") == "부족매장").sum()) if not s.empty else 0,
            "surplus_store_cnt": int((s.get("store_classification", "") == "여유매장").sum()) if not s.empty else 0,
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out["urgency_rank"] = out["urgency"].apply(urgency_rank)
    out = out.sort_values(
        ["urgency_rank", "total_reorder_qty", "total_rotation_qty", "style_code"],
        ascending=[True, False, False, True]
    ).reset_index(drop=True)
    return out


def build_sku_summary_for_style(
    sty: str,
    action_df: pd.DataFrame,
    rotation_df: pd.DataFrame,
    status_df: pd.DataFrame
) -> pd.DataFrame:
    a = action_df[action_df["style_code"] == sty].copy()
    r = (
        rotation_df[rotation_df["style_code"] == sty].copy()
        if "style_code" in rotation_df.columns
        else pd.DataFrame()
    )
    s = status_df[status_df["style_code"] == sty].copy()

    if a.empty and s.empty:
        return pd.DataFrame()

    a_sum = (
        a.groupby(["style_code", "sku"], as_index=False)
        .agg(
            plant_cnt=("plant", "nunique"),
            total_reorder_qty=("reorder_qty", "sum"),
            total_center_alloc_qty=("center_alloc_qty", "sum"),
            total_shortage_qty=("shortage_qty_after_rotation", "sum"),
            avg_lead_time=("lead_time", "mean"),
            final_action=("final_action", "first"),
            reorder_action_year_week=("reorder_action_year_week", "first"),
        )
    ) if not a.empty else pd.DataFrame(columns=["style_code", "sku"])

    s_sum = (
        s.groupby(["style_code", "sku"], as_index=False)
        .agg(
            shortage_store_cnt=("store_classification", lambda x: (x == "부족매장").sum()),
            surplus_store_cnt=("store_classification", lambda x: (x == "여유매장").sum()),
            keep_store_cnt=("store_classification", lambda x: (x == "유지매장").sum()),
            avg_stock_weeks=("stock_weeks", "mean"),
        )
    ) if not s.empty else pd.DataFrame(columns=["style_code", "sku"])

    r_sum = (
        r.groupby(["style_code", "sku"], as_index=False)
        .agg(rotation_qty=("transfer_qty", "sum"))
    ) if not r.empty else pd.DataFrame(columns=["style_code", "sku"])

    out = a_sum.merge(s_sum, on=["style_code", "sku"], how="outer")
    out = out.merge(r_sum, on=["style_code", "sku"], how="left")
    out["rotation_qty"] = pd.to_numeric(out.get("rotation_qty", 0), errors="coerce").fillna(0)
    out["urgency"] = out.get("reorder_action_year_week", "").apply(urgency_label_from_week)
    out["urgency_rank"] = out["urgency"].apply(urgency_rank)

    return out.sort_values(
        ["urgency_rank", "total_reorder_qty", "rotation_qty"],
        ascending=[True, False, False]
    ).reset_index(drop=True)


def build_related_sku_candidates(sty: str, selected_sku: str, sku_summary_df: pd.DataFrame) -> pd.DataFrame:
    if sku_summary_df.empty:
        return pd.DataFrame()

    df = sku_summary_df[sku_summary_df["sku"] != selected_sku].copy()
    if df.empty:
        return df

    df["bundle_score"] = (
        (df["total_reorder_qty"].fillna(0) > 0).astype(int) * 100
        + (df["urgency"].isin(["긴급", "1주 이내"])).astype(int) * 30
        + (df["rotation_qty"].fillna(0) > 0).astype(int) * 10
    )

    return df.sort_values(
        ["bundle_score", "total_reorder_qty", "rotation_qty"],
        ascending=[False, False, False]
    ).reset_index(drop=True)


def build_store_analysis(
    sty: str,
    sku: str,
    action_df: pd.DataFrame,
    status_df: pd.DataFrame
) -> pd.DataFrame:
    a = action_df[(action_df["style_code"] == sty) & (action_df["sku"] == sku)].copy()
    s = status_df[(status_df["style_code"] == sty) & (status_df["sku"] == sku)].copy()

    if a.empty and s.empty:
        return pd.DataFrame()

    out = s.merge(
        a[[
            "style_code", "sku", "plant",
            "current_qty_after_rotation",
            "rotation_in_qty", "rotation_out_qty",
            "shortage_start_year_week",
            "shortage_qty_after_rotation",
            "center_alloc_qty",
            "reorder_qty",
            "reorder_action_year_week",
            "final_action",
            "reason"
        ]],
        on=["style_code", "sku", "plant"],
        how="outer"
    )

    out["urgency"] = out["reorder_action_year_week"].apply(urgency_label_from_week)
    out["risk"] = out.apply(
        lambda r: "위험" if str(r.get("store_classification", "")) == "부족매장"
        else ("주의" if to_float(r.get("reorder_qty", 0), 0) > 0 else "안정"),
        axis=1
    )

    return out.sort_values(
        ["urgency", "reorder_qty", "shortage_qty_after_rotation"],
        ascending=[True, False, False]
    ).reset_index(drop=True)


# =========================
# 표시 함수
# =========================
def render_metric_card(title: str, value: str):
    st.markdown(
        f"""
        <div class="card">
            <div class="card-title">{title}</div>
            <div class="card-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


# =========================
# 메인
# =========================
def main():
    st.title("재고 운영 통합 대시보드")

    try:
        base = load_base_tables()
    except Exception as e:
        st.error(f"데이터 로딩 실패: {e}")
        st.stop()

    action_df = prepare_action_df(base["action"])
    rotation_df = prepare_rotation_df(base["rotation"])
    status_df = prepare_status_df(base["status"])

    if action_df.empty and status_df.empty:
        st.error("대시보드에 표시할 데이터가 없습니다.")
        st.stop()

    style_board_df = build_style_board(action_df, rotation_df, status_df)

    # =========================
    # 상단 KPI
    # =========================
    style_cnt = len(style_board_df)
    reorder_style_cnt = int((style_board_df["action_type"] == "리오더필요").sum()) if not style_board_df.empty else 0
    rotation_style_cnt = int((style_board_df["action_type"] == "회전필요").sum()) if not style_board_df.empty else 0
    both_style_cnt = int((style_board_df["action_type"] == "리오더+회전필요").sum()) if not style_board_df.empty else 0
    none_style_cnt = int((style_board_df["action_type"] == "관리 불필요").sum()) if not style_board_df.empty else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        render_metric_card("전체 스타일", f"{style_cnt:,}")
    with c2:
        render_metric_card("리오더 필요", f"{reorder_style_cnt:,}")
    with c3:
        render_metric_card("회전 필요", f"{rotation_style_cnt:,}")
    with c4:
        render_metric_card("리오더+회전", f"{both_style_cnt:,}")
    with c5:
        render_metric_card("관리 불필요", f"{none_style_cnt:,}")

    tab1, tab2 = st.tabs(["운영판", "스타일 상세"])

    # =========================
    # 운영판
    # =========================
    with tab1:
        st.markdown('<div class="section-title">스타일 운영 보드</div>', unsafe_allow_html=True)

        f1, f2, f3 = st.columns([1.4, 1.2, 1.2])

        with f1:
            action_filter = st.multiselect(
                "액션 유형",
                ["리오더필요", "회전필요", "리오더+회전필요", "관리 불필요"],
                default=["리오더필요", "회전필요", "리오더+회전필요", "관리 불필요"]
            )
        with f2:
            urgency_filter = st.multiselect(
                "긴급도",
                ["긴급", "1주 이내", "2주 이내", "여유"],
                default=["긴급", "1주 이내", "2주 이내", "여유"]
            )
        with f3:
            keyword = st.text_input("스타일 검색", "")

        board_view = style_board_df.copy()
        if action_filter:
            board_view = board_view[board_view["action_type"].isin(action_filter)]
        if urgency_filter:
            board_view = board_view[board_view["urgency"].isin(urgency_filter)]
        if keyword.strip():
            board_view = board_view[
                board_view["style_code"].astype(str).str.contains(keyword.strip(), case=False, na=False)
            ]

        if not board_view.empty:
            board_show = board_view.copy()
            board_show["긴급도"] = board_show["urgency"].apply(lambda x: x)
            board_show = board_show.rename(columns={
                "style_code": "스타일",
                "action_type": "액션",
                "sku_cnt": "SKU수",
                "plant_cnt": "매장수",
                "total_reorder_qty": "총리오더수량",
                "total_center_alloc_qty": "총센터배분수량",
                "total_rotation_qty": "총회전수량",
                "shortage_store_cnt": "부족매장수",
                "surplus_store_cnt": "여유매장수",
            })
            board_show = board_show[
                ["스타일", "액션", "긴급도", "SKU수", "매장수", "총리오더수량", "총센터배분수량", "총회전수량", "부족매장수", "여유매장수"]
            ]
            st.dataframe(board_show, use_container_width=True, hide_index=True)
        else:
            st.info("조건에 맞는 스타일이 없습니다.")

    # =========================
    # 스타일 상세
    # =========================
    with tab2:
        st.markdown('<div class="section-title">스타일 상세</div>', unsafe_allow_html=True)

        style_options = style_board_df["style_code"].dropna().astype(str).tolist()
        if not style_options:
            st.info("선택 가능한 스타일이 없습니다.")
            return

        selected_sty = st.selectbox("스타일 선택", style_options)

        sty_action_df = action_df[action_df["style_code"] == selected_sty].copy()
        sty_rotation_df = (
            rotation_df[rotation_df["style_code"] == selected_sty].copy()
            if "style_code" in rotation_df.columns
            else pd.DataFrame()
        )
        sty_status_df = status_df[status_df["style_code"] == selected_sty].copy()

        action_type = classify_style_action(sty_action_df, sty_rotation_df)
        urgency = style_urgency(sty_action_df)

        k1, k2, k3, k4, k5 = st.columns(5)
        with k1:
            render_metric_card("스타일", selected_sty)
        with k2:
            render_metric_card("액션", action_type)
        with k3:
            render_metric_card("긴급도", urgency)
        with k4:
            render_metric_card("총 리오더", f"{int(pd.to_numeric(sty_action_df.get('reorder_qty', 0), errors='coerce').fillna(0).sum()):,}")
        with k5:
            render_metric_card("총 회전", f"{int(pd.to_numeric(sty_rotation_df.get('transfer_qty', 0), errors='coerce').fillna(0).sum()) if not sty_rotation_df.empty else 0:,}")

        sku_summary_df = build_sku_summary_for_style(
            selected_sty,
            sty_action_df,
            sty_rotation_df,
            sty_status_df
        )

        st.markdown("### SKU별 운영 현황")
        if not sku_summary_df.empty:
            sku_show = sku_summary_df.rename(columns={
                "sku": "SKU",
                "plant_cnt": "매장수",
                "total_reorder_qty": "총리오더수량",
                "total_center_alloc_qty": "총센터배분수량",
                "total_shortage_qty": "총부족수량",
                "avg_lead_time": "리드타임",
                "final_action": "대표액션",
                "reorder_action_year_week": "리오더시점",
                "shortage_store_cnt": "부족매장수",
                "surplus_store_cnt": "여유매장수",
                "keep_store_cnt": "유지매장수",
                "avg_stock_weeks": "평균재고주수",
                "rotation_qty": "회전수량",
                "urgency": "긴급도",
            })
            show_cols = [
                "SKU", "긴급도", "대표액션", "리오더시점",
                "총리오더수량", "총센터배분수량", "회전수량",
                "부족매장수", "여유매장수", "유지매장수",
                "평균재고주수", "리드타임"
            ]
            show_cols = [c for c in show_cols if c in sku_show.columns]
            st.dataframe(sku_show[show_cols], use_container_width=True, hide_index=True)
        else:
            st.info("이 스타일의 SKU 요약이 없습니다.")

        # 상세 SKU 선택
        sku_options = sku_summary_df["sku"].dropna().astype(str).tolist() if not sku_summary_df.empty else []
        if sku_options:
            selected_sku = st.selectbox("상세 SKU 선택", sku_options)

            # 주차별 데이터
            weekly_sty_df = load_weekly_by_sty(selected_sty)
            weekly_sku_df = weekly_sty_df[weekly_sty_df["sku"] == selected_sku].copy()

            st.markdown("### 판매/예측 흐름")
            if not weekly_sku_df.empty:
                plant_option = st.selectbox(
                    "차트 매장 선택",
                    ["전체"] + sorted(weekly_sku_df["plant"].dropna().astype(str).unique().tolist()),
                    key="sku_chart_plant"
                )

                if plant_option == "전체":
                    chart_df = (
                        weekly_sku_df.groupby(["year_week", "is_forecast", "sort_key"], as_index=False)
                        .agg(
                            sale_qty=("sale_qty", "sum"),
                            begin_stock=("begin_stock", "sum")
                        )
                        .sort_values("sort_key")
                    )
                else:
                    chart_df = weekly_sku_df[weekly_sku_df["plant"] == plant_option].copy().sort_values("sort_key")

                chart_df = chart_df[["year_week", "sale_qty"]].copy().set_index("year_week")
                st.line_chart(chart_df)

                detail_view = weekly_sku_df.sort_values(["plant", "sort_key"])[
                    ["plant", "store_name", "year_week", "is_forecast", "sale_qty", "begin_stock"]
                ]
                st.dataframe(detail_view, use_container_width=True, hide_index=True)
            else:
                st.info("선택한 SKU의 주차별 데이터가 없습니다.")

            # 같은 스타일 추천 SKU
            st.markdown("### 함께 발주/관리 추천 SKU")
            related_df = build_related_sku_candidates(selected_sty, selected_sku, sku_summary_df)
            if not related_df.empty:
                related_show = related_df.rename(columns={
                    "sku": "SKU",
                    "final_action": "대표액션",
                    "total_reorder_qty": "총리오더수량",
                    "total_center_alloc_qty": "총센터배분수량",
                    "reorder_action_year_week": "리오더시점",
                    "urgency": "긴급도",
                    "rotation_qty": "회전수량",
                    "bundle_score": "추천점수",
                })
                cols = ["SKU", "긴급도", "대표액션", "리오더시점", "총리오더수량", "총센터배분수량", "회전수량", "추천점수"]
                cols = [c for c in cols if c in related_show.columns]
                st.dataframe(related_show[cols], use_container_width=True, hide_index=True)
            else:
                st.info("같이 볼 다른 SKU가 없습니다.")

            # 매장별 분석
            st.markdown("### 매장별 분석")
            store_df = build_store_analysis(selected_sty, selected_sku, action_df, status_df)
            if not store_df.empty:
                store_show = store_df.rename(columns={
                    "plant": "매장",
                    "store_classification": "매장분류",
                    "lead_time": "리드타임",
                    "current_qty": "현재재고(step1)",
                    "stock_weeks": "재고주수",
                    "shortage_qty": "부족수량(step1)",
                    "surplus_qty": "여유수량(step1)",
                    "current_qty_after_rotation": "회전후재고",
                    "rotation_in_qty": "회전유입",
                    "rotation_out_qty": "회전유출",
                    "shortage_start_year_week": "부족시작주차",
                    "shortage_qty_after_rotation": "회전후부족수량",
                    "center_alloc_qty": "센터배분수량",
                    "reorder_qty": "리오더수량",
                    "reorder_action_year_week": "리오더시점",
                    "final_action": "최종액션",
                    "reason": "사유",
                    "urgency": "긴급도",
                    "risk": "리스크",
                })

                store_cols = [
                    "매장", "매장분류", "리스크", "긴급도",
                    "현재재고(step1)", "재고주수", "부족수량(step1)", "여유수량(step1)",
                    "회전유입", "회전유출", "회전후재고",
                    "부족시작주차", "회전후부족수량",
                    "센터배분수량", "리오더수량", "리오더시점",
                    "최종액션", "사유"
                ]
                store_cols = [c for c in store_cols if c in store_show.columns]
                st.dataframe(store_show[store_cols], use_container_width=True, hide_index=True)
            else:
                st.info("매장별 분석 데이터가 없습니다.")


if __name__ == "__main__":
    main()
