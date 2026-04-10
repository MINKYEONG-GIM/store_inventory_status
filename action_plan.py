import os
import traceback

import psycopg2
import streamlit as st


# -----------------------------
# DB 연결
# -----------------------------
def get_conn():
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT", "5432"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        dbname=os.getenv("PGDATABASE"),
        sslmode=os.getenv("PGSSLMODE", "require"),
    )


# -----------------------------
# step2 적재 SQL
# -----------------------------
LOAD_SQL = """
TRUNCATE TABLE public.store_inventory_status_step2;

INSERT INTO public.store_inventory_status_step2 (
    style_code,
    sku,
    total_shortage_qty,
    shortage_store_count,
    lead_time,
    reorder_needed,
    reorder_urgency,
    order_due_date
)
WITH step1_agg AS (
    SELECT
        sku,
        MAX(style_code) AS style_code,
        COALESCE(SUM(COALESCE(shortage_qty, 0)), 0) AS sum_shortage_qty,
        COALESCE(SUM(COALESCE(surplus_qty, 0)), 0) AS sum_surplus_qty,
        COUNT(*) FILTER (WHERE COALESCE(shortage_qty, 0) > 0) AS shortage_store_count,
        COALESCE(MAX(lead_time), 0) AS max_lead_time
    FROM public.store_inventory_status_step1
    WHERE sku IS NOT NULL
    GROUP BY sku
),
center_agg AS (
    SELECT
        sku,
        COALESCE(SUM(COALESCE(stock_qty, 0)), 0) AS center_stock_qty
    FROM public.center_stock
    WHERE sku IS NOT NULL
    GROUP BY sku
),
final_calc AS (
    SELECT
        s.style_code,
        s.sku,

        -- 매장 부족합 - 매장 여유합 - 센터재고합
        ROUND(
            s.sum_shortage_qty
            - s.sum_surplus_qty
            - COALESCE(c.center_stock_qty, 0)
        )::integer AS total_shortage_qty,

        s.shortage_store_count,
        s.max_lead_time AS lead_time,

        (
            s.sum_shortage_qty
            - s.sum_surplus_qty
            - COALESCE(c.center_stock_qty, 0)
        ) > 0 AS reorder_needed,

        CASE
            -- 애초에 매장끼리 상쇄 후 부족이 없으면 발주 불필요
            WHEN (s.sum_shortage_qty - s.sum_surplus_qty) <= 0 THEN '불필요'

            -- 매장끼리 상쇄 후 부족은 있지만 센터재고로 커버 가능
            WHEN (
                s.sum_shortage_qty
                - s.sum_surplus_qty
                - COALESCE(c.center_stock_qty, 0)
            ) <= 0 THEN '센터출고'

            -- 발주 필요
            WHEN s.max_lead_time <= 7 THEN '긴급'
            WHEN s.max_lead_time <= 14 THEN '주의'
            ELSE '일반'
        END AS reorder_urgency,

        CASE
            WHEN (
                s.sum_shortage_qty
                - s.sum_surplus_qty
                - COALESCE(c.center_stock_qty, 0)
            ) > 0
            THEN (CURRENT_DATE + CEIL(s.max_lead_time)::integer)
            ELSE NULL
        END AS order_due_date
    FROM step1_agg s
    LEFT JOIN center_agg c
        ON s.sku = c.sku
)
SELECT
    style_code,
    sku,
    total_shortage_qty,
    shortage_store_count,
    lead_time,
    reorder_needed,
    reorder_urgency,
    order_due_date
FROM final_calc
ORDER BY sku;
"""


# -----------------------------
# 실행 함수
# -----------------------------
def load_step2():
    conn = None
    cur = None

    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        cur.execute(LOAD_SQL)

        conn.commit()

    except Exception:
        if conn:
            conn.rollback()
        raise

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# -----------------------------
# Streamlit UI
# -----------------------------
def main():
    st.set_page_config(page_title="step2 loader", layout="centered")

    # 화면 요소 최대한 숨김
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

    # 버튼 하나만 노출
    if st.button("데이터 쌓기", use_container_width=True):
        try:
            with st.spinner("적재 중..."):
                load_step2()
            st.success("완료")
        except Exception as e:
            st.error("실패")
            st.code(traceback.format_exc())


if __name__ == "__main__":
    main()
