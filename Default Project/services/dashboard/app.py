"""Real-time dashboard backed directly by ClickHouse."""
import os
import time

import clickhouse_connect
import pandas as pd
import streamlit as st

CH = dict(host=os.getenv("CH_HOST", "clickhouse"), port=int(os.getenv("CH_PORT", "8123")),
          database=os.getenv("CH_DATABASE", "shop"),
          username=os.getenv("CH_USER", "default"), password=os.getenv("CH_PASSWORD", ""))

st.set_page_config(page_title="Realtime Shop Analytics (ClickHouse)", layout="wide")
st.title("Real-Time Analytics — Postgres CDC -> ClickHouse")
st.caption("Live from ClickHouse (ReplacingMergeTree FINAL, is_deleted=0). "
           "Push transactions to Postgres and watch this update.")

REFRESH = st.sidebar.slider("Refresh (sec)", 2, 30, 5)


@st.cache_resource
def client():
    for _ in range(60):
        try:
            cl = clickhouse_connect.get_client(**CH)
            cl.query("SELECT 1")
            return cl
        except Exception:
            time.sleep(2)
    return clickhouse_connect.get_client(**CH)


def q(sql):
    try:
        res = client().query(sql)
        return pd.DataFrame(res.result_rows, columns=res.column_names)
    except Exception as e:
        st.error(f"ClickHouse query failed: {e}")
        return pd.DataFrame()


kpi = q("""SELECT COUNT() AS orders, SUM(total) AS revenue,
                  COUNTIf(status='cancelled') AS cancelled
           FROM shop.orders_ch FINAL WHERE is_deleted = 0""")
c1, c2, c3 = st.columns(3)
if not kpi.empty:
    c1.metric("Orders (CH)", f"{int(kpi.orders[0]):,}")
    c2.metric("Revenue (CH)", f"${float(kpi.revenue[0] or 0):,.2f}")
    c3.metric("Cancelled", f"{int(kpi.cancelled[0]):,}")

col1, col2 = st.columns(2)
with col1:
    st.subheader("Revenue by category (live)")
    df = q("""SELECT p.category, SUM(oi.qty * oi.unit_price) AS revenue
              FROM shop.orders_ch o FINAL
              JOIN shop.order_items_ch oi FINAL ON oi.order_id = o.order_id
              JOIN shop.products_ch p FINAL ON p.product_id = oi.product_id
              WHERE o.is_deleted=0 AND oi.is_deleted=0 AND p.is_deleted=0
              GROUP BY 1 ORDER BY revenue DESC""")
    if not df.empty:
        st.bar_chart(df.set_index("category"))

with col2:
    st.subheader("Orders per minute (live)")
    df = q("""SELECT toStartOfMinute(created_at) AS m, COUNT() AS n
              FROM shop.orders_ch FINAL
              WHERE is_deleted=0 AND created_at > now() - INTERVAL 1 HOUR
              GROUP BY 1 ORDER BY 1""")
    if not df.empty:
        st.line_chart(df.set_index("m"))

st.subheader("Latest orders (proves CDC latency)")
st.dataframe(q("""SELECT order_id, customer_id, status, total, created_at
                  FROM shop.orders_ch FINAL WHERE is_deleted=0
                  ORDER BY created_at DESC LIMIT 15"""))

st.caption("CDC path: PG triggers -> cdc_outbox -> cdc-worker -> "
           "ClickHouse ReplacingMergeTree. Latency is typically < 1-2 s.")
time.sleep(REFRESH)
st.rerun()
