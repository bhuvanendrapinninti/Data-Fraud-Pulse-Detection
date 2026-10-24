import os, time
import pandas as pd
import psycopg
import streamlit as st

st.set_page_config(page_title="DataPulse Fraud Monitor", page_icon="🛡️", layout="wide")
st.title("🛡️ DataPulse — Real-Time Fraud Pipeline")
st.caption("Synthetic transactions for educational purposes only.")

def connect():
    return psycopg.connect(host=os.getenv("DB_HOST","localhost"),
        port=int(os.getenv("DB_PORT","5432")), dbname=os.getenv("DB_NAME","frauddb"),
        user=os.getenv("DB_USER","frauduser"), password=os.getenv("DB_PASSWORD","fraudpass"),
        connect_timeout=5)

refresh = st.sidebar.checkbox("Auto-refresh every 10 seconds", True)
limit = st.sidebar.slider("Recent transactions",10,500,100,10)
try:
    with connect() as conn:
        metrics = pd.read_sql("""SELECT COUNT(*) total,
          COUNT(*) FILTER (WHERE is_suspicious) suspicious,
          COALESCE(SUM(amount),0) total_amount,
          COUNT(DISTINCT customer_id) customers FROM transactions""",conn).iloc[0]
        tx = pd.read_sql("""SELECT transaction_id,customer_id,amount,currency,merchant_id,
          event_time,is_suspicious,risk_reasons FROM transactions
          ORDER BY event_time DESC LIMIT %s""",conn,params=(limit,))
        alerts = pd.read_sql("""SELECT transaction_id,customer_id,amount,event_time,reasons,status
          FROM fraud_alerts ORDER BY created_at DESC LIMIT 100""",conn)
    cols=st.columns(4)
    cols[0].metric("Transactions",f"{int(metrics.total):,}")
    cols[1].metric("Suspicious flags",f"{int(metrics.suspicious):,}")
    cols[2].metric("Flag rate",f"{(100*metrics.suspicious/metrics.total if metrics.total else 0):.2f}%")
    cols[3].metric("Customers",f"{int(metrics.customers):,}")
    st.subheader("Recent transaction volume")
    if not tx.empty:
        tx["event_time"]=pd.to_datetime(tx["event_time"],utc=True)
        st.line_chart(tx.set_index("event_time").resample("1min").size().rename("transactions"))
    st.subheader("Transactions")
    st.dataframe(tx,use_container_width=True,hide_index=True)
    st.subheader("Fraud alerts")
    st.dataframe(alerts,use_container_width=True,hide_index=True)
except Exception as exc:
    st.error(f"PostgreSQL is not ready or query failed: {exc}")
if refresh:
    time.sleep(10)
    st.rerun()
