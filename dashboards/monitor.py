"""
dashboards/monitor.py
Streamlit live monitoring dashboard for the streaming pipeline.
Auto-refreshes every 5 seconds to show live metrics.

Run: streamlit run dashboards/monitor.py
"""

import streamlit as st
import pandas as pd
import os
import sqlite3
import time
from pathlib import Path
from datetime import datetime, timezone

st.set_page_config(
    page_title="Streaming Pipeline Monitor",
    page_icon="⚡",
    layout="wide"
)

BRONZE_PATH = "./data/bronze"
SILVER_PATH = "./data/silver"
DLQ_PATH = "./data/dlq"
SCHEMA_DB = "./data/schema_registry.db"

st.title("⚡ Real-Time Streaming Pipeline Monitor")
st.caption("Kafka → PySpark Structured Streaming → Delta Lake")

# Auto-refresh
refresh_interval = st.sidebar.slider("Auto-refresh (seconds)", 3, 30, 5)
st.sidebar.info(f"Dashboard refreshes every {refresh_interval}s")

placeholder = st.empty()

def count_parquet_rows(path: str) -> int:
    """Count rows across all Parquet files in a Delta table path."""
    try:
        total = 0
        for root, _, files in os.walk(path):
            for f in files:
                if f.endswith(".parquet"):
                    df = pd.read_parquet(os.path.join(root, f))
                    total += len(df)
        return total
    except Exception:
        return 0


def read_latest_parquet(path: str, n: int = 500) -> pd.DataFrame:
    """Read the most recently written Parquet file."""
    try:
        files = []
        for root, _, fs in os.walk(path):
            for f in fs:
                if f.endswith(".parquet"):
                    fp = os.path.join(root, f)
                    files.append((os.path.getmtime(fp), fp))
        if not files:
            return pd.DataFrame()
        files.sort(reverse=True)
        dfs = []
        for _, fp in files[:5]:
            dfs.append(pd.read_parquet(fp))
        df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        return df.tail(n)
    except Exception as e:
        return pd.DataFrame()


def get_schema_events(limit=20) -> pd.DataFrame:
    if not os.path.exists(SCHEMA_DB):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(SCHEMA_DB)
        df = pd.read_sql(
            f"SELECT detected_at, change_type, field_name, event_id FROM schema_evolution_log ORDER BY id DESC LIMIT {limit}",
            conn
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


while True:
    with placeholder.container():

        # ── Top KPI Row ────────────────────────────────────────────────────────
        bronze_count = count_parquet_rows(BRONZE_PATH)
        silver_count = count_parquet_rows(SILVER_PATH)
        dlq_count = count_parquet_rows(DLQ_PATH)
        error_rate = round((dlq_count / bronze_count * 100), 1) if bronze_count > 0 else 0.0

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("🥉 Bronze Events", f"{bronze_count:,}", help="Raw events ingested")
        col2.metric("🥈 Silver Events", f"{silver_count:,}", help="Validated events")
        col3.metric("💀 DLQ Events", f"{dlq_count:,}", help="Invalid / rejected events")
        col4.metric("❌ Error Rate", f"{error_rate}%", delta=f"{error_rate}%", delta_color="inverse")

        st.divider()

        # ── Silver Data Preview ────────────────────────────────────────────────
        col_l, col_r = st.columns([2, 1])

        with col_l:
            st.subheader("📊 Silver Layer — Recent Events")
            silver_df = read_latest_parquet(SILVER_PATH)
            if not silver_df.empty:
                display_cols = [c for c in ["symbol", "price", "volume", "spread", "mid_price", "exchange", "analyst_rating", "_processed_at"] if c in silver_df.columns]
                st.dataframe(silver_df[display_cols].tail(20), use_container_width=True)

                # Price by symbol chart
                if "symbol" in silver_df.columns and "price" in silver_df.columns:
                    latest_by_symbol = silver_df.groupby("symbol")["price"].last().reset_index()
                    st.bar_chart(latest_by_symbol.set_index("symbol")["price"])
            else:
                st.info("Waiting for data... Start the Spark consumer and producer.")

        with col_r:
            st.subheader("🔴 Dead Letter Queue")
            dlq_df = read_latest_parquet(DLQ_PATH)
            if not dlq_df.empty:
                display_cols = [c for c in ["symbol", "price", "_reason", "_rejected_at"] if c in dlq_df.columns]
                st.dataframe(dlq_df[display_cols].tail(10), use_container_width=True)
            else:
                st.success("No invalid events detected ✅")

            st.divider()

            st.subheader("🔄 Schema Evolution Log")
            schema_df = get_schema_events()
            if not schema_df.empty:
                for _, row in schema_df.iterrows():
                    color = "🟡" if row["change_type"] == "NEW_FIELD" else "🔴"
                    st.caption(f"{color} `{row['change_type']}` — field: **{row['field_name']}** @ {row['detected_at'][:19]}")
            else:
                st.caption("No schema changes detected.")

        # ── Volume by Exchange ─────────────────────────────────────────────────
        st.subheader("📈 Volume Distribution by Exchange")
        if not silver_df.empty and "exchange" in silver_df.columns and "volume" in silver_df.columns:
            vol_by_exchange = silver_df.groupby("exchange")["volume"].sum().reset_index()
            st.bar_chart(vol_by_exchange.set_index("exchange")["volume"])

        st.caption(f"Last refreshed: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    time.sleep(refresh_interval)
    st.rerun()
