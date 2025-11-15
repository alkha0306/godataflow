# frontend/pages/live_dashboard.py
import streamlit as st
import time
import pandas as pd
from utils.api import api_get
from config import API_BASE

def live_dashboard_page():
    st.title("📈 Live Dashboard")

    tables = api_get(f"{API_BASE}/tables") or []
    ts_table_names = [t["table_name"] for t in tables if t["table_type"] == "time_series"]

    table = st.selectbox("Select Table", ts_table_names, key = "live_dashboard_select_table")
    refresh = st.slider("Refresh every (sec)", 5, 60, 10,key = "live_dashboard_refresh_slider")

    placeholder_chart = st.empty()
    placeholder_data = st.empty()

    while table:
        data = api_get(f"{API_BASE}/query", params={"table": table})
        rows = data.get("data", []) if data else []
        df = pd.DataFrame(rows)

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.sort_values("timestamp")

        if "value" in df.columns and "timestamp" in df.columns:
            placeholder_chart.line_chart(df.set_index("timestamp")["value"])
        placeholder_data.dataframe(df.tail(5))

        time.sleep(refresh)
        st.rerun()
