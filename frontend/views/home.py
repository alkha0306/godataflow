# frontend/pages/home.py
import streamlit as st
from utils.api import api_get
from config import API_BASE
import pandas as pd

def home_page():
    st.title("🏠 GoDataFlow Overview")

    tables = api_get(f"{API_BASE}/tables") or []
    total = len(tables)
    time_series = len([t for t in tables if t["table_type"] == "time_series"])

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Tables", total)
    col2.metric("Time-Series Tables", time_series)
    col3.metric("Auto-Refresh Enabled", len([t for t in tables if t.get("refresh_interval") is not None]))

    st.subheader("Recent Refresh Logs")
    recent = api_get(f"{API_BASE}/refresh_logs/weather_data")  # example
    if recent:
        df = pd.DataFrame(recent[:5])
        st.dataframe(df)
