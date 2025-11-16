# frontend/pages/etl_automation.py
import streamlit as st
from utils.api import api_get, api_post, api_put
from config import API_BASE
import json
import pandas as pd

def etl_automation_page():
    st.title("⚙️ ETL & Automation")

    tabs = st.tabs(["ETL Mapping Wizard", "Auto Refresh Config", "Manual Refresh", "Refresh History"])

    # --- MAPPING WIZARD ---
    with tabs[0]:
        st.subheader("ETL Mapping Wizard")

        # Step 1: Source URL
        url = st.text_input("Source API URL", key="etl_src_url_map_mapping_wiz")

        # Optional preview
        if st.button("Preview JSON", key="etl_preview_button_map_mapping_wiz"):
            preview = api_get(f"{API_BASE}/preview_source", params={"url": url})
            st.json(preview)

        # Step 2: Select time-series table
        tables = api_get(f"{API_BASE}/tables") or []
        ts_tables = [t["table_name"] for t in tables if t["table_type"] == "time_series"]

        table = st.selectbox("Map to Table", ts_tables, key="etl_table_select_mapping_wiz")

        # Step 3: Get table schema columns
        col_res = api_get(f"{API_BASE}/tables/{table}/columns") or []

        # Step 4: Simple manual mapping:
        # USER enters source JSON key for each COLUMN
        st.write("### Map Table Columns to API Keys")

        mapping = {}

        for col_info in col_res:
            col_name = col_info["column_name"]

            api_key = st.text_input(
                f"API Key for column '{col_name}'",
                key=f"map_{col_name}",
                placeholder="Enter key from API JSON"
            )

            if api_key.strip():
                mapping[api_key] = col_name   # api_key → db column

        st.write("### Generated Mapping JSON")
        st.json(mapping)

        # Save button
        if st.button("Save Mapping", key="etl_save_mapping_wiz"):
            res = api_put(
                f"{API_BASE}/tables/{table}/config",
                json={"mapping_json": mapping}
            )
            st.success("Saved!" if res else "Failed")



    # --- AUTO REFRESH CONFIG ---
    with tabs[1]:
        st.subheader("Auto Refresh Configuration")

        tables = api_get(f"{API_BASE}/tables") or []
        ts = [t["table_name"] for t in tables if t["table_type"] == "time_series"]
        selected = st.selectbox("Select Table", ts, key = "etl_table_select_auto_refresh_config")

        if selected:
            meta = next(t for t in tables if t["table_name"] == selected)

            enable = st.checkbox("Enable Auto Refresh", value=meta.get("refresh_interval") is not None, key = "etl_enable_auto_refresh_config")
            interval = st.number_input("Interval (sec)", min_value=1, value=meta.get("refresh_interval") or 30,key = "etl_interval_auto_refresh_config")
            url = st.text_input("Data Source URL", meta.get("data_source_url") or "", key = "etl_data_source_auto_refresh_config")

            if st.button("Save", key = "etl_save_button_auto_refresh"):
                payload = {
                    "refresh_interval": interval if enable else None,
                    "data_source_url": url or None
                }
                res = api_put(f"{API_BASE}/tables/{selected}/config", json=payload)
                st.success("Saved!" if res else "Failed")

    # --- MANUAL REFRESH ---
    with tabs[2]:
        tables = api_get(f"{API_BASE}/tables") or []
        ts = [t["table_name"] for t in tables if t["table_type"] == "time_series"]
        table = st.selectbox("Select Table", ts, key = "etl_select_table_manual_refresh")
        if st.button("Refresh Now", key = "etl_refresh_button"):
            res = api_post(f"{API_BASE}/refresh/{table}")
            st.success("Triggered!" if res else "Failed")

    # --- REFRESH HISTORY ---
    with tabs[3]:
        tables = api_get(f"{API_BASE}/tables") or []
        names = [t["table_name"] for t in tables]
        table = st.selectbox("Select Table", names, key = "etl_select_table_refresh_hist")

        logs = api_get(f"{API_BASE}/refresh_logs/{table}") or []
        st.dataframe(pd.DataFrame(logs))
