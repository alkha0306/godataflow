# frontend/pages/data_management.py
import datetime
import streamlit as st
from utils.api import api_get, api_post, api_delete
from config import API_BASE
import pandas as pd
import json

SQL_TYPES = [
    "INT",
    "BIGINT",
    "SMALLINT",
    "SERIAL",
    "BIGSERIAL",
    "FLOAT",
    "DOUBLE PRECISION",
    "NUMERIC",
    "BOOLEAN",
    "TEXT",
    "VARCHAR(50)",
    "VARCHAR(100)",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "DATE",
    "TIME",
    "JSON",
    "JSONB"
]


def data_management_page():
    st.title("📁 Data Management")

    tabs = st.tabs(["List Tables", "Create Table", "View Data", "Insert Data", "Delete Table"])

    # --- LIST TABLES ---
    with tabs[0]:
        tables = api_get(f"{API_BASE}/tables") or []
        if tables:
            st.dataframe(pd.DataFrame(tables))
        else:
            st.info("No tables found.")

    # --- CREATE TABLE ---
    with tabs[1]:
        st.subheader("Create Table")

        table_name = st.text_input("Table Name", key="data_mgmt_table_name_create_table")
        table_type = st.selectbox("Table Type", ["normal", "time_series"], key="data_mgmt_table_type_create_table")
        interval = st.number_input("Refresh Interval", min_value=0, step=5, key="data_mgmt_refresh_interval_create_table")

        num_cols = st.number_input("Columns", min_value=1, max_value=10, value=2, key="data_mgmt_col_no_create_table")

        cols = {}

        for i in range(num_cols):
            c1, c2 = st.columns(2)

            name = c1.text_input(f"Column {i+1} Name", key=f"col_name_{i}")

            sql_type = c2.selectbox(
                f"Column {i+1} SQL Type",
                SQL_TYPES,
                key=f"col_type_{i}"
            )

            if name:
                cols[name] = sql_type

        if st.button("Create", key="data_mgmt_create_button_create_table"):
            payload = {
                "table_name": table_name,
                "table_type": table_type,
                "refresh_interval": interval or None,
                "columns": cols
            }
            res = api_post(f"{API_BASE}/tables", json=payload)
            if res:
                st.success("Created!")
            else:
                st.error("Failed.")

    # --- VIEW DATA ---
    with tabs[2]:
        tables = api_get(f"{API_BASE}/tables") or []
        names = [t["table_name"] for t in tables]
        name = st.selectbox("Select", names,key = "data_mgmt_select_table_name_view_table")
        if name and st.button("Load",key = "data_mgmt_load_btn_view_table"):
            data = api_get(f"{API_BASE}/query", params={"table": name})
            rows = data.get("data", []) if data else []
            st.dataframe(pd.DataFrame(rows))

    # --- INSERT DATA ---
    with tabs[3]:
        tables = api_get(f"{API_BASE}/tables") or []
        names = [t["table_name"] for t in tables]

        name = st.selectbox("Insert into", names, key="data_mgmt_insert_into_insert_data")

        # Find column metadata for selected table
        col_res = api_get(f"{API_BASE}/tables/{name}/columns") or {}

        st.write("### Insert Options")

        mode = st.radio(
            "Choose Input Mode",
            ["Fill Column Values", "Raw JSON"],
            key="data_mgmt_insert_mode"
        )

        # ------------------------------
        # MODE 1: FORM-BASED INSERT
        # ------------------------------
        if mode == "Fill Column Values":
            st.write("### Column Values")

            form_data = {}

            for col_info in col_res:
                col = col_info["column_name"]
                typ = col_info["data_type"].lower()

                # correct input widget based on type
                if "int" in typ or "numeric" in typ or "double" in typ or "float" in typ:
                    form_data[col] = st.number_input(col, key=f"val_{col}")

                elif "bool" in typ:
                    form_data[col] = st.checkbox(col, key=f"val_{col}")

                elif "date" in typ and "time" not in typ:
                    form_data[col] = st.date_input(col, key=f"val_{col}")

                elif "timestamp" in typ:
                    d = st.date_input(col + " (date)", key=f"val_date_{col}")
                    t = st.time_input(col + " (time)", key=f"val_time_{col}", value=datetime.time(0, 0))

                    combined = datetime.datetime.combine(d, t).isoformat()
                    form_data[col] = combined

                elif "json" in typ:
                    form_data[col] = st.text_area(col + " (JSON)", key=f"val_{col}")

                else:
                    form_data[col] = st.text_input(col, key=f"val_{col}")

            if st.button("Insert Row", key="insert_row_btn"):
                res = api_post(f"{API_BASE}/ingest/{name}", json=form_data)
                if res:
                    st.success("Inserted!")
                else:
                    st.error("Failed.")

        # ------------------------------
        # MODE 2: RAW JSON
        # ------------------------------
        if mode == "Raw JSON":
            raw = st.text_area("JSON Row(s)", key="data_mgmt_json_rows_insert_data_raw")

            if st.button("Insert JSON", key="insert_json_btn"):
                try:
                    payload = json.loads(raw)
                    res = api_post(f"{API_BASE}/ingest/{name}", json=payload)
                    if res:
                        st.success("Inserted!")
                    else:
                        st.error("Failed.")
                except:
                    st.error("Invalid JSON")


    # --- DELETE TABLE ---
    with tabs[4]:
        tables = api_get(f"{API_BASE}/tables") or []
        names = [t["table_name"] for t in tables]
        name = st.selectbox("Delete table", names,key = "data_mgmt_delete_box_delete_data")
        if st.button("Delete",key = "data_mgmt_delete_btn_delete_data"):
            res = api_delete(f"{API_BASE}/tables/{name}")
            st.success("Deleted!" if res else "Failed")
