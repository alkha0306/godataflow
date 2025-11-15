import streamlit as st
import requests
import pandas as pd
import time
import json

# --- CONFIG ---
API_BASE = "http://localhost:8080"  # your Go server

SQL_TYPES = [
    "SERIAL PRIMARY KEY",
    "INTEGER",
    "BIGINT",
    "FLOAT",
    "NUMERIC",
    "BOOLEAN",
    "TEXT",
    "VARCHAR(255)",
    "DATE",
    "TIMESTAMP",
    "UUID"
]

st.set_page_config(page_title="GoDataFlow Dashboard", layout="wide")

st.title("📊 GoDataFlow Dashboard")
st.sidebar.title("Navigation")

menu = st.sidebar.radio("Select a view:", ["List Tables", "Create Table", "Data Ingestion", "View Data", "Delete Table", "Manual Refresh", "Refresh History", "Data Source Config"])

# --- 1. LIST TABLES ---
if menu == "List Tables":
    st.subheader("Existing Tables")

    try:
        resp = requests.get(f"{API_BASE}/tables")
        if resp.status_code == 200:
            tables = resp.json()
            if tables:
                df = pd.DataFrame(tables)
                st.dataframe(df)
            else:
                st.info("No tables found.")
        else:
            st.error(f"Error: {resp.status_code}")
    except Exception as e:
        st.error(f"Failed to fetch tables: {e}")

# --- 2. CREATE TABLE ---
elif menu == "Create Table":
    st.subheader("Create New Table")

    table_name = st.text_input("Table Name", placeholder="e.g. temperature_data")
    table_type = st.selectbox("Table Type", ["normal", "time_series"])
    refresh_interval = st.number_input("Refresh Interval (seconds)", min_value=0, step=10)

    st.markdown("### Define Columns")
    st.caption("Enter column name and SQL type (e.g. `id` → `SERIAL PRIMARY KEY`, `value` → `FLOAT`, `timestamp` → `TIMESTAMP`).")

    # Let the user add multiple columns dynamically
    col_count = st.number_input("Number of columns", min_value=1, max_value=10, value=2)
    columns = {}

    for i in range(col_count):
        col1, col2 = st.columns(2)
        with col1:
            col_name = st.text_input(f"Column {i+1} Name", key=f"name_{i}")
        with col2:
            col_type = st.selectbox(f"Column {i+1} Type", SQL_TYPES, key=f"type_{i}")
        if col_name and col_type:
            columns[col_name] = col_type

    if st.button("Create Table"):
        if not table_name or not columns:
            st.warning("Please provide a table name and at least one column.")
        else:
            payload = {
                "table_name": table_name,
                "table_type": table_type,
                "refresh_interval": refresh_interval if refresh_interval > 0 else None,
                "columns": columns
            }

            try:
                resp = requests.post(f"{API_BASE}/tables", json=payload)
                if resp.status_code == 201:
                    st.success(f"Table '{table_name}' created successfully!")
                else:
                    st.error(f"Failed: {resp.text}")
            except Exception as e:
                st.error(f"Error sending request: {e}")

# Data Ingestion 
elif menu == "Data Ingestion":
    st.title("📥 Data Ingestion")

    # Fetch table list
    try:
        tables = requests.get(f"{API_BASE}/tables").json()
        table_names = [t["table_name"] for t in tables]
    except Exception as e:
        st.error(f"Failed to load tables: {e}")
        table_names = []

    if not table_names:
        st.warning("No tables found. Please create one first.")
    else:
        table_name = st.selectbox("Select Table", table_names)
        mode = st.radio("Input Mode", ["Form Input", "Raw JSON"])

        if mode == "Form Input":
            # Fetch columns dynamically
            try:
                cols = requests.get(f"{API_BASE}/tables/{table_name}/columns").json()
            except Exception as e:
                st.error(f"Failed to fetch columns: {e}")
                cols = []

            if cols:
                st.subheader(f"Insert data into `{table_name}`")
                record = {}
                for col in cols:
                    col_name = col["column_name"]
                    col_type = col["data_type"].lower()

                    # Input widgets based on SQL type
                    if "int" in col_type:
                        record[col_name] = st.number_input(f"{col_name} (int)", step=1)
                    elif "float" in col_type or "double" in col_type or "numeric" in col_type:
                        record[col_name] = st.number_input(f"{col_name} (float)", step=0.1, format="%.4f")
                    elif "bool" in col_type:
                        record[col_name] = st.checkbox(f"{col_name}")
                    elif "timestamp" in col_type or "date" in col_type:
                        record[col_name] = st.date_input(f"{col_name}").isoformat()
                    else:
                        record[col_name] = st.text_input(f"{col_name} (string)")

                if st.button("Insert Row"):
                    try:
                        res = requests.post(f"{API_BASE}/ingest/{table_name}", json=[record])
                        if res.ok:  # check any 2xx status
                            try:
                                data = res.json()
                                st.success(f"✅ {data.get('message', 'Row inserted successfully!')}")
                                st.write("Inserted columns:", data.get("columns"))
                                st.write("Row count:", data.get("row_count"))
                            except Exception:
                                st.success("✅ Row inserted successfully!")
                        else:
                            st.error(f"❌ Failed: {res.text}")
                    except Exception as e:
                        st.error(f"Request failed: {e}")

        else:  # JSON Mode
            st.subheader("Insert Data via JSON")
            st.caption("Example: [{'id':1,'value':42.5}, {'id':2,'value':38.9}]")
            data_input = st.text_area("Enter JSON array", height=150)

            if st.button("Insert Data"):
                try:
                    records = json.loads(data_input)
                    res = requests.post(f"{API_BASE}/ingest/{table_name}", json=records)
                    if res.ok:  # safer than checking 200 explicitly
                        try:
                            data = res.json()
                            st.success(f"✅ {data.get('message', 'Row inserted successfully!')}")
                        except Exception:
                            st.success("✅ Row inserted successfully!")
                    else:
                        st.error(f"❌ Failed: {res.text}")
                except json.JSONDecodeError:
                    st.error("Invalid JSON format")
                except Exception as e:
                    st.error(f"Request failed: {e}")

# --- 3. VIEW DATA ---
elif menu == "View Data":
    st.subheader("📊 View and Visualize Table Data")

    # Fetch table list
    try:
        resp = requests.get(f"{API_BASE}/tables")
        tables = resp.json() if resp.status_code == 200 else []
        table_names = [t["table_name"] for t in tables]
    except Exception as e:
        st.error(f"Failed to fetch tables: {e}")
        table_names = []

    if table_names:
        selected_table = st.selectbox("Select Table", table_names)

        if selected_table and st.button("Load Data"):
            try:
                # Fetch columns
                cols_resp = requests.get(f"{API_BASE}/tables/{selected_table}/columns")
                columns = [c["column_name"] for c in cols_resp.json()] if cols_resp.status_code == 200 else []

                # Fetch data
                data_resp = requests.get(f"{API_BASE}/query", params={"table": selected_table})
                if data_resp.status_code == 200:
                    resp_json = data_resp.json()
                    rows = resp_json.get("data", [])
                    if rows:
                        import pandas as pd
                        df = pd.DataFrame(rows, columns=columns)
                        st.dataframe(df)  # scrollable table

                        # Detect time-series columns for line chart
                        ts_cols = [c for c in df.columns if "timestamp" in c.lower() or "date" in c.lower()]
                        value_cols = [c for c in df.columns if c.lower() not in ts_cols]

                        if ts_cols and value_cols:
                            for val_col in value_cols:
                                st.subheader(f"📈 {val_col} over {ts_cols[0]}")
                                st.line_chart(df.set_index(ts_cols[0])[val_col])
                        else:
                            st.info("No suitable time-series columns found for charting.")
                    else:
                        st.info("No data available in this table.")
                else:
                    st.error(f"Error fetching data: {data_resp.text}")
            except Exception as e:
                st.error(f"Request failed: {e}")
    else:
        st.warning("No tables found. Please create one first.")


elif menu == "Delete Table":
    st.title("🗑 Delete Table")

    # Fetch table list
    try:
        tables = requests.get(f"{API_BASE}/tables").json()
        table_names = [t["table_name"] for t in tables]
    except Exception as e:
        st.error(f"Failed to load tables: {e}")
        table_names = []

    if table_names:
        table_to_delete = st.selectbox("Select Table to Delete", table_names)
        confirm = st.checkbox("Yes, I want to delete this table permanently")

        if confirm and st.button("Delete Table"):
            try:
                res = requests.delete(f"{API_BASE}/tables/{table_to_delete}")
                if res.ok:
                    st.success(f"✅ Table `{table_to_delete}` deleted successfully!")
                else:
                    st.error(f"❌ Failed: {res.text}")
            except Exception as e:
                st.error(f"Request failed: {e}")
    else:
        st.warning("No tables available to delete")

# --- 4. MANUAL REFRESH ---
elif menu == "Manual Refresh":
    st.subheader("Trigger Manual Refresh")

    resp = requests.get(f"{API_BASE}/tables")
    tables = resp.json() if resp.status_code == 200 else []
    table_names = [t["table_name"] for t in tables if t["table_type"] == "time_series"]

    selected_table = st.selectbox("Select Time-Series Table", table_names)

    if selected_table and st.button("Refresh Now"):
        try:
            resp = requests.post(f"{API_BASE}/refresh/{selected_table}")
            if resp.status_code == 200:
                st.success(f"Manual refresh triggered for {selected_table}")
            else:
                st.error(f"Error: {resp.text}")
        except Exception as e:
            st.error(f"Request failed: {e}")

elif menu == "Refresh History":
    st.title("📜 Refresh History")

    # Fetch tables
    try:
        tables = requests.get(f"{API_BASE}/tables").json()
        table_names = [t["table_name"] for t in tables]
    except:
        table_names = []

    table = st.selectbox("Select Table", table_names)

    if table:
        try:
            logs = requests.get(f"{API_BASE}/refresh_logs/{table}").json()
        except Exception as e:
            st.error(f"Failed to load logs: {e}")
            logs = []

        if logs:
            st.subheader(f"Last refresh attempts for `{table}`")

            df = pd.DataFrame(logs)
            df["created_at"] = pd.to_datetime(df["created_at"])

            st.dataframe(df)

            st.write("### Status Breakdown")
            st.bar_chart(df["status"].value_counts())

        else:
            st.info("No logs found yet.")

elif menu == "Data Source Config":
    st.title("⚙️ Data Source Configuration")

    # Fetch all tables
    tables = requests.get(f"{API_BASE}/tables").json()
    table_names = [t["table_name"] for t in tables]

    table = st.selectbox("Select Table", table_names)

    if table:
        # show existing metadata
        meta = next((x for x in tables if x["table_name"] == table), None)

        st.subheader("Current Settings")
        st.json(meta)

        st.subheader("Update Settings")

        url = st.text_input("Data Source URL", meta.get("data_source_url", ""))
        interval = st.number_input("Refresh Interval (seconds)", min_value=5, value=meta.get("refresh_interval") or 60)

        if st.button("Save Settings"):
            payload = {
                "data_source_url": url,
                "refresh_interval": interval,
            }
            resp = requests.put(f"{API_BASE}/tables/{table}/config", json=payload)

            if resp.ok:
                st.success("Settings updated!")
            else:
                st.error(resp.text)

# --- 5. LIVE DASHBOARD ---
elif menu == "Live Dashboard":
    st.subheader("📈 Live Data Dashboard")

    # Step 1: Choose a table
    resp = requests.get(f"{API_BASE}/tables")
    tables = resp.json() if resp.status_code == 200 else []
    time_series_tables = [t["table_name"] for t in tables if t["table_type"] == "time_series"]

    selected_table = st.selectbox("Select Time-Series Table", time_series_tables)
    refresh_rate = st.slider("Refresh every (seconds):", 5, 60, 10)

    # Step 2: Placeholder for chart
    chart_placeholder = st.empty()
    data_placeholder = st.empty()

    # Step 3: Live update loop
    if selected_table:
        st.info("Live mode started — fetching updates continuously. Stop by changing menu.")
        while True:
            try:
                resp = requests.get(f"{API_BASE}/query", params={"table": selected_table})
                if resp.status_code == 200:
                    data = resp.json()
                    if data:
                        df = pd.DataFrame(data)
                        if "timestamp" in df.columns and "value" in df.columns:
                            df["timestamp"] = pd.to_datetime(df["timestamp"])
                            df = df.sort_values("timestamp")
                            chart_placeholder.line_chart(df.set_index("timestamp")["value"])
                            data_placeholder.dataframe(df.tail(5))
                        else:
                            chart_placeholder.warning("No 'timestamp' or 'value' columns found.")
                    else:
                        chart_placeholder.info("No data available yet.")
                else:
                    chart_placeholder.error(f"Error: {resp.text}")
            except Exception as e:
                chart_placeholder.error(f"Request failed: {e}")

            time.sleep(refresh_rate)
            st.rerun()  # refresh Streamlit loop safely
