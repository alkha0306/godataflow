
import streamlit as st
import requests
import json

def flatten_json(y, prefix=""):
    # returns list of flattened keys like "hourly.time" or "hourly.temperature_2m"
    out = {}
    if isinstance(y, dict):
        for k, v in y.items():
            full = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                out.update(flatten_json(v, full))
            elif isinstance(v, list):
                # for lists, attempt to flatten first element only (preview-based)
                if len(v) > 0 and isinstance(v[0], dict):
                    out.update(flatten_json(v[0], full))
                else:
                    out[full] = v
            else:
                out[full] = v
    else:
        # primitive/array root
        out[prefix or "value"] = y
    return out

def etl_mapping_wizard(API_BASE):
    st.header("🧭 ETL Mapping Wizard")

    # load tables metadata
    resp = requests.get(f"{API_BASE}/tables")
    tables = resp.json() if resp.ok else []
    table_names = [t["table_name"] for t in tables] if tables else []

    if not table_names:
        st.info("No tables found. Create a table first.")
        return

    selected_table = st.selectbox("Select table to map", table_names)

    # find metadata for selected table (to preload mapping_json if present)
    meta = next((t for t in tables if t["table_name"] == selected_table), {})

    col1, col2 = st.columns([2, 3])
    with col1:
        source_url = st.text_input("API / Source URL", value=meta.get("data_source_url") or "")

        if st.button("Preview Source"):
            if not source_url.strip():
                st.error("Provide a URL to preview.")
            else:
                try:
                    pv = requests.get(f"{API_BASE}/preview_source", params={"url": source_url}, timeout=10)
                    if not pv.ok:
                        st.error(f"Preview failed: {pv.text}")
                    else:
                        preview = pv.json().get("preview")
                        st.subheader("Preview JSON")
                        st.json(preview)
                        # store preview in session state for mapping UI
                        st.session_state["_etl_preview"] = preview
                except Exception as e:
                    st.error(f"Preview request failed: {e}")

    with col2:
        st.write("### Current mapping (stored)")
        current_mapping = meta.get("mapping_json")
        if current_mapping:
            try:
                # if already a dict/object
                st.json(current_mapping)
            except:
                st.write(current_mapping)
        else:
            st.info("No mapping saved yet for this table.")

    # If no preview available, attempt to fetch from preview endpoint if data_source_url exists
    preview_obj = st.session_state.get("_etl_preview")
    if not preview_obj and meta.get("data_source_url"):
        try:
            pv = requests.get(f"{API_BASE}/preview_source", params={"url": meta.get("data_source_url")}, timeout=6)
            if pv.ok:
                preview_obj = pv.json().get("preview")
                st.session_state["_etl_preview"] = preview_obj
        except:
            preview_obj = None

    if not preview_obj:
        st.info("Preview JSON not loaded. Enter URL and click Preview Source (or save URL to metadata then Reload).")
        return

    # flatten preview to keys
    flat = flatten_json(preview_obj)
    json_keys = list(flat.keys())
    json_keys_sorted = sorted(json_keys)

    # fetch table columns to map to
    cols_resp = requests.get(f"{API_BASE}/tables/{selected_table}/columns")
    if not cols_resp.ok:
        st.error(f"Failed to load table columns: {cols_resp.text}")
        return
    cols_list = [c["column_name"] for c in cols_resp.json()]

    st.subheader("Map JSON keys → Table columns")
    st.caption("Choose which JSON key maps to each DB column. Leave blank to ignore.")

    # preload mapping if exists
    existing_mapping = {}
    if meta.get("mapping_json"):
        try:
            existing_mapping = meta.get("mapping_json") if isinstance(meta.get("mapping_json"), dict) else json.loads(meta.get("mapping_json"))
        except Exception:
            existing_mapping = {}

    mapping_result = {}
    # two-column layout for mapping form
    for col in cols_list:
        # try preload: find json key mapped to this column in existing_mapping
        pre = None
        for k, v in existing_mapping.items():
            if v == col:
                pre = k
                break

        chosen = st.selectbox(f"→ Map to column: {col}", options=["(none)"] + json_keys_sorted, index=(json_keys_sorted.index(pre) + 1) if pre in json_keys_sorted else 0, key=f"map_{col}")
        if chosen != "(none)":
            mapping_result[chosen] = col

    # also allow user to save mapping without mapping every column (partial mapping)
    st.write("")
    if st.button("Save Mapping"):
        payload = {}
        # always include mapping_json (may be empty)
        payload["mapping_json"] = mapping_result
        # also include data_source_url (if user entered/changed it)
        payload["data_source_url"] = source_url if source_url.strip() else None

        try:
            save = requests.put(f"{API_BASE}/tables/{selected_table}/config", json=payload, timeout=8)
            if save.ok:
                st.success("Mapping saved to table metadata.")
                # update session and meta (so UI reflects saved mapping next runs)
                st.session_state["_etl_preview"] = preview_obj
            else:
                st.error(f"Save failed: {save.text}")
        except Exception as e:
            st.error(f"Save request error: {e}")

