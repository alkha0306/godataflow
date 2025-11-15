import streamlit as st
from views.home import home_page
from views.data_management import data_management_page
from views.etl_automation import etl_automation_page
from views.live_dashboard import live_dashboard_page
from views.queries import queries_page

st.set_page_config(page_title="GoDataFlow", layout="wide")

# ---------- Custom Sidebar Style ----------
st.markdown("""
<style>
.sidebar .menu-button {
    padding: 0.6rem 1rem;
    margin-bottom: 6px;
    border-radius: 8px;
    font-size: 15px;
    cursor: pointer;
    background-color: rgba(240,240,240,0.3);
    color: #222;
    transition: 0.2s;
}
.sidebar .menu-button:hover {
    background-color: #E8E8E8;
}
.sidebar .menu-button-active {
    background-color: #4F8BF9 !important;
    color: white !important;
}
</style>
""", unsafe_allow_html=True)

# ---------- Sidebar Header ----------
st.sidebar.title("🚀 GoDataFlow")

# ---------- Menu Items ----------
PAGES = {
    "Home": ("🏠", home_page),
    "Data Management": ("📦", data_management_page),
    "ETL Automation": ("⚙️", etl_automation_page),
    "Live Dashboard": ("📈", live_dashboard_page),
    "Saved Queries": ("🔍", queries_page),
}

# ---------- Sidebar Navigation ----------
selected_page = st.session_state.get("page", "Home")

for label, (icon, _) in PAGES.items():
    is_active = (label == selected_page)

    button_class = "menu-button-active" if is_active else "menu-button"

    if st.sidebar.button(f"{icon}  {label}", key=label, help=f"Go to {label}", use_container_width=True):
        st.session_state.page = label
        st.rerun()

# ---------- Render Selected Page ----------
page_func = PAGES[st.session_state.get("page", "Home")][1]
page_func()
