# frontend/pages/queries.py
import streamlit as st
from utils.api import api_get
from config import API_BASE

def queries_page():
    st.title("🧠 Saved Queries")
    st.info("Coming soon — run, save and visualize custom SQL queries.")
