import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

st.set_page_config(page_title="Finance KPI Dashboard", layout="wide")

server = "sorenfinance2026.database.windows.net"
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")

conn_str = (
    f"mssql+pyodbc://{username}:{password}@{server}/{database}"
    "?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no"
)

engine = create_engine(conn_str)

df = pd.read_sql("SELECT * FROM finance_kpi", engine)

st.title("Finance KPI Dashboard")

col1, col2, col3 = st.columns(3)

budget = df["budget"].sum()
actual = df["actual"].sum()
variance = actual - budget

col1.metric("Budget", f"{budget:,.0f}")
col2.metric("Actual", f"{actual:,.0f}")
col3.metric("Variance", f"{variance:,.0f}")

st.subheader("Actual vs Budget over time")

chart_df = df.set_index("month")[["actual", "budget"]]
st.line_chart(chart_df)

st.subheader("Raw data")
st.dataframe(df)
