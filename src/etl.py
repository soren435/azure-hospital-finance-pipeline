import os
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

df = pd.read_csv("data/finance_kpi.csv")

df["variance"] = df["actual"] - df["budget"]
df["variance_pct"] = df["variance"] / df["budget"]

server = os.getenv("SQL_SERVER")
database = os.getenv("SQL_DATABASE")
username = os.getenv("SQL_USERNAME")
password = quote_plus(os.getenv("SQL_PASSWORD"))

conn = (
    f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}"
    "?driver=ODBC+Driver+18+for+SQL+Server"
    "&Encrypt=yes"
    "&TrustServerCertificate=no"
)

engine = create_engine(conn)

df.to_sql("finance_kpi", engine, if_exists="replace", index=False)

print("SUCCESS: uploaded to Azure SQL")