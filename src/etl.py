import os
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()

server = os.getenv("SQL_SERVER")
database = os.getenv("SQL_DATABASE")
username = os.getenv("SQL_USERNAME")
password_raw = os.getenv("SQL_PASSWORD")

missing = [
    name for name, value in {
        "SQL_SERVER": server,
        "SQL_DATABASE": database,
        "SQL_USERNAME": username,
        "SQL_PASSWORD": password_raw,
    }.items()
    if not value
]

if missing:
    raise ValueError(f"Missing environment variables: {', '.join(missing)}")

password = quote_plus(password_raw)

connection_string = (
    f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&Encrypt=yes"
    "&TrustServerCertificate=no"
    "&Connection+Timeout=30"
)

engine = create_engine(connection_string)

df = pd.read_csv("data/finance_kpi.csv")

df["variance"] = df["actual"] - df["budget"]
df["variance_pct"] = ((df["actual"] - df["budget"]) / df["budget"] * 100).round(2)

df.to_sql("finance_kpi", engine, if_exists="replace", index=False)

print("SUCCESS: uploaded to Azure SQL")