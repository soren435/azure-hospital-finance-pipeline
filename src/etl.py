import pandas as pd

df = pd.read_csv("data/raw/hospital_kpi.csv")
df["variance"] = df["actual"] - df["budget"]
df["variance_pct"] = (df["variance"] / df["budget"] * 100).round(2)

df.to_csv("data/processed/hospital_kpi_clean.csv", index=False)

print("ETL completed")