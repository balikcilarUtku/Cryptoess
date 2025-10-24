import pyodbc
import pandas as pd
import matplotlib.pyplot as plt

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=CryptoLab;"
    "Trusted_Connection=Yes;"
    "Encrypt=no;"
)
cn = pyodbc.connect(conn_str)

q_close = """
SELECT o.ts_utc, o.[close]
FROM dbo.OHLC o
JOIN dbo.Assets a ON a.assetID = o.assetID
WHERE a.symbol='SHIBUSD' AND o.[interval]='1h'
ORDER BY o.ts_utc;
"""
df_close = pd.read_sql(q_close, cn).astype({"close": float})

q_ma = """
SELECT i.ts_utc, i.value AS MA20
FROM dbo.Indicators i
JOIN dbo.Assets a ON a.assetID = i.assetID
WHERE a.symbol='SHIBAUSD' AND i.name='MA20_1h'
ORDER BY i.ts_utc;
"""
df_ma = pd.read_sql(q_ma, cn).astype({"MA20": float})

cn.close()

df = pd.merge(df_close, df_ma, on="ts_utc", how="left")

##print("---- DEBUG ----")
##print("Close veri sayısı:", len(df_close))
##print("MA20 veri sayısı:", len(df_ma))
##print("Merge sonrası:", len(df))
##print(df.head(10))
##print("----------------")

plt.figure(figsize=(10, 5))
plt.plot(df["ts_utc"], df["close"], label="Close")
plt.plot(df["ts_utc"], df["MA20"], label="MA20")
plt.title("SHIBAUSD - 1h Close vs MA20")
plt.xlabel("Time (UTC)")
plt.ylabel("Price")
plt.legend()
plt.tight_layout()
plt.show()
