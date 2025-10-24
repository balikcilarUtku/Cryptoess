import pyodbc
import pandas as pd

conn_str = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=Cryptolab;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
)
cn = pyodbc.connect(conn_str)

query = """
SELECT ts_utc, [close]
FROM dbo.OHLC
WHERE assetID = (
    SELECT assetID FROM dbo.Assets WHERE symbol = 'SHIBAUSD'
)
AND [interval] = '1h'
ORDER BY ts_utc;
"""

df = pd.read_sql(query, cn)

df["MA20"] = df["close"].rolling(window=20).mean()

records = []
for row in df.dropna().itertuples(index=False):
    records.append((
        "SHIBAUSD",
        row.ts_utc,
        "MA20_1h",
        float(row.MA20)
    ))

cur = cn.cursor()
sql_insert = """
INSERT INTO dbo.Indicators (assetID, ts_utc, name, value)
SELECT assetID, ?, ?, ?
FROM dbo.Assets WHERE symbol = ?;
"""

cur.fast_executemany = True
cur.executemany(sql_insert, [(r[0], r[1], r[2], r[0]) for r in records])
cn.commit()

print(f"{len(records)} MA20 eklendi")
cur.close()
cn.close()