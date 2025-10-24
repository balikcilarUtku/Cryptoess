import pyodbc
import pandas as pd
from datetime import datetime, timezone

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

df["close"] = df["close"].astype(float)
df["MA20"] = df["close"].rolling(window=20, min_periods=20).mean()

print("Toplam satır:", len(df), "| MA20 dolu:", df["MA20"].notna().sum())

def to_naive_datetime(x):
    # pandas/numpy timestamp -> python datetime (tz'siz, UTC)
    if isinstance(x, pd.Timestamp):
        x = x.to_pydatetime()
    if hasattr(x, "tzinfo") and x.tzinfo is not None:
        x = x.astimezone(timezone.utc).replace(tzinfo=None)
    return x

records = []
for row in df[df["MA20"].notna()].itertuples(index=False):
    records.append((
        "SHIBAUSD",        # symbol
        row.ts_utc,        # timestamp
        "MA20_1h",         # indicator name
        float(row.MA20)    # indicator value
    ))

params = [(to_naive_datetime(r[1]), r[2], r[3], r[0]) for r in records]

if not records:
    print("MA20 için en az 20 veri gerekli. Önce daha fazla OHLC yükleyin.")
    cn.close()
    raise SystemExit

cur = cn.cursor()
sql_insert = """
INSERT INTO dbo.Indicators (assetID, ts_utc, name, value)
SELECT assetID, ?, ?, ?
FROM dbo.Assets WHERE symbol = ?;
"""

cur.fast_executemany = True
cur.executemany(sql_insert, params)
cn.commit()

print(f"{len(records)} MA20 eklendi")
cur.close()
cn.close()