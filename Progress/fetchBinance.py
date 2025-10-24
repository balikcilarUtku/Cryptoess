import requests
import pandas as pd
import pyodbc
from datetime import datetime, timezone

BINANCE_SYMBOL = "SHIBUSDT"
DB_SYMBOL = "SHIBUSD"
INTERVAL = "1h"
LIMIT = 50

url = "https://api.binance.com/api/v3/klines"
params = {"symbol": BINANCE_SYMBOL, "interval": INTERVAL, "limit": LIMIT}

r = requests.get(url, params=params, timeout=15)
r.raise_for_status()
data = r.json()

cols = [
    "openTime", "open", "high", "low", "close", "volume",
    "closeTime", "quoteAssetVolume", "numberOfTrades",
    "takerBuyBaseAssetVolume", "takerBuyQuoteAssetVolume", "ignore"
]

df = pd.DataFrame(data,columns=cols)

df["openTime"] = pd.to_datetime(df["openTime"], unit='ms', utc=True)
df["closeTime"] = pd.to_datetime(df["closeTime"], unit='ms', utc=True)

df = df[["openTime", "open", "high", "low", "close", "volume"]].copy()

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=Cryptolab;"
    "Trusted_connection=yes;"
    "Encrypt=no;"
)
cn = pyodbc.connect(conn_str)
cur = cn.cursor()

row = cur.execute("SELECT assetID FROM dbo.Assets WHERE symbol = ?", DB_SYMBOL).fetchone()
if not row:
    cur.execute("INSERT INTO dbo.Assets (symbol, name) VALUES (?,?)", (DB_SYMBOL, "Shiba Inu (USD)"))
    cn.commit()
    row = cur.execute("SELECT assetID FROM dbo.Assets WHERE symbol = ?", DB_SYMBOL).fetchone()

assetID = row[0]

def toNativeUtc(dt):
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt

records = []
for rec in df.itertuples(index=False):
    records.append((
        assetID,
        INTERVAL,
        toNativeUtc(rec.openTime),
        float(rec.open),
        float(rec.high),
        float(rec.low),
        float(rec.close),
        float(rec.volume)
   ))
    
sql_merge = """
MERGE dbo.OHLC AS t
USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?)) AS v(assetID, [interval], ts_utc, [open], [high], [low], [close], volume)
ON t.assetID=v.assetID AND t.[interval]=v.[interval] AND t.ts_utc=v.ts_utc
WHEN MATCHED THEN
    UPDATE SET t.[open]=v.[open], t.[high]=v.[high], t.[low]=v.[low], t.[close]=v.[close], t.volume=v.volume
WHEN NOT MATCHED THEN
    INSERT (assetID, [interval], ts_utc, [open], [high], [low], [close], volume)
    VALUES (v.assetID, v.[interval], v.ts_utc, v.[open], v.[high], v.[low], v.[close], v.volume);
"""

cur.fast_executemany = True
cur.executemany(sql_merge, records)
cn.commit()

check = cur.execute("""
SELECT TOP 5 ts_utc, [open], [high], [low], [close], volume
FROM dbo.OHLC
WHERE assetID = ? AND [interval] = ?
ORDER BY ts_utc DESC
""", (assetID, INTERVAL)).fetchall()

print("Son 5 Kayıt : ")
for r in check:
    print(r)

cur.close()
cn.close()
print("Yükleme tamamlandı.")
