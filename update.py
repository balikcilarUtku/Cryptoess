import requests, pyodbc, pandas as pd
from datetime import timezone

BINANCE_SYMBOL = "SHIBUSDT"
DB_SYMBOL      = "SHIBUSD"
INTERVAL       = "1h"
LIMIT          = 200

url = "https://api.binance.com/api/v3/klines"
params = {"symbol": BINANCE_SYMBOL, "interval": INTERVAL, "limit": LIMIT}
r = requests.get(url, params=params, timeout=15)
r.raise_for_status()
data = r.json()

cols = ["open_time","open","high","low","close","volume",
        "close_time","q","n","tb_base","tb_quote","ignore"]
df = pd.DataFrame(data, columns=cols)
df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=CryptoLab;"
    "Trusted_Connection=Yes;"
    "Encrypt=no;"
)
cn = pyodbc.connect(conn_str); cur = cn.cursor()

row = cur.execute("SELECT assetID FROM dbo.Assets WHERE symbol = ?", DB_SYMBOL).fetchone()
if not row:
    raise SystemExit(f"Assets'ta {DB_SYMBOL} yok.")
assetID = row[0]

from datetime import timezone
def to_naive_utc(ts):
    ts = pd.to_datetime(ts, utc=True)
    return ts.tz_convert(None) if hasattr(ts, "tz_convert") else ts.tz_localize(None)

records = []
for rec in df.itertuples(index=False):
    records.append((
        assetID,
        INTERVAL,
        to_naive_utc(rec.open_time),
        float(rec.open), float(rec.high), float(rec.low), float(rec.close),
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

print(f"{len(records)} Kayıt işlendi.")
