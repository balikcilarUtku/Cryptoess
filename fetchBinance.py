import requests
import pandas as pd

symbol = "SHIBUSDT"
interval = "1h"
limit = 5

url = "https://api.binance.com/api/v3/klines"
params = {"symbol": symbol, "interval": interval, "limit": limit}

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

print(df[["openTime", "open", "high", "low", "close", "volume"]])
