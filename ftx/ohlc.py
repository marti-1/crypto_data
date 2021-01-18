import arrow
import requests
import pandas as pd

host = 'https://ftx.com/api'

market_name = 'AMPL/USD'
# daily
resolution = 86400
url = f"{host}/markets/{market_name}/candles?resolution={resolution}&limit=5000"
r = requests.get(url).json()

df = pd.DataFrame(columns=["dt", "open", "high", "low", "close", "volume"])
for row in r['result']:
    df = df.append({
        "dt": row['time'] // 1000,
        "open": row['open'],
        "high": row['high'],
        "low": row['low'],
        "close": row['close'],
        "volume": row['volume']
    }, ignore_index=True)
df = df.astype({"dt": int})
df.to_csv('ampl_usd_ohlc_1d.csv', index=False, line_terminator='\n')