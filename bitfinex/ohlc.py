import argparse
import config as cfg
import db
import requests
import pandas as pd

config = cfg.load()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('pair', type=str)

    args = parser.parse_args(['tETHUSD'])
    # args = parser.parse_args()
    pair = args.pair.upper()
    url = f"https://api-pub.bitfinex.com/v2/candles/trade:1D:{args.pair}/hist?limit=10000"
    r = requests.get(url)

    if r.status_code == 200:
        df = pd.DataFrame(columns=["dt", "open", "high", "low", "close", "volume"])
        for row in r.json():
            df = df.append({
                "dt": row[0] // 1000,
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4],
                "volume": row[5]
            }, ignore_index=True)
        df = df.astype({"dt": int})
        df.to_csv(f'{args.pair}_ohlc_1d.csv', index=False, line_terminator='\n')
    else:
        raise Exception('Failed.')