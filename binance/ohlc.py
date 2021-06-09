import argparse
import config as cfg
import requests
import arrow
import time
import pandas as pd

config = cfg.load()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', type=str)
    parser.add_argument('--interval', type=str, default='1d')
    parser.add_argument('--start_time', type=int, default=arrow.get('2015-01-01').timestamp)

    args = parser.parse_args()
    symbol = args.symbol.upper()
    start_time = args.start_time

    raw = []
    while True:
        # [
        #   [
        #     1499040000000,      // Open time
        #     "0.01634790",       // Open
        #     "0.80000000",       // High
        #     "0.01575800",       // Low
        #     "0.01577100",       // Close
        #     "148976.11427815",  // Volume
        #     1499644799999,      // Close time
        #     "2434.19055334",    // Quote asset volume
        #     308,                // Number of trades
        #     "1756.87402397",    // Taker buy base asset volume
        #     "28.46694368",      // Taker buy quote asset volume
        #     "17928899.62484339" // Ignore.
        #   ]
        # ]
        r = requests.get(f"https://api.binance.com/api/v3/klines?symbol={args.symbol}&interval={args.interval}&startTime={start_time}")
        if r.status_code == 200:
            if len(r.json()) == 0:
                break
            raw = raw + r.json()
            start_time = raw[-1][0] + 1
        else:
            raise Exception(r.text)

        time.sleep(1)

    dt = [int(tick[0]) for tick in raw]
    open = [float(tick[1]) for tick in raw]
    high = [float(tick[2]) for tick in raw]
    low = [float(tick[3]) for tick in raw]
    close = [float(tick[4]) for tick in raw]

    df = pd.DataFrame.from_dict({
        'dt': dt,
        'open': open,
        'high': high,
        'low': low,
        'close': close
    })

    df.to_csv(f'output/{args.symbol}_ohlc.csv', index=False)