import argparse
import config as cfg
import json
from datatlon import *

config = cfg.load()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('trades_file', type=str)
    parser.add_argument('pair', type=str)
    parser.add_argument('--interval', type=int, default=60*60*24)

    args = parser.parse_args()

    all_trades = []
    with open(args.trades_file) as f:
        for line in f:
            trades = json.loads(line)[args.pair]
            for trade in trades:
                all_trades.append(trade)
    table = {
        'price': np.array([float(x[0]) for x in all_trades]),
        'time': np.array([int(x[2]) for x in all_trades])
    }

    table['time_truncated'] = table['time'] // args.interval * args.interval
    table = arrange(table, 'time_truncated')
    gs = group_by(table, 'time_truncated')

    ohlc = summarize(gs,
         dt=sf(first, 'time_truncated'),
         open=sf(first, 'price'),
         high=sf(np.max, 'price'),
         low=sf(np.min, 'price'),
         close=sf(last, 'price')
    )

    df = to_df(ohlc)
    df.to_csv(f"output/{args.pair}_ohlc.csv", index=False)
