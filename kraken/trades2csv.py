import argparse
import config as cfg
import json
import csv

config = cfg.load()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('trades_file', type=str)
    parser.add_argument('pair', type=str)
    parser.add_argument('output', type=str)

    args = parser.parse_args(['output/XETHZUSD_trades.json', 'XETHZUSD', 'output/XETHZUSD_trades.csv'])

    all_trades = []
    with open(args.trades_file) as f:
        for line in f:
            trades = json.loads(line)[args.pair]
            for trade in trades:
                all_trades.append({
                    'price': trade[0],
                    'volume': trade[1],
                    'time': int(trade[2]),
                    'buy_sell': trade[3],
                    'market_limit': trade[4],
                    'misc': trade[5]
                })

    with open(args.output, 'w', newline='') as csvfile:
        wr = csv.DictWriter(csvfile, fieldnames=['price', 'volume', 'time', 'buy_sell', 'market_limit', 'misc'])
        wr.writeheader()
        wr.writerows(all_trades)
