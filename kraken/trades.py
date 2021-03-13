import argparse
import config as cfg
import requests
import arrow
import time
import json
from pathlib import Path

config = cfg.load()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('pair', type=str)
    parser.add_argument('--since', type=int, default=arrow.get('2015-01-01').timestamp)

    args = parser.parse_args(['XETHZUSD'])

    since = args.since
    all_trades = []
    output_filename = f"{args.pair}_trades.json"

    if Path(output_filename).is_file():
        with open(output_filename, 'r', encoding='utf-8') as f:
            for line in f:
                pass
            last = line
        since = json.loads(line)['last']

    while True:
        print(since)
        r = requests.post(f"https://api.kraken.com/0/public/Trades?pair={args.pair}&since={since}")
        if r.status_code == 200:
            trades = r.json()['result'][args.pair]
            if len(trades) == 0:
                break
            for t in trades:
                all_trades.append(t)
        else:
            break
        since = r.json()['result']['last']

        with open(f"{args.pair}_trades.json", 'a', encoding='utf-8') as f:
            json.dump(r.json()['result'], f, ensure_ascii=False)
            f.write("\n")

        time.sleep(2)
