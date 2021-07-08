import argparse
import arrow
import db
from tqdm import tqdm
import requests
import time
from pathlib import Path
from decimal import Decimal
import csv
from psycopg2.extras import DictCursor


def get_since_param(args):
    if args.db:
        with db.get_connection() as conn:
            row = db.one(conn, f'select time from kraken_{args.pair}_trades order by time desc limit 1')
            if row is not None:
                return int(row[0])
    elif args.csv:
        if Path(args.csv).is_file():
            with open(args.csv, 'r', encoding='utf-8') as f:
                for line in f:
                    pass
            return Decimal(line.split(',')[2])
    return args.since


def get_trades(pair, since):
    acc = []
    r = requests.post(f"https://api.kraken.com/0/public/Trades?pair={pair}&since={since}")
    if r.status_code == 200:
        trades = r.json()['result'][args.pair]
        if len(trades) == 0:
            return None
        for t in trades:
            acc.append(t)
    return acc


HEADER = 'price,volume,time,buy_sell,market_limit,misc'


def store_trades(trades, args):
    if args.db:
        with db.get_connection() as conn:
            cur = conn.cursor(cursor_factory=DictCursor)
            args_str = b','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s)", x) for x in trades)
            cur.execute(
                f"INSERT INTO kraken_{args.pair}_trades ({HEADER}) VALUES " + args_str.decode('utf-8'))
            conn.commit()
            cur.close()
    elif args.csv:
        if Path(args.csv).is_file():
            with open(args.csv, 'a') as f:
                writer = csv.writer(f)
                writer.writerows(trades)
        else:
            with open(args.csv, 'w') as f:
                writer = csv.writer(f)
                writer.writerow(HEADER.split(','))
                writer.writerows(trades)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('pair', type=str)
    parser.add_argument('--db', dest='db', action='store_true')
    parser.add_argument('--csv', type=str)
    parser.add_argument('--since', type=Decimal, default=arrow.get('2015-01-01').timestamp)
    parser.add_argument('--to', type=int, default=arrow.get().timestamp)

    args = parser.parse_args()
    if args.csv and args.csv == '':
        raise Exception("CSV path can't be empty")

    if not (args.db or args.csv):
        raise Exception("Output destination not specified, use --db or --csv")

    since = get_since_param(args)
    pbar = tqdm(total=args.to - args.since)
    while True:
        trades = get_trades(args.pair, since)
        store_trades(trades, args)

        new_since = trades[-1][2]
        pbar.update(int(new_since) - int(since))
        since = new_since
        if since >= args.to:
            break

        time.sleep(2)
