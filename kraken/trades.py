import argparse
import config as cfg
import requests
import arrow
import json
from pathlib import Path
import sys
import db
from psycopg2.extras import DictCursor
import time

config = cfg.load()


def get_since_param(pair, use_db=False, since=None):
    if use_db:
        with db.get_connection() as conn:
            row = db.one(conn, f'select time from kraken_{pair}_trades order by time desc limit 1')
            if row is not None:
                return int(row[0])
    else:
        output_filename = get_output_filename(pair)
        if Path(output_filename).is_file():
            with open(output_filename, 'r', encoding='utf-8') as f:
                for line in f:
                    pass
            return json.loads(line)['last'] // 1e9
    return since


def get_output_filename(pair):
    return f"output/{pair}_trades.json"


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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('pair', type=str)
    parser.add_argument('--db', dest='db', action='store_true')
    parser.add_argument('--since', type=int, default=arrow.get('2015-01-01').timestamp)
    parser.add_argument('--to', type=int, default=arrow.get().timestamp)

    args = parser.parse_args()

    to = args.to
    since = get_since_param(args.pair, args.db, args.since)

    if since >= to:
        sys.exit()

    while True:
        print(since)
        trades = get_trades(args.pair, since)
        if trades is None:
            break
        if args.db:
            with db.get_connection() as conn:
                cur = conn.cursor(cursor_factory=DictCursor)
                args_str = b','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s)", x) for x in trades)
                cur.execute(
                    f"INSERT INTO kraken_{args.pair}_trades (price,volume,time,buy_sell,market_limit,misc) VALUES " + args_str.decode(
                        'utf-8'))
                conn.commit()
                cur.close()
        else:
            pass
        since = int(trades[-1][2])
        if since >= to:
            break

        time.sleep(2)