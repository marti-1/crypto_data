import argparse
import config as cfg
import db
import requests

config = cfg.load()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', type=str)
    parser.add_argument('--limit', type=int, nargs="?", default=100)

    args = parser.parse_args()
    pair = args.symbol.upper()

    r = requests.post(f"{config['exchange_proxy']['host']}/binance", json={
            'method': 'GET',
            'path': f"/api/v3/depth?symbol={args.symbol}&limit={args.limit}"
        })
    if r.status_code == 200:
        with db.get_connection() as conn:
            db.exec(conn, 'INSERT INTO binance_order_book (symbol, response) VALUES (%s, %s)', (pair, r.text))