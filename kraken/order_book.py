import argparse
import config as cfg
import db
import requests

config = cfg.load()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('pair', type=str)

    args = parser.parse_args()
    pair = args.pair.upper()

    r = requests.post(f"{config['exchange_proxy']['host']}/kraken", json={
            'path': f"/0/public/Depth?pair={args.pair}",
            'data': {}
        })
    if r.status_code == 200:
        with db.get_connection() as conn:
            db.exec(conn, 'INSERT INTO kraken_order_book (symbol, response) VALUES (%s, %s)', (pair, r.text))