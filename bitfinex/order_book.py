import argparse
import config as cfg
import db
import requests

config = cfg.load()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('pair', type=str)

    # args = parser.parse_args(['daiusd'])
    args = parser.parse_args()
    pair = args.pair.upper()

    r = requests.get(f"https://api-pub.bitfinex.com/v2/book/{args.pair}/R0?len=100")
    if r.status_code == 200:
        with db.get_connection() as conn:
            db.exec(conn, 'INSERT INTO bitfinex_order_book (symbol, response) VALUES (%s, %s)', (pair, r.text))
