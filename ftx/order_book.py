import requests
import argparse
import config as cfg
import db

config = cfg.load()

parser = argparse.ArgumentParser()
parser.add_argument('market_name', type=str)
parser.add_argument('--depth', type=int, default=100)
args = parser.parse_args()

host = 'https://ftx.com/api'
url = f"{host}/markets/{args.market_name}/orderbook?depth={args.depth}"
r = requests.get(url)

with db.get_connection() as conn:
    db.exec(conn, "INSERT INTO ftx_order_book (symbol, response) VALUES (%s, %s)", (args.market_name, r.text,))