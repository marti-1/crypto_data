import requests
import config as cfg
import db
import json

config = cfg.load()

url = 'http://api.kyber.network/market'
data = requests.get(url).json()['data']

with db.get_connection() as conn:
    db.exec(conn, 'INSERT INTO kyber_market (data) VALUES (%s)', (json.dumps(data),))
