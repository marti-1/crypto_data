import config as cfg
import requests
import json
import db

config = cfg.load()
url = f"{config['coinmarketcap']['pro']['host']}/v1/cryptocurrency/map"
r = requests.get(url, headers={
    'X-CMC_PRO_API_KEY': config['coinmarketcap']['pro']['apiKey']
})

with db.get_connection() as conn:
    data_str = json.dumps(r.json()['data'])
    db.exec(conn, 'INSERT INTO coinmarketcap_map_data (data) VALUES (%s)', (data_str,))