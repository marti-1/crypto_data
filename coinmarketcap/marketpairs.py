import db
import requests
import json
import time
from datetime import date
from tqdm import tqdm


def get_id_map(conn):
    x = db.one(conn, 'select * from coinmarketcap_map_data order by dt desc')
    return x['data']


with db.get_connection() as conn:
    id_map = get_id_map(conn)
    for row in tqdm(id_map):
        id = row['id']

        # get last time it was scraped
        last = db.one(conn, 'select * from coinmarketcap_marketpairs_data where cmc_id = %s order by dt desc limit 1',
                      (id,))
        if last is not None and last['dt'].date() == date.today():
            continue

        url = f"https://web-api.coinmarketcap.com/v1/cryptocurrency/market-pairs/latest?aux=num_market_pairs,category,market_url,notice,price_quote,effective_liquidity,market_score,market_reputation&convert=USD&id={id}&limit=300&sort=cmc_rank&start=1"
        r = requests.get(url)
        r_json = r.json()
        if r_json['status']['error_message'] == 'No matching markets found.':
            continue
        data = r_json['data']
        sql = 'INSERT INTO coinmarketcap_marketpairs_data (cmc_id, data) VALUES (%s, %s)'
        db.exec(conn, sql, (id, json.dumps(data),))
        time.sleep(1)
