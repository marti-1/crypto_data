import json
import arrow
import requests
from tqdm import tqdm
import db
import time
import numpy as np


def test(quotes):
    t = sorted([arrow.get(x) for x in quotes.keys()])
    t_s = [x.format('YYYY-MM-DD') for x in t]
    dt = [(x[0].date() - x[1].date()).days for x in zip(t[1:], t[0:-1])]
    assert (len(t_s) == len(set(t_s)))
    assert (max(dt) == 1)


def interpolate1(x1, x2, n):
    if x1 is None or x2 is None:
        return [0.0] * n
    else:
        return np.linspace(x1, x2, 2 + n)[1:-1]


def interpolate_n(xs1, xs2, n):
    out = np.zeros((n, len(xs1)))
    for i in range(len(xs1)):
        out[:, i] = interpolate1(xs1[i], xs2[i], n)
    return out


def interpolate(d1, d2, n):
    out = {}
    for k in d1.keys():
        out[k] = interpolate_n(d1[k], d2[k], n)
    out_flat = []
    for i in range(n):
        y = {}
        for k in d1.keys():
            y[k] = list(out[k][i])
        out_flat.append(y)
    return out_flat


def data_url(_id, time_start, time_end):
    return f"https://web-api.coinmarketcap.com/v1.1/cryptocurrency/quotes/historical?convert=USD,BTC&format=chart_crypto_details&id={_id}&interval=1d&time_end={time_end}&time_start={time_start}"


def get_last_scraped(conn):
    return {x['cmc_id']: x['ts']
            for x in db.all(conn, 'SELECT cmc_id, max(timestamp) as ts FROM coinmarketcap_daily_data GROUP BY cmc_id')}


def get_id_map(conn):
    x = db.one(conn, 'select * from coinmarketcap_map_data order by dt desc')
    return x['data']


with db.get_connection() as conn:
    id_map = get_id_map(conn)
    # scrape data
    scraped = get_last_scraped(conn)
    for row in tqdm(id_map):
        _id = row['id']
        symbol = row['symbol']

        time_end = arrow.utcnow().timestamp
        if _id in scraped:
            time_start = scraped[_id]
        else:
            time_start = arrow.get('2013-04-28').timestamp

        if _id in scraped and time_end - 60 * 60 * 24 < scraped[_id]:
            continue

        url = data_url(_id, time_start, time_end)
        r = requests.get(url)

        data = r.json()['data']
        if len(data) == 0:
            continue
        first_row = list(data.values())[0]
        if not ('BTC' in first_row or 'ETH' in first_row or 'USD' in first_row):
            continue

        values = (_id, symbol, json.dumps(data), time_end,)
        db.exec(conn, 'INSERT INTO coinmarketcap_daily_data (cmc_id, symbol, data, timestamp) VALUES (%s, %s, %s, %s)',
                values)

        time.sleep(1)

    # fix data
    rows = db.all(conn, 'SELECT * FROM coinmarketcap_daily_data WHERE fixed_data IS NULL')
    for row in tqdm(rows):
        quotes = sorted(row['data'].items(), key=lambda x: x[0])
        fixed = {}
        if len(quotes) == 1:
            continue
        for i in range(len(quotes) - 1):
            q_dt, q_val = quotes[i]
            z_dt, z_val = quotes[i + 1]

            q_dt = arrow.get(q_dt)
            z_dt = arrow.get(z_dt)

            ddays = (z_dt.date() - q_dt.date()).days
            if ddays == 0:
                continue

            fixed[q_dt.format()] = q_val

            if ddays > 1:
                interpolated = interpolate(q_val, z_val, ddays - 1)
                for j in range(ddays - 1):
                    fixed[q_dt.shift(days=j + 1).format()] = interpolated[j]

        fixed[z_dt.format()] = z_val

        # run the same tests on fixed
        test(fixed)

        db.exec(conn, 'UPDATE coinmarketcap_daily_data SET fixed_data = %s WHERE id = %s;',
                (json.dumps(fixed), row['id'],))
