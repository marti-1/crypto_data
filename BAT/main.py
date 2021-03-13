import config as cfg
import db
import requests
import json
config = cfg.load()

if __name__ == '__main__':
    r = requests.get("https://brave.com/transparency-data.json", headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0'})
    if r.status_code == 200:
        with db.get_connection() as conn:
            db.exec(conn, 'INSERT INTO bat_data (type, data) VALUES (%s, %s)', ('transparency-data.json', r.text))

    r = requests.get("https://brave.com/transparency/", headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0'})
    if r.status_code == 200:
        with db.get_connection() as conn:
            db.exec(conn, 'INSERT INTO bat_data (type, data) VALUES (%s, %s)', ('transparency', json.dumps({'html': r.text})))