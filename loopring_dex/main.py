import config as cfg
import db
import requests

config = cfg.load()

if __name__ == '__main__':
    r = requests.get('https://api3.loopring.io/api/v2/amm/poolsStats')
    if r.status_code == 200:
        with db.get_connection() as conn:
            db.exec(conn, 'INSERT INTO dex_pool_stats (name, data) VALUES (%s, %s)', ('loopring_dex', r.text))