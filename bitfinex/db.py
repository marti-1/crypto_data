import psycopg2
from psycopg2.extras import DictCursor
import psycopg2.extras
import config
from contextlib import contextmanager

config = config.load('config.json')


@contextmanager
def get_connection():
    opts = config['db']
    conn = psycopg2.connect(host=opts['host'],
                            user=opts['user'],
                            password=opts['password'],
                            dbname=opts['db'])
    try:
        yield conn
    finally:
        conn.close()


def exec(conn, sql, params=None):
    cur = conn.cursor(cursor_factory=DictCursor)
    cur.execute(sql, params)
    conn.commit()
    cur.close()


def all(conn, sql, params=None):
    cur = conn.cursor(cursor_factory=DictCursor)
    cur.execute(sql, params)
    r = cur.fetchall()
    cur.close()
    return r


def one(conn, sql, params=None):
    cur = conn.cursor(cursor_factory=DictCursor)
    cur.execute(sql, params)
    r = cur.fetchone()
    cur.close()
    return r
