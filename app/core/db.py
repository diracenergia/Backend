# app/core/db.py
import os
import psycopg
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()  # carga .env desde la raíz del repo

DB_URL = os.getenv("DB_URL", "postgresql://postgres:postgres@localhost:5432/munirdls")

try:
    from psycopg_pool import ConnectionPool  # type: ignore
    pool = ConnectionPool(conninfo=DB_URL, min_size=1, max_size=10, max_idle=30)
except Exception:
    pool = None

@contextmanager
def get_conn():
    if pool:
        with pool.connection() as conn:
            yield conn
    else:
        with psycopg.connect(DB_URL) as conn:
            yield conn
