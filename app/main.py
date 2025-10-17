# main.py (temporal)
from fastapi import FastAPI
from psycopg.rows import dict_row
from app.db import get_conn

app = FastAPI()

@app.get("/health/db")
def health_db():
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            select current_user,
                   inet_server_addr()::text as host,
                   inet_server_port() as port,
                   now() as ts
        """)
        return cur.fetchone()
