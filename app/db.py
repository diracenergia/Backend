# app/db.py
import os
from psycopg_pool import ConnectionPool

DSN = os.environ.get("DATABASE_URL")
if not DSN:
    raise RuntimeError("Falta la env DATABASE_URL")

# Pool chico y estable. Ajustá max_size si lo necesitás (con 60 conexiones globales, 8 es seguro).
pool = ConnectionPool(
    conninfo=DSN,
    min_size=1,
    max_size=8,
    max_idle=30,          # segundos
    max_lifetime=3600,    # recicla conexiones cada hora
    timeout=5,            # espera por un conn del pool
    kwargs=dict(
        connect_timeout=5,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
        options="-c statement_timeout=60000"  # 60s por query
    ),
)

def get_conn():
    """Usar como: with get_conn() as conn:"""
    return pool.connection()
