# app/core/db.py
import os
from contextlib import contextmanager

from dotenv import load_dotenv
import psycopg

# Carga .env desde la raíz del repo
load_dotenv()

def _get_raw_dsn() -> str:
    # Prioridad: DATABASE_URL > DB_URL > default local
    return os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "postgresql://postgres:postgres@localhost:5432/munirdls"

def _clean_dsn(v: str) -> str:
    # Quita espacios y saltos (\n, \r) que rompen psycopg: "require\n"
    v = (v or "").strip()
    return v

def _get_dsn() -> str:
    return _clean_dsn(_get_raw_dsn())

DSN = _get_dsn()

# Log opcional para ver el valor real (sin exponer credenciales en prod)
if os.getenv("DEBUG_DB_DSN") == "1":
    # Muestra el repr para detectar \n o \r
    print(f"[DB] DSN (repr): {DSN!r}")

# Pool opcional (si psycopg_pool está disponible)
try:
    from psycopg_pool import ConnectionPool  # type: ignore

    # Parámetros conservadores; ajustá si necesitás
    pool = ConnectionPool(
        conninfo=DSN,
        min_size=1,
        max_size=10,
        max_idle=30,
        timeout=10,           # tiempo máximo esperando una conn del pool
        kwargs={"connect_timeout": 10},  # timeout de conexión a PG
    )
except Exception as e:
    print(f"[DB] psycopg_pool no disponible o fallo creando pool: {e}")
    pool = None

@contextmanager
def get_conn():
    """
    Entrega una conexión limpia, usando pool si está disponible.
    """
    if pool is not None:
        with pool.connection() as conn:
            yield conn
    else:
        # Conexión directa (sin pool). Usa el DSN ya limpiado.
        with psycopg.connect(DSN) as conn:
            yield conn
