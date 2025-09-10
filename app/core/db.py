# app/core/db.py
import os
import socket
from contextlib import contextmanager
from urllib.parse import urlparse

from dotenv import load_dotenv
import psycopg

# Carga .env desde la raíz del repo (Render también inyecta envs)
load_dotenv()

# -----------------------------
# Helpers
# -----------------------------
def _clean(v: str | None) -> str:
    # Quita espacios/saltos (\n, \r) que rompen psycopg, p.ej. "require\n"
    return (v or "").strip()

def _main_raw_dsn() -> str:
    # Prioridad: DATABASE_URL > DB_URL > default local
    return os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "postgresql://postgres:postgres@localhost:5432/munirdls"

def _events_raw_dsn() -> str:
    # DSN especial para LISTEN/NOTIFY.
    # Si no está seteado, cae al DSN principal.
    return os.getenv("EVENTS_DB_URL") or _main_raw_dsn()

DSN        = _clean(_main_raw_dsn())
EVENTS_DSN = _clean(_events_raw_dsn())

# Logs opcionales (no exponen secret salvo que vos lo habilites)
if os.getenv("DEBUG_DB_DSN") == "1":
    print(f"[DB] DSN (repr): {DSN!r}")
if os.getenv("DEBUG_EVENTS_DSN") == "1":
    print(f"[DB] EVENTS_DSN (repr): {EVENTS_DSN!r}")

# -----------------------------
# Pool para operaciones normales (HTTP/API, repos, etc.)
# -----------------------------
try:
    from psycopg_pool import ConnectionPool  # type: ignore
    pool = ConnectionPool(
        conninfo=DSN,
        min_size=1,
        max_size=10,
        max_idle=30,
        timeout=10,                     # espera máx. por una conexión del pool
        kwargs={"connect_timeout": 10}  # timeout de conexión a PG
    )
except Exception as e:
    print(f"[DB] psycopg_pool no disponible o fallo creando pool: {e}")
    pool = None

@contextmanager
def get_conn():
    """
    Conexión para operaciones normales de la app.
    Usa pool si está disponible.
    """
    if pool is not None:
        with pool.connection() as conn:
            yield conn
    else:
        with psycopg.connect(DSN, connect_timeout=10) as conn:
            yield conn

# -----------------------------
# Conexión dedicada para LISTEN/NOTIFY (alarm listener)
# IMPORTANTE: esta conexión NO debe pasar por PgBouncer en modo transaction.
# Apuntá EVENTS_DB_URL a un pooler en *session* o directo :5432 (sslmode=require).
# Soporte de fallback IPv4 cuando el host resuelve a IPv6 y la red no lo soporta.
# -----------------------------
@contextmanager
def get_events_conn():
    """
    Conexión dedicada para el listener (LISTEN/NOTIFY).
    - autocommit=True para que LISTEN reciba notificaciones.
    - No usa pool.
    """
    try:
        with psycopg.connect(EVENTS_DSN, autocommit=True, connect_timeout=10) as conn:
            yield conn
            return
    except psycopg.OperationalError as e:
        # Fallback IPv4 si el host resolvió a IPv6 y la red no lo soporta
        if ("Network is unreachable" in str(e) or "No route to host" in str(e)) and os.getenv("DB_FORCE_IPV4") == "1":
            u = urlparse(EVENTS_DSN)
            host = u.hostname
            try:
                ipv4 = socket.getaddrinfo(host, None, family=socket.AF_INET)[0][4][0]
                with psycopg.connect(EVENTS_DSN, autocommit=True, connect_timeout=10, hostaddr=ipv4) as conn:
                    yield conn
                    return
            except Exception:
                pass
        # Relevantar la excepción original si no hubo fallback
        raise
