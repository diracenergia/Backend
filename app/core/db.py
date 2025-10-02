# app/core/db.py
from __future__ import annotations

import os
import socket
import time
from contextlib import contextmanager
from urllib.parse import urlparse

from dotenv import load_dotenv
import psycopg
from psycopg import pq
from psycopg.errors import OperationalError

try:
    from psycopg_pool import ConnectionPool  # type: ignore
except Exception:
    ConnectionPool = None  # type: ignore

load_dotenv()

def _clean(v: str | None) -> str:
    return (v or "").strip()

def _main_raw_dsn() -> str:
    return os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "postgresql://postgres:postgres@localhost:5432/postgres"

def _events_raw_dsn() -> str:
    return os.getenv("EVENTS_DB_URL") or _main_raw_dsn()

DSN        = _clean(_main_raw_dsn())
EVENTS_DSN = _clean(_events_raw_dsn())

if os.getenv("DEBUG_DB_DSN") == "1":
    print(f"[DB] DSN (repr): {DSN!r}")
if os.getenv("DEBUG_EVENTS_DSN") == "1":
    print(f"[DB] EVENTS_DSN (repr): {EVENTS_DSN!r}")

FORCE_IPV4 = os.getenv("DB_FORCE_IPV4") == "1"

def _resolve_ipv4(host: str | None) -> str | None:
    if not host:
        return None
    try:
        return socket.getaddrinfo(host, None, family=socket.AF_INET)[0][4][0]
    except Exception:
        return None

MAIN_IPV4   = _resolve_ipv4(urlparse(DSN).hostname)        if FORCE_IPV4 else None
EVENTS_IPV4 = _resolve_ipv4(urlparse(EVENTS_DSN).hostname) if FORCE_IPV4 else None
if FORCE_IPV4:
    print(f"[DB] FORCE_IPV4=1 MAIN_IPV4={MAIN_IPV4} EVENTS_IPV4={EVENTS_IPV4}")

# ---------- Tenancy ----------
try:
    from app.core.tenancy import get_context
except Exception:  # pragma: no cover
    def get_context():
        return (None, None, None)

def _begin_if_idle(cur: "psycopg.Cursor") -> None:
    conn = cur.connection
    status = conn.pgconn.transaction_status
    if status == pq.TransactionStatus.IDLE:
        cur.execute("BEGIN")

def _apply_rls_context(conn: "psycopg.Connection") -> None:
    try:
        user_id, org_id, role = get_context()
    except Exception:
        user_id = org_id = role = None

    with conn.cursor() as cur:
        _begin_if_idle(cur)
        # *** LOCAL a la transacción (true) para evitar problemas con pooler transaction ***
        if org_id is not None:
            cur.execute("SELECT set_config('app.org_id', %s, true)", (str(int(org_id)),))
        if user_id is not None:
            cur.execute("SELECT set_config('app.user_id', %s, true)", (str(int(user_id)),))
        if role is not None:
            cur.execute("SELECT set_config('app.role', %s, true)", (str(role),))

# ---------- Parámetros tunables ----------
DB_DISABLE_POOL   = os.getenv("DB_DISABLE_POOL", "0") in ("1", "true", "yes")
DB_POOL_MIN       = int(os.getenv("DB_POOL_MIN", "1"))
DB_POOL_MAX       = int(os.getenv("DB_POOL_MAX", "5"))      # mantén 5 para Supabase free
DB_POOL_TIMEOUT   = float(os.getenv("DB_POOL_TIMEOUT", "8"))
DB_CONNECT_TIMEOUT= int(os.getenv("DB_CONNECT_TIMEOUT", "8"))

# TCP keepalives útiles con poolers / NATs
KEEPALIVES_KW = dict(
    keepalives = int(os.getenv("DB_KEEPALIVES", "1")),
    keepalives_idle = int(os.getenv("DB_KEEPALIVES_IDLE", "30")),
    keepalives_interval = int(os.getenv("DB_KEEPALIVES_INTERVAL", "10")),
    keepalives_count = int(os.getenv("DB_KEEPALIVES_COUNT", "5")),
)

def _connect_kwargs() -> dict:
    kw = dict(connect_timeout=DB_CONNECT_TIMEOUT)
    kw.update(KEEPALIVES_KW)
    if MAIN_IPV4:
        kw["hostaddr"] = MAIN_IPV4
    return kw

def _connect_events_kwargs() -> dict:
    kw = dict(connect_timeout=DB_CONNECT_TIMEOUT, autocommit=True)
    kw.update(KEEPALIVES_KW)
    if EVENTS_IPV4:
        kw["hostaddr"] = EVENTS_IPV4
    return kw

# ---------- Pool opcional ----------
pool = None
if not DB_DISABLE_POOL and ConnectionPool is not None:
    try:
        pool = ConnectionPool(
            conninfo=DSN,
            min_size=DB_POOL_MIN,
            max_size=DB_POOL_MAX,
            max_idle=30,
            timeout=DB_POOL_TIMEOUT,
            kwargs=_connect_kwargs(),
        )
    except Exception as e:
        print(f"[DB] psycopg_pool init failed -> {e}")
        pool = None
else:
    if DB_DISABLE_POOL:
        print("[DB] Pool DISABLED by env DB_DISABLE_POOL")
    elif ConnectionPool is None:
        print("[DB] psycopg_pool not installed")

# ---------- Conexión normal con 1 retry corto ----------
@contextmanager
def get_conn():
    """
    Conexión para rutas/repos.
    - Usa pool si está disponible.
    - 1 retry corto ante OperationalError al conectar.
    - Setea GUCs de RLS como LOCAL a la transacción.
    """
    def _yield_connected(conn: "psycopg.Connection"):
        _apply_rls_context(conn)
        return conn

    if pool is not None:
        # pool ya gestiona retries internos; igual hacemos 1 intento + retry manual
        for attempt in (1, 2):
            try:
                with pool.connection() as conn:
                    yield _yield_connected(conn)
                    return
            except OperationalError as e:
                if attempt == 2:
                    raise
                time.sleep(0.3)
        return

    # sin pool
    for attempt in (1, 2):
        try:
            with psycopg.connect(DSN, **_connect_kwargs()) as conn:
                yield _yield_connected(conn)
                return
        except OperationalError:
            if attempt == 2:
                raise
            time.sleep(0.3)

# ---------- Conexión de eventos (si la usas) ----------
@contextmanager
def get_events_conn():
    for attempt in (1, 2):
        try:
            with psycopg.connect(EVENTS_DSN, **_connect_events_kwargs()) as conn:
                yield conn
                return
        except OperationalError:
            if attempt == 2:
                raise
            time.sleep(0.5)
