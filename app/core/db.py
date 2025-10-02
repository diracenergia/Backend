# app/core/db.py
from __future__ import annotations

import os
import socket
from contextlib import contextmanager
from urllib.parse import urlparse

from dotenv import load_dotenv
import psycopg
from psycopg import pq  # para chequear el estado de transacción

# Pool (psycopg3)
try:
    from psycopg_pool import ConnectionPool  # type: ignore
except Exception:
    ConnectionPool = None  # type: ignore

# ---------------------------------------------------------------------
# Carga .env (en Render no hace falta, pero no molesta)
# ---------------------------------------------------------------------
load_dotenv()

# ---------------------------------------------------------------------
# Helpers DSN
# ---------------------------------------------------------------------
def _strip_quotes(v: str) -> str:
    # Si por error el DSN viene con comillas del panel de Render -> quitalas
    if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
        return v[1:-1]
    return v

def _clean(v: str | None) -> str:
    return _strip_quotes((v or "").strip())

def _main_raw_dsn() -> str:
    # Prioridad: DATABASE_URL > DB_URL > default local
    return os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "postgresql://postgres:postgres@localhost:5432/munirdls"

def _events_raw_dsn() -> str:
    return os.getenv("EVENTS_DB_URL") or _main_raw_dsn()

DSN        = _clean(_main_raw_dsn())
EVENTS_DSN = _clean(_events_raw_dsn())

if os.getenv("DEBUG_DB_DSN") == "1":
    print(f"[DB] DSN (repr): {DSN!r}")
if os.getenv("DEBUG_EVENTS_DSN") == "1":
    print(f"[DB] EVENTS_DSN (repr): {EVENTS_DSN!r}")

# ---------------------------------------------------------------------
# IPv4 forzado (útil en algunos hosts)
# ---------------------------------------------------------------------
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

# ---------------------------------------------------------------------
# Contexto multi-tenant (org/user/role) para RLS
# ---------------------------------------------------------------------
try:
    from app.core.tenancy import get_context  # devuelve (user_id, org_id, role)
except Exception:  # pragma: no cover
    def get_context():
        return (None, None, None)

def _begin_if_idle(cur: "psycopg.Cursor") -> None:
    """
    Abre una transacción solo si la conexión está IDLE.
    Evita 'there is already a transaction in progress'.
    """
    conn = cur.connection
    status = conn.pgconn.transaction_status
    if status == pq.TransactionStatus.IDLE:
        cur.execute("BEGIN")

def _apply_rls_context(conn: "psycopg.Connection") -> None:
    """
    Transacción + GUCs scopeadas a la TX actual (SET LOCAL via set_config(..., true)).
    Además fija timeouts para evitar cuelgues.
    """
    try:
        user_id, org_id, role = get_context()
    except Exception:
        user_id = org_id = role = None

    with conn.cursor() as cur:
        _begin_if_idle(cur)

        # Timeouts razonables por request:
        #  - 5s por consulta
        #  - 30s si alguien deja la TX abierta por error
        cur.execute("SET LOCAL statement_timeout = 5000")
        cur.execute("SET LOCAL idle_in_transaction_session_timeout = 30000")

        # GUCs LOCAL (true) => se limpian al COMMIT/ROLLBACK
        if org_id is not None:
            cur.execute("SELECT set_config('app.org_id', %s, true)", (str(int(org_id)),))
        if user_id is not None:
            cur.execute("SELECT set_config('app.user_id', %s, true)", (str(int(user_id)),))
        if role is not None:
            cur.execute("SELECT set_config('app.role', %s, true)", (str(role),))

# ---------------------------------------------------------------------
# Pool para HTTP/API
#   - Para Supabase Pooler (6543) mantener chico (3–5).
#   - No te olvides de sslmode=require en el DSN.
# ---------------------------------------------------------------------
pool = None
try:
    if ConnectionPool is not None:
        kwargs = {
            "connect_timeout": 10,
            "sslmode": "require",
        }
        if MAIN_IPV4:
            kwargs["hostaddr"] = MAIN_IPV4

        pool = ConnectionPool(
            conninfo=DSN,
            min_size=int(os.getenv("DB_POOL_MIN", "1")),
            max_size=int(os.getenv("DB_POOL_MAX", "5")),   # ← chico y estable
            max_idle=30,
            timeout=10,                     # espera máx. para obtener una conn del pool
            kwargs=kwargs,                  # pasa a psycopg.connect(...)
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
            _apply_rls_context(conn)
            yield conn
    else:
        if MAIN_IPV4:
            with psycopg.connect(DSN, connect_timeout=10, sslmode="require", hostaddr=MAIN_IPV4) as conn:
                _apply_rls_context(conn)
                yield conn
        else:
            with psycopg.connect(DSN, connect_timeout=10, sslmode="require") as conn:
                _apply_rls_context(conn)
                yield conn

# ---------------------------------------------------------------------
# Conexión dedicada para LISTEN/NOTIFY (no usar en PgBouncer transaction)
# ---------------------------------------------------------------------
@contextmanager
def get_events_conn():
    try:
        if EVENTS_IPV4:
            with psycopg.connect(
                EVENTS_DSN,
                autocommit=True,
                connect_timeout=10,
                sslmode="require",
                hostaddr=EVENTS_IPV4,
            ) as conn:
                yield conn
                return

        with psycopg.connect(EVENTS_DSN, autocommit=True, connect_timeout=10, sslmode="require") as conn:
            yield conn
            return

    except psycopg.OperationalError as e:
        if ("Network is unreachable" in str(e) or "No route to host" in str(e)) and os.getenv("DB_FORCE_IPV4") == "1":
            u = urlparse(EVENTS_DSN)
            host = u.hostname
            try:
                ipv4 = socket.getaddrinfo(host, None, family=socket.AF_INET)[0][4][0]
                with psycopg.connect(
                    EVENTS_DSN,
                    autocommit=True,
                    connect_timeout=10,
                    sslmode="require",
                    hostaddr=ipv4,
                ) as conn:
                    yield conn
                    return
            except Exception:
                pass
        raise
