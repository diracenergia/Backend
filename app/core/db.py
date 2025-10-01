# app/core/db.py
from __future__ import annotations

import os
import socket
from contextlib import contextmanager
from urllib.parse import urlparse

from dotenv import load_dotenv
import psycopg

# opcional: para inspeccionar estado de transacción si lo necesitás
try:
    from psycopg import pq  # noqa: F401
except Exception:
    pq = None  # type: ignore

# Pool opcional
try:
    from psycopg_pool import ConnectionPool  # type: ignore
except Exception:
    ConnectionPool = None  # type: ignore

# ------------------------------------------------------------------------------
# .env (Render igual inyecta envs)
# ------------------------------------------------------------------------------
load_dotenv()

# ------------------------------------------------------------------------------
# DSNs
# ------------------------------------------------------------------------------
def _clean(v: str | None) -> str:
    return (v or "").strip()


def _main_raw_dsn() -> str:
    # Prioridad: DATABASE_URL > DB_URL > default local
    return (
        os.getenv("DATABASE_URL")
        or os.getenv("DB_URL")
        or "postgresql://postgres:postgres@localhost:5432/postgres"
    )


def _events_raw_dsn() -> str:
    # Si no hay EVENTS_DB_URL, cae al DSN principal
    return os.getenv("EVENTS_DB_URL") or _main_raw_dsn()


DSN = _clean(_main_raw_dsn())
EVENTS_DSN = _clean(_events_raw_dsn())

if os.getenv("DEBUG_DB_DSN") == "1":
    print(f"[DB] DSN (repr): {DSN!r}")
if os.getenv("DEBUG_EVENTS_DSN") == "1":
    print(f"[DB] EVENTS_DSN (repr): {EVENTS_DSN!r}")

# Detectar si apunta a PgBouncer (pooler de Supabase)
def _is_pooler(dsn: str) -> bool:
    try:
        u = urlparse(dsn)
        host = (u.hostname or "").lower()
        port = (u.port or 0)
        return host.endswith("pooler.supabase.com") or port == 6543
    except Exception:
        return False


IS_POOLER_MAIN = _is_pooler(DSN)
IS_POOLER_EVENTS = _is_pooler(EVENTS_DSN)

# ------------------------------------------------------------------------------
# IPv4 forzado (algunos egress no tienen IPv6)
# ------------------------------------------------------------------------------
FORCE_IPV4 = os.getenv("DB_FORCE_IPV4") in ("1", "true", "yes")


def _resolve_ipv4(host: str | None) -> str | None:
    if not host:
        return None
    try:
        return socket.getaddrinfo(host, None, family=socket.AF_INET)[0][4][0]
    except Exception:
        return None


MAIN_IPV4 = _resolve_ipv4(urlparse(DSN).hostname) if FORCE_IPV4 else None
EVENTS_IPV4 = _resolve_ipv4(urlparse(EVENTS_DSN).hostname) if FORCE_IPV4 else None

if FORCE_IPV4:
    print(f"[DB] FORCE_IPV4=1 MAIN_IPV4={MAIN_IPV4} EVENTS_IPV4={EVENTS_IPV4}")

# ------------------------------------------------------------------------------
# Prepared statements vs PgBouncer
# ------------------------------------------------------------------------------
# Automático: si DSN es pooler, desactivar prepared statements; configurable vía env
_env_disable_prep = os.getenv("DB_DISABLE_PREPARED")
if _env_disable_prep is None:
    DISABLE_PREPARED = IS_POOLER_MAIN  # auto
else:
    DISABLE_PREPARED = _env_disable_prep in ("1", "true", "yes")

if DISABLE_PREPARED:
    print("[DB] prepared statements desactivados (prepare_threshold=None)")

# ------------------------------------------------------------------------------
# Parámetros del pool y conexión
# ------------------------------------------------------------------------------
POOL_MIN = int(os.getenv("DB_POOL_MIN", "0"))
POOL_MAX = int(os.getenv("DB_POOL_MAX", "8"))
POOL_ACQUIRE_TIMEOUT = float(os.getenv("DB_POOL_ACQUIRE_TIMEOUT", "30.0"))
POOL_MAX_IDLE = float(os.getenv("DB_POOL_MAX_IDLE", "30.0"))
CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "10"))

common_kwargs: dict = {"connect_timeout": CONNECT_TIMEOUT}
if FORCE_IPV4 and MAIN_IPV4:
    common_kwargs["hostaddr"] = MAIN_IPV4
# Desactivar prepared statements cuando corresponda
if DISABLE_PREPARED:
    common_kwargs["prepare_threshold"] = None

# Podés sumar un application_name útil para ver en pg_stat_activity
_app_name = os.getenv("DB_APP_NAME", "backend")
common_kwargs["application_name"] = _app_name

# ------------------------------------------------------------------------------
# Contexto multi-tenant (RLS)
# ------------------------------------------------------------------------------
try:
    # Debe devolver (user_id, org_id, role)
    from app.core.tenancy import get_context  # type: ignore
except Exception:  # pragma: no cover
    def get_context():
        return (None, None, None)


def _apply_rls_context(conn: "psycopg.Connection") -> None:
    """
    Settea variables de sesión (GUCs) para RLS.
    Usamos set_config; el tercer parámetro:
      - false  -> a nivel sesión
      - true   -> local a la transacción
    """
    try:
        user_id, org_id, role = get_context()
    except Exception:
        user_id = org_id = role = None

    with conn.cursor() as cur:
        # Si querés que sobreviva a la sesión sin exigir BEGIN:
        if org_id is not None:
            cur.execute("SELECT set_config('app.org_id', %s, false)", (str(int(org_id)),))
        if user_id is not None:
            cur.execute("SELECT set_config('app.user_id', %s, false)", (str(int(user_id)),))
        if role is not None:
            cur.execute("SELECT set_config('app.role', %s, false)", (str(role),))

# ------------------------------------------------------------------------------
# Pool de conexiones
# ------------------------------------------------------------------------------
pool: ConnectionPool | None = None

try:
    if ConnectionPool is not None:
        pool = ConnectionPool(
            conninfo=DSN,
            min_size=POOL_MIN,
            max_size=POOL_MAX,
            max_idle=POOL_MAX_IDLE,
            timeout=POOL_ACQUIRE_TIMEOUT,  # espera para obtener una conexión
            kwargs=common_kwargs,          # pasa a psycopg.connect(...)
        )
        print(f"[DB] ConnectionPool OK (min={POOL_MIN}, max={POOL_MAX}, timeout={POOL_ACQUIRE_TIMEOUT}s)")
except Exception as e:
    print(f"[DB] psycopg_pool no disponible o fallo creando pool: {e}")
    pool = None

# ------------------------------------------------------------------------------
# API pública
# ------------------------------------------------------------------------------
@contextmanager
def get_conn():
    """
    Conexión para operaciones normales.
    Usa pool si está disponible; si no, conexión directa.
    """
    if pool is not None:
        with pool.connection() as conn:
            _apply_rls_context(conn)
            yield conn
    else:
        with psycopg.connect(DSN, **common_kwargs) as conn:
            _apply_rls_context(conn)
            yield conn


@contextmanager
def get_events_conn():
    """
    Conexión dedicada para listeners o flujos especiales.
    Si no la usás, podés ignorarla; no usa pool.
    """
    kwargs = dict(common_kwargs)
    if FORCE_IPV4 and EVENTS_IPV4:
        kwargs["hostaddr"] = EVENTS_IPV4

    # Para casos de LISTEN/NOTIFY reales sería autocommit=True.
    # Como hoy no lo usamos, lo dejamos en default (False).
    with psycopg.connect(EVENTS_DSN, **kwargs) as conn:
        yield conn


def ping() -> bool:
    """Chequeo simple: abre conexión y hace SELECT 1."""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True
    except Exception:
        return False
