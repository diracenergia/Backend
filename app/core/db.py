# app/core/db.py
from __future__ import annotations

import os
import socket
import logging
from contextlib import contextmanager
from urllib.parse import urlparse

from dotenv import load_dotenv
import psycopg
from psycopg import pq  # para chequear estado de transacción

# ------------------------------------------------------------------------------
# Pool (psycopg_pool)
# ------------------------------------------------------------------------------
try:
    from psycopg_pool import ConnectionPool, PoolTimeout  # type: ignore
except Exception:  # pragma: no cover
    ConnectionPool = None  # type: ignore
    class PoolTimeout(Exception):  # fallback por si no está instalado
        pass

log = logging.getLogger("rdls.db")

# ------------------------------------------------------------------------------
# Carga .env (en Render las envs ya vienen inyectadas)
# ------------------------------------------------------------------------------
load_dotenv()

# ------------------------------------------------------------------------------
# Helpers DSN
# ------------------------------------------------------------------------------
def _clean(v: str | None) -> str:
    # Quita espacios/saltos (\n, \r) que rompen psycopg (p.ej. "require\n")
    return (v or "").strip()


def _main_raw_dsn() -> str:
    # Prioridad: DATABASE_URL > DB_URL > default local
    return os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "postgresql://postgres:postgres@localhost:5432/munirdls"


def _events_raw_dsn() -> str:
    # DSN para LISTEN/NOTIFY; si no está, usa el principal
    return os.getenv("EVENTS_DB_URL") or _main_raw_dsn()


def _ensure_dsn_params(dsn: str) -> str:
    """
    Garantiza parámetros compatibles con Supabase/Render:
    - sslmode=require
    - channel_binding=disable
    """
    dsn = _clean(dsn)
    glue = "&" if "?" in dsn else "?"
    if "sslmode=" not in dsn:
        dsn += f"{glue}sslmode=require"
        glue = "&"
    if "channel_binding=" not in dsn:
        dsn += f"{glue}channel_binding=disable"
    return dsn


DSN = _ensure_dsn_params(_main_raw_dsn())
EVENTS_DSN = _ensure_dsn_params(_events_raw_dsn())

# Debug opcional (no imprimir secrets a menos que vos lo pidas)
if os.getenv("DEBUG_DB_DSN") == "1":
    print(f"[DB] DSN (repr): {DSN!r}")
if os.getenv("DEBUG_EVENTS_DSN") == "1":
    print(f"[DB] EVENTS_DSN (repr): {EVENTS_DSN!r}")

# ------------------------------------------------------------------------------
# IPv4 forzado (útil en Render si hay egress IPv6 raro)
# ------------------------------------------------------------------------------
FORCE_IPV4 = os.getenv("DB_FORCE_IPV4") == "1"


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
# Contexto multi-tenant (RLS)
# ------------------------------------------------------------------------------
# Import perezoso por si en dev aún no existe tenancy.py
try:
    from app.core.tenancy import get_context  # -> (user_id, org_id, role)
except Exception:  # pragma: no cover
    def get_context():
        return (None, None, None)


def _begin_if_idle(cur: "psycopg.Cursor") -> None:
    """Abre una transacción solo si la conexión está IDLE."""
    conn = cur.connection
    status = conn.pgconn.transaction_status
    if status == pq.TransactionStatus.IDLE:
        cur.execute("BEGIN")


def _apply_rls_context(conn: "psycopg.Connection") -> None:
    """
    Setea variables de sesión (GUC) en la TX actual:
      - app.org_id, app.user_id, app.role
    """
    try:
        user_id, org_id, role = get_context()
    except Exception:
        user_id = org_id = role = None

    with conn.cursor() as cur:
        _begin_if_idle(cur)
        if org_id is not None:
            cur.execute("SELECT set_config('app.org_id', %s, false)", (str(int(org_id)),))
        if user_id is not None:
            cur.execute("SELECT set_config('app.user_id', %s, true)", (str(int(user_id)),))
        if role is not None:
            cur.execute("SELECT set_config('app.role', %s, true)", (str(role),))

# ------------------------------------------------------------------------------
# Pool global
# ------------------------------------------------------------------------------
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", "0"))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "8"))          # conservador para free tier
POOL_TIMEOUT = float(os.getenv("DB_POOL_TIMEOUT", "30"))  # tiempo máx. para obtener conn del pool

pool: ConnectionPool | None = None
try:
    if ConnectionPool is not None:
        pool_connect_kwargs = {
            "connect_timeout": 10,
            # ⚠ CLAVE para PgBouncer (transaction pooling)
            "prepare_threshold": 0,
            # keepalives (evitar cortes silenciosos)
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 3,
            # cortar queries colgadas / idle in transaction
            "options": "-c statement_timeout=8000 -c idle_in_transaction_session_timeout=5000",
        }
        if MAIN_IPV4:
            pool_connect_kwargs["hostaddr"] = MAIN_IPV4

        pool = ConnectionPool(
            conninfo=DSN,
            min_size=DB_POOL_MIN,
            max_size=DB_POOL_MAX,
            max_idle=30,
            max_lifetime=1800,   # recicla conexiones cada 30 minutos
            timeout=POOL_TIMEOUT,
            kwargs=pool_connect_kwargs,
        )
        print(f"[DB] ConnectionPool OK (min={DB_POOL_MIN}, max={DB_POOL_MAX}, timeout={POOL_TIMEOUT}s)")
except Exception as e:
    print(f"[DB] psycopg_pool no disponible o fallo creando pool: {e}")
    pool = None

# ------------------------------------------------------------------------------
# Obtener conexión (con pool + fallback)
# ------------------------------------------------------------------------------
@contextmanager
def get_conn():
    """
    Devuelve una conexión para operaciones normales.
    - Usa el pool global si está disponible.
    - Aplica GUCs de RLS en la transacción actual.
    - Fallback a conexión directa si el pool está saturado (pico transitorio).
    """
    if pool is not None:
        try:
            with pool.connection(timeout=POOL_TIMEOUT) as conn:
                _apply_rls_context(conn)
                yield conn
                return
        except PoolTimeout:
            # Fallback de emergencia para evitar 500 en ráfagas cortas
            extra = {"hostaddr": MAIN_IPV4} if MAIN_IPV4 else {}
            with psycopg.connect(
                DSN,
                connect_timeout=10,
                prepare_threshold=0,  # ⚠ evita prepared statements con PgBouncer
                keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3,
                options="-c statement_timeout=8000 -c idle_in_transaction_session_timeout=5000",
                **extra,
            ) as conn:
                _apply_rls_context(conn)
                yield conn
                return

    # Sin pool (o falló al crear): conexión directa
    extra = {"hostaddr": MAIN_IPV4} if MAIN_IPV4 else {}
    with psycopg.connect(
        DSN,
        connect_timeout=10,
        prepare_threshold=0,
        keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3,
        options="-c statement_timeout=8000 -c idle_in_transaction_session_timeout=5000",
        **extra,
    ) as conn:
        _apply_rls_context(conn)
        yield conn

# ------------------------------------------------------------------------------
# Conexión dedicada para LISTEN/NOTIFY
#  - NO pasar por PgBouncer en modo transaction.
#  - Usar EVENTS_DB_URL apuntando a :5432 (directo) o pooler en modo session.
# ------------------------------------------------------------------------------
@contextmanager
def get_events_conn():
    """
    Conexión dedicada al listener (LISTEN/NOTIFY).
    - autocommit=True para recibir notificaciones.
    - prepare_threshold=0 por consistencia (y por si se hacen consultas).
    - No setea GUCs por defecto (no suele hacer falta para escuchar).
    """
    extra = {"hostaddr": EVENTS_IPV4} if EVENTS_IPV4 else {}
    try:
        with psycopg.connect(
            EVENTS_DSN,
            autocommit=True,
            connect_timeout=10,
            prepare_threshold=0,
            keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3,
            options="-c statement_timeout=8000 -c idle_in_transaction_session_timeout=5000",
            **extra,
        ) as conn:
            yield conn
            return
    except psycopg.OperationalError as e:
        # Intento de fallback IPv4 si la red no admite IPv6 y no pudimos resolver antes
        if ("Network is unreachable" in str(e) or "No route to host" in str(e)) and os.getenv("DB_FORCE_IPV4") == "1":
            u = urlparse(EVENTS_DSN)
            host = u.hostname
            if host:
                try:
                    ipv4 = socket.getaddrinfo(host, None, family=socket.AF_INET)[0][4][0]
                    with psycopg.connect(
                        EVENTS_DSN,
                        autocommit=True,
                        connect_timeout=10,
                        hostaddr=ipv4,
                        prepare_threshold=0,
                        keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3,
                        options="-c statement_timeout=8000 -c idle_in_transaction_session_timeout=5000",
                    ) as conn:
                        yield conn
                        return
                except Exception:
                    pass
        raise

# ------------------------------------------------------------------------------
# Healthcheck simple (para /health/db)
# ------------------------------------------------------------------------------
def db_health_ok() -> bool:
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("select 1")
            cur.fetchone()
        return True
    except Exception as e:
        log.warning("DB health check failed: %s", e)
        return False
