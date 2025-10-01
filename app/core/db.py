# app/core/db.py
from __future__ import annotations

import os
import socket
import logging
from contextlib import contextmanager
from urllib.parse import urlparse

from dotenv import load_dotenv
import psycopg
from psycopg import pq  # para chequear el estado de transacción

# Nota: si usás pool
try:
    from psycopg_pool import ConnectionPool  # type: ignore
except Exception:  # pragma: no cover
    ConnectionPool = None  # type: ignore

log = logging.getLogger("rdls.db")

# ------------------------------------------------------------------------------
# Carga .env desde la raíz del repo (Render también inyecta envs)
# ------------------------------------------------------------------------------
load_dotenv()

# ------------------------------------------------------------------------------
# Helpers DSN
# ------------------------------------------------------------------------------
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


def _ensure_dsn_params(dsn: str) -> str:
    """
    Garantiza parámetros seguros/recomendados para Supabase/Render:
    - sslmode=require
    - channel_binding=disable
    """
    dsn = _clean(dsn)
    # Si viene en URL estilo postgresql://...?... agregá como query params
    glue = "&" if "?" in dsn else "?"
    if "sslmode=" not in dsn:
        dsn += f"{glue}sslmode=require"
        glue = "&"
    if "channel_binding=" not in dsn:
        dsn += f"{glue}channel_binding=disable"
    return dsn


DSN = _ensure_dsn_params(_main_raw_dsn())
EVENTS_DSN = _ensure_dsn_params(_events_raw_dsn())

# Logs opcionales (no exponen secrets salvo que vos lo habilites)
if os.getenv("DEBUG_DB_DSN") == "1":
    print(f"[DB] DSN (repr): {DSN!r}")
if os.getenv("DEBUG_EVENTS_DSN") == "1":
    print(f"[DB] EVENTS_DSN (repr): {EVENTS_DSN!r}")

# ------------------------------------------------------------------------------
# IPv4 forzado (Render sin egress IPv6)
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
# Contexto multi-tenant (org/user/role) para RLS
# ------------------------------------------------------------------------------
# Importamos perezosamente por si aún no existe tenancy.py en dev.
try:
    from app.core.tenancy import get_context  # devuelve (user_id, org_id, role)
except Exception:  # pragma: no cover
    def get_context():
        return (None, None, None)


def _begin_if_idle(cur: "psycopg.Cursor") -> None:
    """
    Abre una transacción solo si la conexión está IDLE.
    Evita 'ERROR: there is already a transaction in progress'.
    """
    conn = cur.connection
    status = conn.pgconn.transaction_status
    if status == pq.TransactionStatus.IDLE:
        cur.execute("BEGIN")
    # si está INTRANS/ACTIVE ya hay TX; si INERROR dejá que el caller maneje.


def _apply_rls_context(conn: "psycopg.Connection") -> None:
    """
    Asegura una transacción abierta y setea variables de sesión
    scopeadas a la TX actual: app.org_id, app.user_id, app.role.
    Usamos set_config(..., ..., true/false) (SET LOCAL no soporta bind params).
    """
    try:
        user_id, org_id, role = get_context()
    except Exception:
        user_id = org_id = role = None

    with conn.cursor() as cur:
        _begin_if_idle(cur)

        # IMPORTANTE: set_config recibe strings
        if org_id is not None:
            cur.execute("SELECT set_config('app.org_id', %s, false)", (str(int(org_id)),))
        if user_id is not None:
            cur.execute("SELECT set_config('app.user_id', %s, true)", (str(int(user_id)),))
        if role is not None:
            cur.execute("SELECT set_config('app.role', %s, true)", (str(role),))


# ------------------------------------------------------------------------------
# Pool para operaciones normales (HTTP/API, repos, etc.)
# ------------------------------------------------------------------------------
pool = None
try:
    if ConnectionPool is not None:
        # Parámetros que pasarán a psycopg.connect(...)
        pool_connect_kwargs = {
            "connect_timeout": 10,
            # Evita prepared statements (pgbouncer transaction pooling)
            "prepare_threshold": 0,
            # Keepalives agresivos (sobre todo en Render free)
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 3,
            # Limitar queries colgados y transacciones idle
            "options": "-c statement_timeout=8000 -c idle_in_transaction_session_timeout=5000",
        }
        if MAIN_IPV4:
            # Fuerza IPv4 explícito en cada conexión del pool
            pool_connect_kwargs["hostaddr"] = MAIN_IPV4

        pool = ConnectionPool(
            conninfo=DSN,
            # Free tier: ser conservador con conexiones simultáneas
            min_size=int(os.getenv("DB_POOL_MIN", "0")),
            max_size=int(os.getenv("DB_POOL_MAX", "4")),
            max_idle=30,
            timeout=10,  # espera máx. para obtener una conexión del pool
            kwargs=pool_connect_kwargs,  # pasa tal cual a psycopg.connect(...)
        )
        print(f"[DB] ConnectionPool OK (min={os.getenv('DB_POOL_MIN','0')}, max={os.getenv('DB_POOL_MAX','4')})")
except Exception as e:
    print(f"[DB] psycopg_pool no disponible o fallo creando pool: {e}")
    pool = None


@contextmanager
def get_conn():
    """
    Conexión para operaciones normales de la app.
    Usa pool si está disponible.
    - Abre una transacción y setea GUCs app.* (RLS/tenancy) en la TX actual.
    - Tus repos pueden usar conn.cursor() y hacer commit/rollback cuando quieran.
    - Si olvidan commit, al cerrar la conexión el driver hace rollback.
    """
    if pool is not None:
        with pool.connection() as conn:
            _apply_rls_context(conn)
            yield conn
    else:
        # Conexión directa (sin pool), con fallback IPv4 si corresponde
        if MAIN_IPV4:
            with psycopg.connect(
                DSN,
                connect_timeout=10,
                hostaddr=MAIN_IPV4,
                prepare_threshold=0,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=3,
                options="-c statement_timeout=8000 -c idle_in_transaction_session_timeout=5000",
            ) as conn:
                _apply_rls_context(conn)
                yield conn
        else:
            with psycopg.connect(
                DSN,
                connect_timeout=10,
                prepare_threshold=0,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=3,
                options="-c statement_timeout=8000 -c idle_in_transaction_session_timeout=5000",
            ) as conn:
                _apply_rls_context(conn)
                yield conn


# ------------------------------------------------------------------------------
# Conexión dedicada para LISTEN/NOTIFY (alarm listener)
# IMPORTANTE: esta conexión NO debe pasar por PgBouncer en modo transaction.
# Apuntá EVENTS_DB_URL a un pooler en *session* o directo :5432 (sslmode=require).
# ------------------------------------------------------------------------------
@contextmanager
def get_events_conn():
    """
    Conexión dedicada para el listener (LISTEN/NOTIFY).
    - autocommit=True para que LISTEN reciba notificaciones.
    - No usa pool.
    - No setea GUCs por defecto (no lo necesitás para escuchar),
      pero podés agregarlos si el listener consulta tablas multi-tenant.
    """
    try:
        if EVENTS_IPV4:
            with psycopg.connect(
                EVENTS_DSN,
                autocommit=True,
                connect_timeout=10,
                hostaddr=EVENTS_IPV4,
                prepare_threshold=0,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=3,
                options="-c statement_timeout=8000 -c idle_in_transaction_session_timeout=5000",
            ) as conn:
                yield conn
                return

        with psycopg.connect(
            EVENTS_DSN,
            autocommit=True,
            connect_timeout=10,
            prepare_threshold=0,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=3,
            options="-c statement_timeout=8000 -c idle_in_transaction_session_timeout=5000",
        ) as conn:
            yield conn
            return

    except psycopg.OperationalError as e:
        # Fallback IPv4 explícito si el host resolvió a IPv6 y la red no lo soporta
        if ("Network is unreachable" in str(e) or "No route to host" in str(e)) and os.getenv("DB_FORCE_IPV4") == "1":
            u = urlparse(EVENTS_DSN)
            host = u.hostname
            try:
                ipv4 = socket.getaddrinfo(host, None, family=socket.AF_INET)[0][4][0]
                with psycopg.connect(
                    EVENTS_DSN,
                    autocommit=True,
                    connect_timeout=10,
                    hostaddr=ipv4,
                    prepare_threshold=0,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=3,
                    options="-c statement_timeout=8000 -c idle_in_transaction_session_timeout=5000",
                ) as conn:
                    yield conn
                    return
            except Exception:
                pass
        # Relevantar la excepción original si no hubo fallback
        raise


# ------------------------------------------------------------------------------
# Healthcheck simple para /health/db
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
