# app/core/db.py
from __future__ import annotations

import os
import socket
from contextlib import contextmanager
from urllib.parse import urlparse

from dotenv import load_dotenv
import psycopg
from psycopg import pq  # para chequear el estado de transacción
# Nota: si usás pool
try:
    from psycopg_pool import ConnectionPool  # type: ignore
except Exception:
    ConnectionPool = None  # type: ignore

# Carga .env desde la raíz del repo (Render también inyecta envs)
load_dotenv()

# -----------------------------
# Helpers DSN
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

# Logs opcionales (no exponen secrets salvo que vos lo habilites)
if os.getenv("DEBUG_DB_DSN") == "1":
    print(f"[DB] DSN (repr): {DSN!r}")
if os.getenv("DEBUG_EVENTS_DSN") == "1":
    print(f"[DB] EVENTS_DSN (repr): {EVENTS_DSN!r}")

# -----------------------------
# IPv4 forzado (Render sin egress IPv6)
# -----------------------------
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

# -----------------------------
# Contexto multi-tenant (org/user/role) para RLS
# -----------------------------
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
    """
    try:
        user_id, org_id, role = get_context()
    except Exception:
        user_id = org_id = role = None

    # Si no hay contexto, igual abrimos TX si hace falta: las policies RLS
    # harán que SELECT devuelva 0 filas y INSERT/UPDATE fallen si corresponden.
    with conn.cursor() as cur:
        _begin_if_idle(cur)
        if org_id is not None:
            cur.execute("SET LOCAL app.org_id = %s", (int(org_id),))
        if user_id is not None:
            cur.execute("SET LOCAL app.user_id = %s", (int(user_id),))
        if role is not None:
            cur.execute("SET LOCAL app.role = %s", (str(role),))

# -----------------------------
# Pool para operaciones normales (HTTP/API, repos, etc.)
# -----------------------------
pool = None
try:
    if ConnectionPool is not None:
        _pool_kwargs: dict = {"connect_timeout": 10}
        if MAIN_IPV4:
            _pool_kwargs["hostaddr"] = MAIN_IPV4

        pool = ConnectionPool(
            conninfo=DSN,
            min_size=1,
            max_size=10,
            max_idle=30,
            timeout=10,             # espera máx. por una conexión del pool
            kwargs=_pool_kwargs,    # kwargs hacia psycopg.connect(...)
        )
except Exception as e:
    print(f"[DB] psycopg_pool no disponible o fallo creando pool: {e}")
    pool = None

@contextmanager
def get_conn():
    """
    Conexión para operaciones normales de la app.
    Usa pool si está disponible.
    - Abre una transacción y setea SET LOCAL app.* (RLS/tenancy).
    - Tus repos pueden usar conn.cursor() y hacer commit/rollback cuando quieran.
    - Si olvidan commit, al cerrar la conexión el driver hace rollback (seguro).
    """
    if pool is not None:
        with pool.connection() as conn:
            _apply_rls_context(conn)
            yield conn
    else:
        if MAIN_IPV4:
            with psycopg.connect(DSN, connect_timeout=10, hostaddr=MAIN_IPV4) as conn:
                _apply_rls_context(conn)
                yield conn
        else:
            with psycopg.connect(DSN, connect_timeout=10) as conn:
                _apply_rls_context(conn)
                yield conn

# -----------------------------
# Conexión dedicada para LISTEN/NOTIFY (alarm listener)
# IMPORTANTE: esta conexión NO debe pasar por PgBouncer en modo transaction.
# Apuntá EVENTS_DB_URL a un pooler en *session* o directo :5432 (sslmode=require).
# -----------------------------
@contextmanager
def get_events_conn():
    """
    Conexión dedicada para el listener (LISTEN/NOTIFY).
    - autocommit=True para que LISTEN reciba notificaciones.
    - No usa pool.
    - No setea SET LOCAL por defecto (no lo necesitás para escuchar),
      pero podés agregarlo si tu listener consulta tablas multi-tenant.
    """
    try:
        if EVENTS_IPV4:
            with psycopg.connect(EVENTS_DSN, autocommit=True, connect_timeout=10, hostaddr=EVENTS_IPV4) as conn:
                yield conn
                return
        with psycopg.connect(EVENTS_DSN, autocommit=True, connect_timeout=10) as conn:
            yield conn
            return
    except psycopg.OperationalError as e:
        # Fallback IPv4 explícito si el host resolvió a IPv6 y la red no lo soporta
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
