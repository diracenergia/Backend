# app/core/db.py
from __future__ import annotations

import os
import socket
import time
from contextlib import contextmanager
from urllib.parse import urlparse, urlsplit, urlunsplit, parse_qsl, urlencode

from dotenv import load_dotenv
import psycopg
from psycopg import pq
from psycopg.errors import OperationalError

try:
    from psycopg_pool import ConnectionPool  # type: ignore
except Exception:
    ConnectionPool = None  # type: ignore

load_dotenv()

# ---------------------------------------------------------------------
# Helpers básicos
# ---------------------------------------------------------------------
def _clean(v: str | None) -> str:
    return (v or "").strip()

def _main_raw_dsn() -> str:
    return (
        os.getenv("DATABASE_URL")
        or os.getenv("DB_URL")
        or "postgresql://postgres:postgres@localhost:5432/postgres"
    )

def _events_raw_dsn() -> str:
    return os.getenv("EVENTS_DB_URL") or _main_raw_dsn()

def _resolve_ipv4(host: str | None) -> str | None:
    if not host:
        return None
    try:
        return socket.getaddrinfo(host, None, family=socket.AF_INET)[0][4][0]
    except Exception:
        return None

def _add_params_to_dsn(dsn: str) -> str:
    """
    Inyecta en el DSN parámetros críticos para robustez/latencia:
      - sslmode=require, channel_binding=disable
      - connect_timeout (handshake)
      - options: statement_timeout, idle_in_tx_timeout
      - application_name
    Así los timeouts aplican incluso ANTES de entrar a nuestra lógica.
    """
    u = urlsplit(dsn)
    q = dict(parse_qsl(u.query, keep_blank_values=True))

    # Defaults seguros
    q.setdefault("sslmode", "require")
    q.setdefault("channel_binding", "disable")

    # Timeouts
    q["connect_timeout"] = os.getenv("DB_CONNECT_TIMEOUT_SEC", "4")  # handshake
    app_name = os.getenv("DB_APP_NAME", "scada_api")
    q["application_name"] = app_name

    # options (-c ...) -> timeouts de ejecución / sesión
    stmt_ms = os.getenv("DB_STMT_TIMEOUT_MS", "6000")
    idle_ms = os.getenv("DB_IDLE_XACT_TIMEOUT_MS", "6000")
    lock_ms = os.getenv("DB_LOCK_TIMEOUT_MS", "3000")

    opts = q.get("options", "")
    needed = [
        f"-c statement_timeout={stmt_ms}",
        f"-c idle_in_transaction_session_timeout={idle_ms}",
        f"-c lock_timeout={lock_ms}",
    ]
    for opt in needed:
        if opt not in opts:
            opts = (opts + " " + opt).strip()
    q["options"] = opts

    new_q = urlencode(q, doseq=True)
    return urlunsplit((u.scheme, u.netloc, u.path, new_q, u.fragment))


# ---------------------------------------------------------------------
# DSNs (crudos y “afinados”)
# ---------------------------------------------------------------------
RAW_DSN        = _clean(_main_raw_dsn())
RAW_EVENTS_DSN = _clean(_events_raw_dsn())

DSN        = _add_params_to_dsn(RAW_DSN)
EVENTS_DSN = _add_params_to_dsn(RAW_EVENTS_DSN)

if os.getenv("DEBUG_DB_DSN") == "1":
    print(f"[DB] DSN (repr): {DSN!r}")
if os.getenv("DEBUG_EVENTS_DSN") == "1":
    print(f"[DB] EVENTS_DSN (repr): {EVENTS_DSN!r}")

# IPv4 forzado (opcional)
FORCE_IPV4 = os.getenv("DB_FORCE_IPV4") == "1"
MAIN_IPV4   = _resolve_ipv4(urlparse(DSN).hostname)        if FORCE_IPV4 else None
EVENTS_IPV4 = _resolve_ipv4(urlparse(EVENTS_DSN).hostname) if FORCE_IPV4 else None
if FORCE_IPV4:
    print(f"[DB] FORCE_IPV4=1 MAIN_IPV4={MAIN_IPV4} EVENTS_IPV4={EVENTS_IPV4}")

# ---------------------------------------------------------------------
# Tenancy / RLS
# ---------------------------------------------------------------------
try:
    from app.core.tenancy import get_context  # -> (user_id, org_id, role)
except Exception:  # pragma: no cover
    def get_context():
        return (None, None, None)

def _begin_if_idle(cur: "psycopg.Cursor") -> None:
    conn = cur.connection
    status = conn.pgconn.transaction_status
    if status == pq.TransactionStatus.IDLE:
        cur.execute("BEGIN")

def _apply_rls_context(conn: "psycopg.Connection") -> None:
    """
    Aplica contexto por request:
      - BEGIN (si está idle)
      - SET LOCAL statement_timeout (refuerzo)
      - set_config(app.org_id/user_id/role, ..., is_local=true)
    """
    # valores por env (refuerzo del DSN)
    stmt_ms = int(os.getenv("DB_STMT_TIMEOUT_MS", "6000"))

    try:
        user_id, org_id, role = get_context()
    except Exception:
        user_id = org_id = role = None

    with conn.cursor() as cur:
        _begin_if_idle(cur)
        # refuerzo de timeout por si el DSN no se aplicó (o para tests)
        cur.execute("SET LOCAL statement_timeout = %s", (stmt_ms,))

        # GUCs en scope de transacción (seguros con pool transaction)
        if org_id is not None:
            cur.execute("SELECT set_config('app.org_id', %s, true)", (str(int(org_id)),))
        if user_id is not None:
            cur.execute("SELECT set_config('app.user_id', %s, true)", (str(int(user_id)),))
        if role is not None:
            cur.execute("SELECT set_config('app.role', %s, true)", (str(role),))


# ---------------------------------------------------------------------
# Parámetros tunables
# ---------------------------------------------------------------------
DB_DISABLE_POOL    = os.getenv("DB_DISABLE_POOL", "0").lower() in ("1", "true", "yes")
DB_POOL_MIN        = int(os.getenv("DB_POOL_MIN", "1"))
DB_POOL_MAX        = int(os.getenv("DB_POOL_MAX", "5"))         # 5 va bien con Supabase pooler free
# timeout para adquirir una conexión del pool
DB_POOL_ACQ_TIMEOUT= float(os.getenv("DB_POOL_ACQUIRE_TIMEOUT_SEC", os.getenv("DB_POOL_TIMEOUT", "2")))
DB_CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT_SEC", os.getenv("DB_CONNECT_TIMEOUT", "4")))

# TCP keepalives: útiles con poolers / NATs
KEEPALIVES_KW = dict(
    keepalives = int(os.getenv("DB_KEEPALIVES", "1")),
    keepalives_idle = int(os.getenv("DB_KEEPALIVES_IDLE", "30")),
    keepalives_interval = int(os.getenv("DB_KEEPALIVES_INTERVAL", "10")),
    keepalives_count = int(os.getenv("DB_KEEPALIVES_COUNT", "5")),
)

def _connect_kwargs_main() -> dict:
    kw = dict(connect_timeout=DB_CONNECT_TIMEOUT)
    kw.update(KEEPALIVES_KW)
    if MAIN_IPV4:
        kw["hostaddr"] = MAIN_IPV4
    return kw

def _connect_kwargs_events() -> dict:
    kw = dict(connect_timeout=DB_CONNECT_TIMEOUT, autocommit=True)
    kw.update(KEEPALIVES_KW)
    if EVENTS_IPV4:
        kw["hostaddr"] = EVENTS_IPV4
    return kw


# ---------------------------------------------------------------------
# Pool (opcional)
# ---------------------------------------------------------------------
pool = None
if not DB_DISABLE_POOL and ConnectionPool is not None:
    try:
        pool = ConnectionPool(
            conninfo=DSN,
            min_size=DB_POOL_MIN,
            max_size=DB_POOL_MAX,
            max_idle=30,                    # cierra conns ociosas
            timeout=DB_POOL_ACQ_TIMEOUT,    # espera para adquirir conn
            kwargs=_connect_kwargs_main(),  # kwargs a psycopg.connect
        )
    except Exception as e:
        print(f"[DB] psycopg_pool init failed -> {e}")
        pool = None
else:
    if DB_DISABLE_POOL:
        print("[DB] Pool DISABLED by env DB_DISABLE_POOL")
    elif ConnectionPool is None:
        print("[DB] psycopg_pool not installed")


# ---------------------------------------------------------------------
# Conexiones
# ---------------------------------------------------------------------
@contextmanager
def get_conn():
    """
    Conexión para rutas/repos.
    - Usa pool si está disponible.
    - 1 retry corto ante OperationalError al conectar.
    - Aplica RLS y timeouts por request.
    """
    def _yield_connected(conn: "psycopg.Connection"):
        _apply_rls_context(conn)
        return conn

    if pool is not None:
        for attempt in (1, 2):
            try:
                with pool.connection() as conn:
                    yield _yield_connected(conn)
                    return
            except OperationalError as e:
                if attempt == 2:
                    raise
                time.sleep(0.25)
        return

    # sin pool
    for attempt in (1, 2):
        try:
            with psycopg.connect(DSN, **_connect_kwargs_main()) as conn:
                yield _yield_connected(conn)
                return
        except OperationalError:
            if attempt == 2:
                raise
            time.sleep(0.25)


@contextmanager
def get_events_conn():
    """
    Conexión para stream/eventos (autocommit).
    """
    for attempt in (1, 2):
        try:
            with psycopg.connect(EVENTS_DSN, **_connect_kwargs_events()) as conn:
                yield conn
                return
        except OperationalError:
            if attempt == 2:
                raise
            time.sleep(0.5)
