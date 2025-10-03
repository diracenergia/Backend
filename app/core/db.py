# app/core/db.py
from __future__ import annotations
import os, socket, logging
from contextlib import contextmanager
from urllib.parse import urlparse
from typing import Iterator, Optional
from dotenv import load_dotenv
import psycopg
from psycopg import pq

load_dotenv()
log = logging.getLogger("rdls.db")

PRIMARY_DSN  = (os.getenv("DB_URL") or os.getenv("DATABASE_URL") or "").strip()
FALLBACK_DSN = (os.getenv("DB_URL_FALLBACK") or os.getenv("DB_FALLBACK_URL") or "").strip()

CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "5"))
FORCE_IPV4      = os.getenv("DB_FORCE_IPV4") == "1"

# 👇 NUEVO: permite fijar IP manual
DB_HOSTADDR       = (os.getenv("DB_HOSTADDR") or "").strip() or None
EVENTS_HOSTADDR   = (os.getenv("EVENTS_HOSTADDR") or "").strip() or None
FALLBACK_HOSTADDR = (os.getenv("DB_HOSTADDR_FALLBACK") or "").strip() or None

def _ipv4_for_host(host: Optional[str]) -> Optional[str]:
    if not host:
        return None
    try:
        return socket.getaddrinfo(host, None, family=socket.AF_INET)[0][4][0]
    except Exception:
        return None

def _auto_ipv4_from_dsn(dsn: str) -> Optional[str]:
    if not (dsn and FORCE_IPV4):
        return None
    host = urlparse(dsn).hostname
    return _ipv4_for_host(host)

def _connect(dsn: str, hostaddr_override: Optional[str] = None) -> psycopg.Connection:
    if not dsn:
        raise RuntimeError("DSN vacío (DB_URL/DATABASE_URL no definido)")
    kw = {"connect_timeout": CONNECT_TIMEOUT}
    ha = hostaddr_override or _auto_ipv4_from_dsn(dsn)
    if ha:
        kw["hostaddr"] = ha
        log.info("[db] connect hostaddr=%s", ha)
    else:
        log.info("[db] connect host=%s", urlparse(dsn).hostname)
    return psycopg.connect(dsn, **kw)

@contextmanager
def _connect_with_fallback() -> Iterator[psycopg.Connection]:
    last_err = None
    # 1) PRIMARY
    try:
        conn = _connect(PRIMARY_DSN, hostaddr_override=DB_HOSTADDR)
        try:
            yield conn
        finally:
            conn.close()
        return
    except Exception as e:
        last_err = e
        log.error("[db] primary failed: %s", e)

    # 2) FALLBACK
    if FALLBACK_DSN:
        try:
            conn = _connect(FALLBACK_DSN, hostaddr_override=FALLBACK_HOSTADDR or DB_HOSTADDR)
            try:
                yield conn
            finally:
                conn.close()
            return
        except Exception as e:
            last_err = e
            log.error("[db] fallback failed: %s", e)

    raise last_err if last_err else RuntimeError("DB connection failed")

# ----- RLS context (igual que tenías) -----
try:
    from app.core.tenancy import get_context
except Exception:
    def get_context():
        return (None, None, None)

def _begin_if_idle(cur: "psycopg.Cursor") -> None:
    if cur.connection.pgconn.transaction_status == pq.TransactionStatus.IDLE:
        cur.execute("BEGIN")

def _apply_rls_context(conn: "psycopg.Connection") -> None:
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

@contextmanager
def get_conn() -> Iterator[psycopg.Connection]:
    with _connect_with_fallback() as conn:
        _apply_rls_context(conn)
        yield conn

# ---- Events (LISTEN/NOTIFY) ----
EVENTS_DSN = (os.getenv("EVENTS_DB_URL") or PRIMARY_DSN).strip()

@contextmanager
def get_events_conn():
    kw = {"autocommit": True, "connect_timeout": CONNECT_TIMEOUT}
    ha = EVENTS_HOSTADDR or _auto_ipv4_from_dsn(EVENTS_DSN)
    if ha:
        kw["hostaddr"] = ha
        log.info("[db-events] hostaddr=%s", ha)
    with psycopg.connect(EVENTS_DSN, **kw) as conn:
        yield conn
