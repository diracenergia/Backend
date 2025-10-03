# app/core/db.py
from __future__ import annotations
import os, logging, socket
from contextlib import contextmanager
from typing import Iterator, Optional, List, Tuple
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from dotenv import load_dotenv
import psycopg
from psycopg import pq

load_dotenv()
log = logging.getLogger("rdls.db")

PRIMARY_DSN  = (os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "").strip()
FALLBACK_DSN = (os.getenv("DB_URL_FALLBACK") or os.getenv("DB_FALLBACK_URL") or "").strip()
EVENTS_DSN   = (os.getenv("EVENTS_DB_URL") or PRIMARY_DSN).strip()

CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "15"))
FORCE_IPV4 = (os.getenv("DB_FORCE_IPV4") or "0").strip().lower() in ("1", "true", "yes")

def _dsn_force_ipv4(dsn: str) -> str:
    """
    Si DB_FORCE_IPV4=1, resuelve el hostname a IPv4 y lo inyecta como hostaddr=<ipv4>
    manteniendo host=<fqdn> para que SNI/SSL sigan correctos.
    """
    if not FORCE_IPV4 or not dsn:
        return dsn
    try:
        u = urlparse(dsn)
        if not u.scheme.startswith("postgresql") or not u.hostname:
            return dsn
        port = u.port or 5432
        # Primera A-record (IPv4)
        infos = socket.getaddrinfo(u.hostname, port, socket.AF_INET, socket.SOCK_STREAM)
        ipv4 = infos[0][4][0]
        # rearmar query con hostaddr=<ipv4>
        q = dict(parse_qsl(u.query, keep_blank_values=True))
        q["hostaddr"] = ipv4
        new_q = urlencode(q)
        new_dsn = urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))
        return new_dsn
    except Exception as e:
        log.warning("[db] _dsn_force_ipv4 skip: %s", e)
        return dsn

def _connect_once(dsn: str, *, autocommit: bool = False) -> psycopg.Connection:
    if not dsn:
        raise RuntimeError("DSN vacío (DATABASE_URL/DB_URL no definido)")
    kw = {"connect_timeout": CONNECT_TIMEOUT}
    if autocommit:
        kw["autocommit"] = True
    dsn2 = _dsn_force_ipv4(dsn)
    log.info("[db] try dsn=%s", dsn2.split("@")[-1].split("?")[0])
    return psycopg.connect(dsn2, **kw)

def _candidate_attempts() -> List[Tuple[str, str]]:
    attempts: List[Tuple[str, str]] = []
    if PRIMARY_DSN:
        attempts.append(("primary", PRIMARY_DSN))
    if FALLBACK_DSN:
        attempts.append(("fallback", FALLBACK_DSN))
    return attempts

@contextmanager
def _connect_with_fallback() -> Iterator[psycopg.Connection]:
    last_err: Optional[Exception] = None
    for label, dsn in _candidate_attempts():
        try:
            conn = _connect_once(dsn)
            log.info("[db] connected via %s", label)
            try:
                yield conn
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
            return
        except Exception as e:
            last_err = e
            log.warning("[db] attempt %s failed: %s", label, e)
    raise last_err if last_err else RuntimeError("DB connection failed")

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
    if user_id is None and org_id is None and role is None:
        return
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

@contextmanager
def get_events_conn():
    with _connect_once(EVENTS_DSN, autocommit=True) as conn:
        yield conn
