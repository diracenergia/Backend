# app/core/db.py
from __future__ import annotations

import os
import socket
import logging
from contextlib import contextmanager
from urllib.parse import urlparse
from typing import Iterator, Optional

from dotenv import load_dotenv
import psycopg
from psycopg import pq  # para chequear el estado de transacción

load_dotenv()
log = logging.getLogger("rdls.db")

# =========================
# DSNs y opciones
# =========================
PRIMARY_DSN  = (os.getenv("DB_URL") or os.getenv("DATABASE_URL") or "").strip()
FALLBACK_DSN = (
    os.getenv("DB_URL_FALLBACK")
    or os.getenv("DB_FALLBACK_URL")
    or ""
).strip()

if os.getenv("DEBUG_DB_DSN") == "1":
    print(f"[DB] PRIMARY_DSN (repr): {PRIMARY_DSN!r}")
    print(f"[DB] FALLBACK_DSN (repr): {FALLBACK_DSN!r}")

CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "5"))

# =========================
# IPv4 helper (forzamos IPv4 si existe)
# =========================
def _ipv4_for_host(host: Optional[str]) -> Optional[str]:
    if not host:
        return None
    try:
        return socket.getaddrinfo(host, None, family=socket.AF_INET)[0][4][0]
    except Exception:
        return None

def _hostaddr_for(dsn: str) -> Optional[str]:
    """
    Siempre intentamos resolver IPv4 del host del DSN.
    Si existe, lo devolvemos para pasarlo como hostaddr=... a psycopg,
    evitando intentos IPv6 que en algunos entornos fallan.
    """
    try:
        host = urlparse(dsn).hostname
        return _ipv4_for_host(host)
    except Exception:
        return None

# =========================
# Tenancy / RLS helpers (GUCs)
# =========================
try:
    from app.core.tenancy import get_context  # devuelve (user_id, org_id, role)
except Exception:
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
        if org_id is not None:
            cur.execute("SELECT set_config('app.org_id', %s, false)", (str(int(org_id)),))
        if user_id is not None:
            cur.execute("SELECT set_config('app.user_id', %s, true)", (str(int(user_id)),))
        if role is not None:
            cur.execute("SELECT set_config('app.role', %s, true)", (str(role),))

# =========================
# Conexión sin pool con fallback
# =========================
def _connect(dsn: str) -> psycopg.Connection:
    if not dsn:
        raise RuntimeError("DSN vacío (DB_URL/DATABASE_URL no definido)")
    kw = {"connect_timeout": CONNECT_TIMEOUT}
    ha = _hostaddr_for(dsn)  # intentamos siempre IPv4
    if ha:
        kw["hostaddr"] = ha
        log.info("[db] connect (hostaddr=%s)", ha)
    else:
        log.info("[db] connect -> %s", "pooler" if "pooler" in dsn else "direct")
    return psycopg.connect(dsn, **kw)

@contextmanager
def _connect_with_fallback() -> Iterator[psycopg.Connection]:
    last_err = None
    # 1) PRIMARY
    try:
        conn = _connect(PRIMARY_DSN)
        try:
            yield conn
        finally:
            conn.close()
        return
    except Exception as e:
        last_err = e
        log.error("[db] primary failed: %s", e)

    # 2) FALLBACK (opcional)
    if FALLBACK_DSN:
        try:
            conn = _connect(FALLBACK_DSN)
            try:
                yield conn
            finally:
                conn.close()
            return
        except Exception as e:
            last_err = e
            log.error("[db] fallback failed: %s", e)

    # 3) Nada funcionó
    raise last_err if last_err else RuntimeError("DB connection failed")

# API pública
@contextmanager
def get_conn() -> Iterator[psycopg.Connection]:
    """
    Conexión por request (sin pool). Intenta PRIMARY y luego FALLBACK.
    Aplica contexto RLS (app.*) en la transacción actual.
    """
    with _connect_with_fallback() as conn:
        _apply_rls_context(conn)
        yield conn

# =========================
# Conexión para LISTEN/NOTIFY (opcional)
# =========================
EVENTS_DSN = (os.getenv("EVENTS_DB_URL") or PRIMARY_DSN).strip()

@contextmanager
def get_events_conn():
    """
    Conexión dedicada (autocommit=True) para LISTEN/NOTIFY.
    Sugerido: apuntar a writer 5432 o pooler en modo 'session'.
    """
    try:
        kw = {"autocommit": True, "connect_timeout": CONNECT_TIMEOUT}
        ha = _hostaddr_for(EVENTS_DSN)  # también forzamos IPv4 si está
        if ha:
            kw["hostaddr"] = ha
        with psycopg.connect(EVENTS_DSN, **kw) as conn:
            yield conn
    except Exception as e:
        log.error("[db-events] failed: %s", e)
        raise
