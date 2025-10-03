from fastapi import APIRouter
import psycopg
from app.core.db import PRIMARY_DSN, FALLBACK_DSN
from urllib.parse import urlparse

router = APIRouter()

def hostport(dsn: str):
    try:
        u = urlparse(dsn)
        return {"host": u.hostname, "port": u.port, "path": u.path}
    except Exception:
        return None

def test_dsn(dsn: str):
    if not dsn:
        return {"ok": False, "error": "empty DSN"}
    try:
        with psycopg.connect(dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("select inet_server_addr(), inet_server_port()")
                a, p = cur.fetchone()
            return {"ok": True, "server_addr": str(a), "server_port": p}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@router.get("/__db_diag_full")
def db_diag_full():
    return {
        "primary": {"dsn": hostport(PRIMARY_DSN),  "result": test_dsn(PRIMARY_DSN)},
        "fallback": {"dsn": hostport(FALLBACK_DSN), "result": test_dsn(FALLBACK_DSN)},
    }
