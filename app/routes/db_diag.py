from fastapi import APIRouter
from app.core.db import get_conn, PRIMARY_DSN, FALLBACK_DSN
from urllib.parse import urlparse

router = APIRouter()

def hostport(dsn: str):
    try:
        u = urlparse(dsn)
        return {"host": u.hostname, "port": u.port, "path": u.path}
    except Exception:
        return None

@router.get("/__db_diag")
def db_diag():
    info = {
        "primary": hostport(PRIMARY_DSN),
        "fallback": hostport(FALLBACK_DSN),
    }
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("select inet_server_addr(), inet_server_port()")
            a, p = cur.fetchone()
        info["connect"] = {"ok": True, "server_addr": str(a), "server_port": p}
    except Exception as e:
        info["connect"] = {"ok": False, "error": str(e)}
    return info
