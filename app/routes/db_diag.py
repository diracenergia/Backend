from fastapi import APIRouter
import time, os, psycopg
from urllib.parse import urlparse
from app.core.db import (
    _connect_once, _candidate_attempts, CONNECT_TIMEOUT
)

router = APIRouter()

def _safe(s: str | None) -> str:
    if not s:
        return ""
    try:
        u = urlparse(s)
        netloc = u.netloc
        if "@" in netloc and ":" in netloc.split("@")[0]:
            user = netloc.split("@")[0].split(":")[0]
            rest = netloc.split("@")[1]
            netloc = f"{user}:[REDACTED]@{rest}"
        return u._replace(netloc=netloc).geturl()
    except Exception:
        return s or ""

@router.get("/__db_diag_full")
def db_diag_full():
    out = {
        "lib": {"psycopg": psycopg.__version__},
        "env": {
            "DB_URL": _safe(os.getenv("DB_URL")),
            "DATABASE_URL": _safe(os.getenv("DATABASE_URL")),
            "DB_URL_FALLBACK": _safe(os.getenv("DB_URL_FALLBACK")),
            "EVENTS_DB_URL": _safe(os.getenv("EVENTS_DB_URL")),
            "DB_CONNECT_TIMEOUT": os.getenv("DB_CONNECT_TIMEOUT") or str(CONNECT_TIMEOUT),
            "DB_FORCE_IPV4": os.getenv("DB_FORCE_IPV4") or "",
        },
        "attempts": []
    }

    for label, dsn in _candidate_attempts():
        parsed = urlparse(dsn)
        meta = {
            "label": label,
            "host": parsed.hostname,
            "port": parsed.port,
            "path": parsed.path,
            "hostaddr": "n/a",  # ya no usamos hostaddr
        }
        t0 = time.time()
        try:
            with _connect_once(dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        select inet_server_addr()::text,
                               inet_server_port(),
                               current_database(),
                               current_user,
                               ssl_is_used()
                    """)
                    a, p, db, u, ssl = cur.fetchone()
            dt = (time.time() - t0) * 1000
            out["attempts"].append({
                **meta, "ok": True,
                "server_addr": a, "server_port": p,
                "db": db, "user": u, "ssl": bool(ssl),
                "duration_ms": round(dt, 1)
            })
        except Exception as e:
            dt = (time.time() - t0) * 1000
            out["attempts"].append({
                **meta, "ok": False, "error": str(e),
                "duration_ms": round(dt, 1)
            })

    return out
