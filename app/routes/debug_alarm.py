# app/routes/debug_alarm.py
from __future__ import annotations
import os, json, time
from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter
from psycopg import conninfo
from app.core.db import get_events_conn as get_conn

router = APIRouter()

def _mask_user(u: str | None) -> str | None:
    if not u: return u
    if len(u) <= 2: return "*" * len(u)
    return u[0] + "***" + u[-1]

def _now_iso(): return datetime.now(timezone.utc).isoformat()

@router.get("/__alarm_diag")
def alarm_diag():
    out = {}

    # --- estado del listener
    try:
        import app.services.alarm_listener as al
        thr = getattr(al, "_thread", None)
        out["listener"] = {
            "import_ok": True,
            "thread_alive": bool(thr and thr.is_alive()),
        }
    except Exception as e:
        out["listener"] = {"import_ok": False, "error": repr(e)}

    # --- DSN del listener (ENV)
    dsn_env = os.getenv("DB_URL") or ""
    d = {}
    try:
        if dsn_env:
            d = conninfo.conninfo_to_dict(dsn_env)
    except Exception:
        pass
    out["listener_env_dsn"] = {
        "present": bool(dsn_env),
        "host": d.get("host"),
        "port": d.get("port"),
        "dbname": d.get("dbname"),
        "user": _mask_user(d.get("user")),
    }

    # --- DSN de la conexión real usada por el app (get_conn)
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
              SELECT current_database(), current_user,
                     inet_server_addr()::text, inet_server_port()
            """)
            db, usr, host, port = cur.fetchone()
        out["app_db"] = {
            "host": host, "port": str(port), "dbname": db, "user": _mask_user(usr)
        }
    except Exception as e:
        out["app_db"] = {"error": repr(e)}

    # --- comparación rápida
    try:
        same = (
            out.get("listener_env_dsn",{}).get("host") == out.get("app_db",{}).get("host")
            and out.get("listener_env_dsn",{}).get("dbname") == out.get("app_db",{}).get("dbname")
        )
        out["same_db_host"] = same
    except Exception:
        out["same_db_host"] = None

    return out

@router.post("/__alarm_start")
def alarm_start():
    try:
        from app.services.alarm_listener import start_alarm_listener
        start_alarm_listener()
        return {"ok": True, "msg": "listener started"}
    except Exception as e:
        return {"ok": False, "error": repr(e)}

@router.post("/__alarm_stop")
def alarm_stop():
    try:
        from app.services.alarm_listener import stop_alarm_listener
        stop_alarm_listener()
        return {"ok": True, "msg": "listener stopped"}
    except Exception as e:
        return {"ok": False, "error": repr(e)}

@router.get("/__alarm_notify_ping")
def alarm_notify_ping():
    ping = {
        "op": "PING", "asset_type": "test", "asset_id": 0, "code": "PING",
        "alarm_id": int(time.time()*1000), "severity": "INFO",
        "threshold": "n/a", "ts": _now_iso(), "message": "diagnostic ping"
    }
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_notify('alarm_events', %s)", (json.dumps(ping),))
    return {"ok": True, "sent": ping}
