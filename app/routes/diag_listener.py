# app/routes/diag_listener.py
from __future__ import annotations
import os, json, time
from fastapi import APIRouter
from app.core.db import get_conn

router = APIRouter(prefix="/diag", tags=["diag"])

CHAN = os.getenv("ALARM_NOTIFY_CHANNEL", "alarm_events")

@router.get("/listener/status")
def listener_status():
    try:
        from app.services import alarm_listener as al
        alive = bool(getattr(al, "_thread", None) and getattr(al._thread, "is_alive", lambda: False)())
        last = getattr(al, "_last_sent", [])[-5:]
        return {
            "alive": alive,
            "channel": getattr(al, "CHANNEL", None) or getattr(al, "CHAN", None),
            "last_sent_count": len(getattr(al, "_last_sent", [])),
            "last_sent_tail": last,
            "version": getattr(al, "__VERSION__", None),
        }
    except Exception as e:
        return {"alive": False, "error": f"import_error: {e}"}

@router.post("/listener/restart")
def listener_restart():
    try:
        from app.services.alarm_listener import stop_alarm_listener, start_alarm_listener
        stop_alarm_listener()
        time.sleep(0.2)
        start_alarm_listener()
        return {"restarted": True}
    except Exception as e:
        return {"restarted": False, "error": str(e)}

@router.post("/listener/ping")
def listener_ping():
    """Dispara un pg_notify sintético y devuelve lo enviado."""
    payload = {
        "op": "RAISED",
        "asset_type": "tank",
        "asset_id": 9999,
        "code": "TEST_NOTIFY",
        "message": "ping",
        "severity": "info",
        "value": 42.0,
        "threshold": "test",
        "ts_raised": "now",
    }
    text = json.dumps(payload)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_notify(%s, %s)", (CHAN, text))
    return {"ok": True, "channel": CHAN, "payload": payload}
