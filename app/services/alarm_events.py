# app/services/alarm_events.py
from __future__ import annotations
import os, json, logging
from datetime import date, datetime
from decimal import Decimal

# ⬇️ usar SIEMPRE la conexión de eventos (5432 directo)
from app.core.db import get_events_conn as get_conn

__VERSION__ = "ae-2025-09-10T14:10Z"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="ts=%(asctime)s level=%(levelname)s module=%(name)s msg=%(message)s",
)
log = logging.getLogger("alarm-events")

CHANNEL = os.getenv("ALARM_NOTIFY_CHANNEL", "alarm_events")

def _to_jsonable(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat().replace("+00:00", "Z")
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_to_jsonable(v) for v in obj]
    return obj

def _notify(payload: dict):
    """Publica usando SELECT pg_notify(canal, payload) por la conexión de eventos."""
    try:
        safe = _to_jsonable(payload)
        text = json.dumps(safe)
        size = len(text)
        log.info("notify start channel=%s size=%s op=%s keys=%s",
                 CHANNEL, size, safe.get("op"), list(safe.keys()))
        # ⬇️ acá usamos la conexión de eventos (autocommit=True)
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_notify(%s, %s)", (CHANNEL, text))
        log.info("notify done channel=%s size=%s", CHANNEL, size)
    except Exception as e:
        log.exception("notify error err=%s channel=%s", e, CHANNEL)

def publish_raised(asset_type, asset_id, code, message, severity, value, threshold):
    payload = {
        "op": "RAISED",
        "asset_type": asset_type,
        "asset_id": asset_id,
        "code": code,
        "message": message,
        "severity": severity,
        "value": value,
        "threshold": threshold,
        "ts_raised": datetime.utcnow().isoformat() + "Z",
    }
    _notify(payload)

def publish_cleared(asset_type, asset_id, code, message, severity, value, threshold):
    payload = {
        "op": "CLEARED",
        "asset_type": asset_type,
        "asset_id": asset_id,
        "code": code,
        "message": message,
        "severity": severity,
        "value": value,
        "threshold": threshold,
        "ts_cleared": datetime.utcnow().isoformat() + "Z",
    }
    _notify(payload)
