# app/services/alarm_events.py
from __future__ import annotations

import os
import json
import logging
from datetime import date, datetime
from decimal import Decimal

from app.core.db import get_conn

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="ts=%(asctime)s level=%(levelname)s module=%(name)s msg=%(message)s",
)
log = logging.getLogger("alarm-events")

CHANNEL = os.getenv("ALARM_NOTIFY_CHANNEL", "alarm_events")
__VERSION__ = "ae-2025-09-10T13:15Z"
log.info("alarm-events loaded file=%s version=%s channel=%s", __file__, __VERSION__, CHANNEL)


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
    """
    Publica a través de SELECT pg_notify(nombre_canal, payload_text).
    Evita NOTIFY con %s (que rompe en psycopg v3) y hace commit explícito.
    """
    try:
        safe = _to_jsonable(payload)
        text = json.dumps(safe)
        size = len(text)

        log.info(
            "notify start channel=%s size=%s op=%s keys=%s",
            CHANNEL, size, safe.get("op"), list(safe.keys())
        )

        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_notify(%s, %s)", (CHANNEL, text))
            try:
                conn.commit()  # por si la conexión no está en autocommit
            except Exception:
                pass

        log.info("notify done channel=%s size=%s", CHANNEL, size)
    except Exception as e:
        log.exception("notify error err=%s channel=%s", e, CHANNEL)


def publish_raised(asset_type, asset_id, code, message, severity, value, threshold, ts_raised):
    payload = {
        "op": "RAISED",
        "asset_type": asset_type,
        "asset_id": asset_id,
        "code": code,
        "message": message,
        "severity": severity,
        "value": value,
        "threshold": threshold,
        "ts_raised": ts_raised,
    }
    log.debug("publish_raised payload=%s", payload)
    _notify(payload)


def publish_cleared(asset_type, asset_id, code, message, severity, value, threshold, ts_cleared):
    payload = {
        "op": "CLEARED",
        "asset_type": asset_type,
        "asset_id": asset_id,
        "code": code,
        "message": message,
        "severity": severity,
        "value": value,
        "threshold": threshold,
        "ts_cleared": ts_cleared,
    }
    log.debug("publish_cleared payload=%s", payload)
    _notify(payload)
