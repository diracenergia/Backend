# app/services/alarm_events.py
from __future__ import annotations

import os
import json
import logging

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

def _notify(payload: dict):
    try:
        text = json.dumps(payload)
        size = len(text)
        log.info("notify start channel=%s size=%s op=%s keys=%s",
                 CHANNEL, size, payload.get("op"), list(payload.keys()))
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(f'NOTIFY "{CHANNEL}", %s;', (text,))
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
