# app/services/alarm_events.py
from __future__ import annotations
import json
from typing import Optional, Dict, Any
from app.core.db import get_conn

_CHANNEL = "alarm_events"

def _safe_float(v) -> Optional[float]:
    try:
        return None if v is None else float(v)
    except Exception:
        return None

def _payload(
    *,
    op: str,                   # "RAISED" | "CLEARED" | "ACK"
    asset_type: str,           # "tank" | "pump"
    asset_id: int,
    code: str,                 # p.ej. "LEVEL"
    alarm_id: int,
    severity: str,             # "critical" | "warning" | (casing libre)
    threshold: str,            # "very_high" | "high" | "low" | "very_low" | ...
    value: Optional[float] = None,
    ts: Optional[str] = None,
    message: Optional[str] = None,
) -> Dict[str, Any]:
    p: Dict[str, Any] = {
        "op": (op or "").upper(),
        "asset_type": str(asset_type),
        "asset_id": int(asset_id),
        "code": str(code or "LEVEL"),
        "alarm_id": int(alarm_id),
        "severity": (severity or "").upper(),
        "threshold": str(threshold or ""),
    }
    fv = _safe_float(value)
    if fv is not None:
        p["value"] = fv
    if ts:
        p["ts"] = str(ts)
    if message:
        p["message"] = str(message)
    return p

def publish_alarm_event(
    op: str,
    *,
    asset_type: str,
    asset_id: int,
    code: str,
    alarm_id: int,
    severity: str,
    threshold: str,
    value: Optional[float] = None,
    ts: Optional[str] = None,
    message: Optional[str] = None,
) -> None:
    """
    Publica un evento en el canal PostgreSQL 'alarm_events' (LISTEN/NOTIFY),
    con logs previos y posteriores para diagnóstico en Render.
    """
    payload = _payload(
        op=op,
        asset_type=asset_type,
        asset_id=asset_id,
        code=code,
        alarm_id=alarm_id,
        severity=severity,
        threshold=threshold,
        value=value,
        ts=ts,
        message=message,
    )

    try:
        print("[alarms-eval] ➜ NOTIFY", _CHANNEL + ":", json.dumps(payload, ensure_ascii=False))
        with get_conn() as conn, conn.cursor() as cur:
            # pg_notify(channel TEXT, payload TEXT)
            cur.execute("SELECT pg_notify(%s, %s)", (_CHANNEL, json.dumps(payload)))
        print(f"[alarms-eval] ✓ NOTIFY ok  op={payload['op']} id={payload['alarm_id']} asset={asset_type}-{asset_id}")
    except Exception as e:
        print(f"[alarms-eval] ⚠️ NOTIFY failed: {e}  payload={payload}")

# -------- Helpers opcionales (azúcar) --------

def publish_raised(
    *,
    asset_type: str,
    asset_id: int,
    code: str,
    alarm_id: int,
    severity: str,
    threshold: str,
    value: Optional[float] = None,
    ts: Optional[str] = None,
    message: Optional[str] = None,
) -> None:
    publish_alarm_event(
        "RAISED",
        asset_type=asset_type,
        asset_id=asset_id,
        code=code,
        alarm_id=alarm_id,
        severity=severity,
        threshold=threshold,
        value=value,
        ts=ts,
        message=message,
    )

def publish_cleared(
    *,
    asset_type: str,
    asset_id: int,
    code: str,
    alarm_id: int,
    severity: str,
    threshold: str,
    value: Optional[float] = None,
    ts: Optional[str] = None,
    message: Optional[str] = None,
) -> None:
    publish_alarm_event(
        "CLEARED",
        asset_type=asset_type,
        asset_id=asset_id,
        code=code,
        alarm_id=alarm_id,
        severity=severity,
        threshold=threshold,
        value=value,
        ts=ts,
        message=message,
    )

def publish_ack(
    *,
    asset_type: str,
    asset_id: int,
    code: str,
    alarm_id: int,
    severity: str,
    threshold: str,
    ts: Optional[str] = None,
    user: Optional[str] = None,
) -> None:
    msg = f"ACK por {user}" if user else None
    publish_alarm_event(
        "ACK",
        asset_type=asset_type,
        asset_id=asset_id,
        code=code,
        alarm_id=alarm_id,
        severity=severity,
        threshold=threshold,
        ts=ts,
        message=msg,
    )
