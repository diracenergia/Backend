# app/services/alarm_events.py
from __future__ import annotations
import os
import json
from typing import Optional, Dict, Any
from app.core.db import get_conn

# Canal de NOTIFY (configurable por env)
_CHANNEL_DEFAULT = "alarm_events"
_CHANNEL = os.getenv("ALARM_NOTIFY_CHANNEL", _CHANNEL_DEFAULT)

def _safe_float(v) -> Optional[float]:
    try:
        return None if v is None else float(v)
    except Exception:
        return None

def _norm_op(op: str) -> str:
    return (op or "").upper()

def _norm_code(code: str) -> str:
    # códigos suelen representarse en mayúsculas
    return (code or "LEVEL").upper()

def _norm_severity(sev: str) -> str:
    # el listener/notify soporta ambos cases; dejamos MAYÚSCULAS aquí
    return (sev or "").upper()

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
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    p: Dict[str, Any] = {
        "op": _norm_op(op),
        "asset_type": str(asset_type),
        "asset_id": int(asset_id),
        "code": _norm_code(code),
        "alarm_id": int(alarm_id),
        "severity": _norm_severity(severity),
        "threshold": str(threshold or ""),
    }
    fv = _safe_float(value)
    if fv is not None:
        p["value"] = fv
    if ts:
        p["ts"] = str(ts)
    if message:
        p["message"] = str(message)
    if isinstance(extra, dict) and extra:
        # No pisamos claves básicas
        for k, v in extra.items():
            if k not in p and v is not None:
                p[k] = v
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
    channel: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Publica un evento en el canal PostgreSQL (LISTEN/NOTIFY),
    con logs previos y posteriores para diagnóstico en Render.

    Args:
        channel: Si querés usar un canal distinto (default: env ALARM_NOTIFY_CHANNEL o "alarm_events")
        extra:   Dict opcional con campos adicionales (p.ej. {"ts_raised": "...", "site": "PLANTA A"})
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
        extra=extra,
    )
    ch = (channel or _CHANNEL)

    try:
        print("[alarms-eval] ➜ NOTIFY", ch + ":", json.dumps(payload, ensure_ascii=False))
        with get_conn() as conn, conn.cursor() as cur:
            # NOTIFY se entrega en COMMIT (explícito por las dudas)
            cur.execute("SELECT pg_notify(%s, %s)", (ch, json.dumps(payload)))
            conn.commit()
        print(f"[alarms-eval] ✓ NOTIFY ok  op={payload['op']} id={payload['alarm_id']} asset={asset_type}-{asset_id} ch={ch}")
    except Exception as e:
        # Log completo del payload para diagnóstico
        print(f"[alarms-eval] ⚠️ NOTIFY failed: {e}  payload={payload} ch={ch}")

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
    channel: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
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
        channel=channel,
        extra=extra,
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
    channel: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
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
        channel=channel,
        extra=extra,
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
    channel: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
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
        channel=channel,
        extra=extra,
    )
