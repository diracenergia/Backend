# app/services/alarm_events.py
from __future__ import annotations
import os
import json
from typing import Optional, Dict, Any
from app.core.db import get_conn

# Canal LISTEN/NOTIFY (podés cambiarlo por env)
_CHANNEL = os.getenv("ALARM_NOTIFY_CHANNEL", "alarm_events")

def _safe_float(v) -> Optional[float]:
    try:
        return None if v is None else float(v)
    except Exception:
        return None

def _norm(s: Optional[str], *, upper: bool = False, default: str = "") -> str:
    s = s or default
    return s.upper() if upper else s

def _payload(
    *,
    op: str,                   # "RAISED" | "CLEARED" | "ACK"
    asset_type: str,           # "tank" | "pump"
    asset_id: int,
    code: str,                 # p.ej. "LEVEL"
    alarm_id: int,
    severity: str,             # "critical" | "warning" (acepta casing libre)
    threshold: str,            # "very_high" | "high" | "low" | "very_low" | ...
    value: Optional[float] = None,
    ts: Optional[str] = None,
    message: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    p: Dict[str, Any] = {
        "op": _norm(op, upper=True),
        "asset_type": str(asset_type),
        "asset_id": int(asset_id),
        "code": _norm(code or "LEVEL", upper=True),
        "alarm_id": int(alarm_id),
        "severity": _norm(severity, upper=True),
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
    """Publica un evento en PostgreSQL (LISTEN/NOTIFY) con logs de diagnóstico."""
    payload = _payload(
        op=op, asset_type=asset_type, asset_id=asset_id, code=code, alarm_id=alarm_id,
        severity=severity, threshold=threshold, value=value, ts=ts, message=message, extra=extra,
    )
    ch = channel or _CHANNEL
    try:
        print("[alarms-eval] ➜ NOTIFY", ch + ":", json.dumps(payload, ensure_ascii=False))
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_notify(%s, %s)", (ch, json.dumps(payload)))
            conn.commit()  # por si tu get_conn no es autocommit
        print(f"[alarms-eval] ✓ NOTIFY ok  op={payload['op']} id={payload['alarm_id']} asset={asset_type}-{asset_id} ch={ch}")
    except Exception as e:
        print(f"[alarms-eval] ⚠️ NOTIFY failed: {e}  payload={payload} ch={ch}")

# Azúcar:
def publish_raised(**kw) -> None:
    publish_alarm_event("RAISED", **kw)

def publish_cleared(**kw) -> None:
    publish_alarm_event("CLEARED", **kw)

def publish_ack(*, asset_type: str, asset_id: int, code: str, alarm_id: int,
                severity: str, threshold: str, ts: Optional[str] = None,
                user: Optional[str] = None, channel: Optional[str] = None,
                extra: Optional[Dict[str, Any]] = None) -> None:
    msg = f"ACK por {user}" if user else None
    publish_alarm_event("ACK", asset_type=asset_type, asset_id=asset_id, code=code,
                        alarm_id=alarm_id, severity=severity, threshold=threshold,
                        ts=ts, message=msg, channel=channel, extra=extra)
