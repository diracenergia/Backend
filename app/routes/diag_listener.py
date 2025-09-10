# app/routes/diag_listener.py
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services import alarm_events
from app.services import alarm_listener

router = APIRouter(prefix="/diag/listener", tags=["diag-listener"])

class PublishIn(BaseModel):
    op: str = Field(..., pattern="^(RAISED|CLEARED)$")
    asset_type: str = Field(..., pattern="^(tank|pump)$")
    asset_id: int
    code: str
    message: str = ""
    severity: str = Field(..., pattern="^(info|warning|critical)$")
    value: float | None = None
    threshold: str | None = None
    # timestamps opcionales
    ts_raised: datetime | None = None
    ts_cleared: datetime | None = None

def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

@router.post("/publish")
def publish(payload: PublishIn) -> Dict[str, Any]:
    """
    Publica un evento en el canal (SELECT pg_notify) para que el listener
    lo reciba y llame a notify_alarm.send() → Telegram.
    """
    data = payload.model_dump()
    # completar timestamps si faltan
    if payload.op == "RAISED" and not payload.ts_raised:
        data["ts_raised"] = _utcnow_iso()
    if payload.op == "CLEARED" and not payload.ts_cleared:
        data["ts_cleared"] = _utcnow_iso()

    try:
        # Usamos directamente el notificador de eventos
        # (misma ruta que usa alarms_eval)
        alarm_events._notify(data)  # pylint: disable=protected-access
        return {"ok": True, "published": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"publish failed: {e}")

@router.get("/last")
def last_sent() -> Dict[str, Any]:
    """
    Devuelve el cache de últimos envíos que procesó el listener (si hubo).
    Útil para ver qué se despachó a Telegram.
    """
    try:
        cache = getattr(alarm_listener, "_last_sent", [])
        return {
            "alive": bool(getattr(alarm_listener, "_thread", None)
                          and alarm_listener._thread.is_alive()),  # type: ignore[attr-defined]
            "channel": getattr(alarm_listener, "CHANNEL", None)
                       or getattr(alarm_listener, "CHAN", None),
            "count": len(cache),
            "items": cache[-10:],  # últimas 10
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"read cache failed: {e}")

@router.post("/ping")
def ping() -> Dict[str, Any]:
    """
    Envía un RAISED sintético (LOW en tank 1) para prueba rápida.
    """
    data = {
        "op": "RAISED",
        "asset_type": "tank",
        "asset_id": 1,
        "code": "LOW",
        "message": "diag ping",
        "severity": "warning",
        "value": 9.9,
        "threshold": "low",
        "ts_raised": _utcnow_iso(),
    }
    alarm_events._notify(data)  # pylint: disable=protected-access
    return {"ok": True, "published": data, "at": time.time()}
