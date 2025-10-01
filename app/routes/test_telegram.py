# app/routes/test_telegram.py
from __future__ import annotations
import json, time
from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, Query
from app.core.db import get_conn
from app.core.telegram import send_telegram

router = APIRouter()

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# ✅ Publica NOTIFY -> lo consume alarm_listener -> envía Telegram
@router.get("/__test_alarm_notify")
def test_alarm_notify(
    op: str = Query("RAISED", description="RAISED | CLEARED | ACK"),
    asset_type: str = Query("tank"),
    asset_id: int = Query(1),
    code: str = Query("LEVEL"),
    severity: str = Query("CRITICAL"),
    threshold: str = Query("very_high"),
    value: Optional[float] = Query(None),
    message: Optional[str] = Query("Test desde endpoint"),
    alarm_id: Optional[int] = Query(None, description="Si no se pasa, se genera uno único"),
    ts: Optional[str] = Query(None, description="ISO-8601; si no se pasa se usa ahora UTC"),
):
    # Genera un alarm_id único si no viene, para evitar dedupe del listener
    if alarm_id is None:
        alarm_id = int(time.time() * 1000)

    payload = {
        "op": (op or "RAISED").upper(),
        "asset_type": asset_type,
        "asset_id": int(asset_id),
        "code": (code or "LEVEL").upper(),
        "alarm_id": int(alarm_id),
        "severity": (severity or "CRITICAL").upper(),
        "threshold": threshold,
        "ts": ts or _now_iso(),
    }
    if value is not None:
        payload["value"] = float(value)
    if message:
        payload["message"] = str(message)

    print("[test_alarm] NOTIFY alarm_events:", json.dumps(payload, ensure_ascii=False))
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_notify('alarm_events', %s)", (json.dumps(payload),))
    return {"ok": True, "sent": payload}

# ✅ Envío directo a Telegram (sin listener, útil para aislar problemas)
@router.get("/__ping_telegram")
async def ping_telegram():
    text = "✅ Telegram OK desde Render (test directo)"
    result = await send_telegram(text)
    return {"ok": True, "sent": text, "result": result}
