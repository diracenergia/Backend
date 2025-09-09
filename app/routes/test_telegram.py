from fastapi import APIRouter
from app.core.db import get_conn
from app.core.telegram import send_telegram
import json

router = APIRouter()

# 🔹 Endpoint que manda un NOTIFY a Postgres (flujo normal con listener)
@router.get("/__test_alarm_notify")
def test_alarm_notify():
    payload = {
        "op": "RAISED",
        "asset_type": "tank",
        "asset_id": 1,
        "code": "LEVEL",
        "alarm_id": 266,
        "severity": "CRITICAL",
        "threshold": "very_high"
    }
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_notify('alarm_events', %s)", (json.dumps(payload),))
    return {"ok": True, "sent": payload}


# 🔹 Endpoint que envía directo a Telegram (para testear sin listener)
@router.get("/__ping_telegram")
async def ping_telegram():
    text = "✅ Telegram OK desde Render (test directo)"
    result = await send_telegram(text)
    return {"ok": True, "sent": text, "result": result}
