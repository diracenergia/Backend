from fastapi import APIRouter
from app.core.db import get_conn
import json

router = APIRouter()

@router.get("/__test_alarm_notify")
def test_alarm_notify():
    payload = {
        "op":"RAISED","asset_type":"tank","asset_id":1,
        "code":"LEVEL","alarm_id":266,"severity":"CRITICAL","threshold":"very_high"
    }
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_notify('alarm_events', %s)", (json.dumps(payload),))
    return {"ok": True, "sent": payload}
