# app/services/alarm_events.py
import os, json, psycopg
from contextlib import closing

_CHANNEL = os.getenv("ALARM_NOTIFY_CHANNEL", "alarm_events")
_DSN     = os.getenv("DATABASE_URL") or os.getenv("DB_URL")

def publish_alarm_event(payload: dict):
    """Publica un evento en el canal de Postgres para que lo consuma alarm_listener."""
    if not _DSN:
        print("⚠️ publish_alarm_event: falta DATABASE_URL/DB_URL")
        return
    try:
        msg = json.dumps(payload, ensure_ascii=False)
        with closing(psycopg.connect(_DSN)) as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_notify(%s, %s);", (_CHANNEL, msg))
            conn.commit()
        print(f"[alarm-publish] canal={_CHANNEL} payload={msg}")
    except Exception as e:
        print(f"⚠️ publish_alarm_event error: {e}")
