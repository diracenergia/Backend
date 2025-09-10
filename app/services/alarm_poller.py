# app/services/alarm_poller.py
from __future__ import annotations
import os, time, threading, logging, asyncio   # <-- agrega asyncio
from typing import Optional, Dict, Any

from app.core.db import get_conn
# CAMBIA ESTE IMPORT:
# from app.services import notify_alarm
# POR ESTE:
from app.services.notify_alarm import notify_alarm as _notify_alarm
# si preferís, también podés importar notify_ack si lo usás

# ... (resto igual)

def _process_once() -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            select id, asset_type, asset_id, code, severity, message, ts_raised, extra
            from public.alarms
            where telegram = true
              and is_active = true
              and tg_notified_at is null
            order by ts_raised asc
            limit %s
            for update skip locked
        """, (BATCH,))
        rows = cur.fetchall()
        if not rows:
            return 0

        cols = [d[0] for d in cur.description]
        count_ok = 0

        for r in rows:
            a = dict(zip(cols, r))
            try:
                # ⚠️ CORRECCIÓN: usar la función async del módulo notify_alarm
                asyncio.run(_notify_alarm(_row_to_evt(a)))
                cur.execute("update public.alarms set tg_notified_at = now() where id = %s", (a["id"],))
                count_ok += 1
            except Exception as e:
                log.exception("telegram_error alarm_id=%s err=%s", a["id"], e)

        conn.commit()
        return count_ok
