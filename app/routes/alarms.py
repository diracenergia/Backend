# app/routes/alarms.py
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
from psycopg.types.json import Json
from app.core.db import get_conn
from app.services.notify_alarm import notify_ack  # ya lo tenés en tu proyecto

router = APIRouter(prefix="/alarms", tags=["alarms"])

class AckIn(BaseModel):
    user: str
    note: Optional[str] = None

@router.get("")
def list_alarms(active: Optional[bool] = True):
    with get_conn() as conn, conn.cursor() as cur:
        if active is None:
            cur.execute("""
                SELECT id, asset_type, asset_id, code, severity, message,
                       ts_raised, ts_cleared, ack_by, ts_ack, is_active
                FROM alarms
                ORDER BY ts_raised DESC
            """)
        else:
            cur.execute("""
                SELECT id, asset_type, asset_id, code, severity, message,
                       ts_raised, ts_cleared, ack_by, ts_ack, is_active
                FROM alarms
                WHERE is_active = %s
                ORDER BY ts_raised DESC
            """, (active,))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]

@router.post("/{alarm_id}/ack")
def ack_alarm(alarm_id: int, body: AckIn, background_tasks: BackgroundTasks):
    with get_conn() as conn, conn.cursor() as cur:
        # marcar ACK solo si sigue activa
        cur.execute("""
            UPDATE alarms
               SET ack_by = %s,
                   ts_ack = COALESCE(ts_ack, now())
             WHERE id = %s
               AND is_active = TRUE
             RETURNING id, asset_type, asset_id, code, severity, message,
                       ts_raised, ts_cleared, ack_by, ts_ack, is_active
        """, (body.user, alarm_id))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Alarma no activa o inexistente")

        cols = [d[0] for d in cur.description]
        alarm_dict = dict(zip(cols, row))

        # auditoría
        asset_type, asset_id, code, severity = row[1], row[2], row[3], row[4]
        asset_label = f"TK-{asset_id}" if asset_type == "tank" else f"PU-{asset_id}"
        cur.execute("""
            INSERT INTO audit_events(
                ts,"user",role,action,asset,details,result,
                domain,asset_type,asset_id,code,severity,state
            )
            VALUES (
                now(), %s, 'operator', 'ALARM', %s,
                %s,
                'ok',
                'ALARM', %s, %s, %s, %s, 'ACKED'
            )
        """, (body.user, asset_label, Json({"note": body.note}),
              asset_type, asset_id, code, severity))

        conn.commit()

    # notificación async (telegram, etc.)
    background_tasks.add_task(notify_ack, alarm_dict, body.user)
    return {"ok": True, "alarm": alarm_dict}
