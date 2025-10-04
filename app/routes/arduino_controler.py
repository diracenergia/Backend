# app/routes/arduino_controler.py
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import datetime, timezone

from app.db import get_conn
from psycopg.rows import dict_row

router = APIRouter(prefix="/arduino-controler", tags=["arduino-controler"])

# ==== Schemas ====

class HeartbeatIn(BaseModel):
    pump_id: int
    rssi: Optional[int] = None
    payload: Optional[dict] = None  # diag extra opcional

class StateIn(BaseModel):
    pump_id: int
    state: Literal["run", "stop"]           # estado real del relé
    source: str = "device"                  # 'device' por defecto
    user: Optional[str] = None              # quién operó (si aplica)
    command_id: Optional[int] = None        # enlazar con un comando (opcional)

class CommandOut(BaseModel):
    id: int
    pump_id: int
    action: Literal["start", "stop"]
    status: Literal["pending", "sent", "acked", "failed", "expired"]
    requested_at: datetime
    sent_at: Optional[datetime] = None


# ==== Arduino -> Backend ====

@router.post("/heartbeat")
def push_heartbeat(body: HeartbeatIn):
    """
    Arduino envía heartbeat (nutre v_pump_latest: online/age_sec).
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT 1 FROM public.pumps WHERE id=%s", (body.pump_id,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail="pump not found")

        cur.execute("""
            INSERT INTO public.pump_heartbeat (pump_id, rssi, payload)
            VALUES (%s, %s, %s)
            RETURNING id, created_at
        """, (body.pump_id, body.rssi, body.payload))
        row = cur.fetchone()
        conn.commit()

    return {"ok": True, "hb_id": row["id"], "ts": row["created_at"]}


@router.post("/state")
def push_state(body: StateIn):
    """
    Arduino confirma estado REAL del relé (run/stop).
    Alimenta v_pump_latest (state). Si llega command_id, se marca ACK.
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT 1 FROM public.pumps WHERE id=%s", (body.pump_id,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail="pump not found")

        cur.execute("""
            INSERT INTO public.pump_events (pump_id, state, source, created_by)
            VALUES (%s, %s, %s, %s)
            RETURNING id, created_at
        """, (body.pump_id, body.state, body.source, body.user))
        ev = cur.fetchone()

        if body.command_id is not None:
            cur.execute("""
                UPDATE public.pump_commands
                SET status='acked', acked_at=now()
                WHERE id=%s AND pump_id=%s AND status IN ('pending','sent')
            """, (body.command_id, body.pump_id))

        conn.commit()

    return {"ok": True, "event_id": ev["id"], "state": body.state, "ts": ev["created_at"]}


# ==== Backend -> Arduino ====

@router.get("/next_commands")
def next_commands(
    pump_id: int = Query(..., description="ID de la bomba"),
    limit: int = Query(5, ge=1, le=50, description="Máx. comandos a devolver")
):
    """
    Arduino consulta comandos pendientes. Los 'pending' se marcan 'sent' al entregarlos.
    """
    now = datetime.now(timezone.utc)

    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT id, pump_id, action, status, requested_at, sent_at
            FROM public.pump_commands
            WHERE pump_id=%s AND status IN ('pending','sent')
            ORDER BY requested_at ASC
            LIMIT %s
        """, (pump_id, limit))
        rows = cur.fetchall()

        ids_to_mark = [r["id"] for r in rows if r["status"] == "pending"]
        if ids_to_mark:
            cur.execute("""
                UPDATE public.pump_commands
                SET status='sent', sent_at=%s
                WHERE id = ANY(%s)
            """, (now, ids_to_mark))
            for r in rows:
                if r["id"] in ids_to_mark:
                    r["status"] = "sent"
                    r["sent_at"] = now

        conn.commit()

    # salida simple (dicts); si querés estricta, usa pydantic:
    cmds: List[CommandOut] = [
        CommandOut(
            id=r["id"],
            pump_id=r["pump_id"],
            action=r["action"],
            status=r["status"],
            requested_at=r["requested_at"],
            sent_at=r.get("sent_at"),
        ) for r in rows
    ]
    return {"commands": [c.model_dump() for c in cmds]}
