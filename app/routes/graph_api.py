# app/routes/graph_api.py
from fastapi import APIRouter, HTTPException
from typing import Optional, Literal
from psycopg.rows import dict_row
from psycopg.types.json import Json
from app.core.db import pool
from pydantic import BaseModel

router = APIRouter()

# ----- Topología -----
@router.get("/graph/nodes")
def graph_nodes():
    with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT type, asset_id, name, code FROM v_asset_nodes ORDER BY type, name;")
        return cur.fetchall()

@router.get("/graph/edges")
def graph_edges():
    with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT id, from_type, from_id, from_name, from_code,
                   to_type,   to_id,   to_name,   to_code,
                   pipe_diameter_mm, length_m, is_active
            FROM v_topology_edges
            ORDER BY id;
        """)
        return cur.fetchall()

# ----- Pumps -----
@router.get("/pumps")
def list_pumps():
    with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT id, name FROM pumps ORDER BY id;")
        return cur.fetchall()

@router.get("/pumps/{pump_id}/latest")
def pump_latest(pump_id: int):
    with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM v_pump_latest WHERE pump_id=%s;", (pump_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Pump or reading not found")
        return row

class PumpCommandIn(BaseModel):
    cmd: Literal['START','STOP','AUTO','MAN','SPEED'] | str
    user: Optional[str] = "api"
    speed_pct: Optional[int] = None

@router.post("/pumps/{pump_id}/command")
def pump_command(pump_id: int, body: PumpCommandIn):
    payload = None
    if body.cmd.upper() == "SPEED" and body.speed_pct is not None:
        payload = {"speed_pct": body.speed_pct}
    elif body.cmd.upper() in ("AUTO","MAN"):
        payload = {"mode": "auto" if body.cmd.upper()=="AUTO" else "manual"}

    with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            INSERT INTO pump_commands (pump_id, cmd, payload, requested_by)
            VALUES (%s, %s, %s, %s)
            RETURNING id, pump_id, cmd, status, ts_created;
        """, (pump_id, body.cmd, Json(payload) if payload else None, body.user))
        return cur.fetchone()

# ----- Tanks -----
@router.get("/tanks")
def list_tanks():
    with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT id, name FROM tanks ORDER BY id;")
        return cur.fetchall()

@router.get("/tanks/{tank_id}/latest")
def tank_latest(tank_id: int):
    with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM v_tank_latest WHERE tank_id=%s;", (tank_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Tank or reading not found")
        return row

class TankCommandIn(BaseModel):
    cmd: Literal['SCENARIO','SET_TANK_LEVEL','SET_VALVE'] | str
    user: Optional[str] = "api"
    payload: Optional[dict] = None

@router.post("/tanks/{tank_id}/command")
def tank_command(tank_id: int, body: TankCommandIn):
    with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            INSERT INTO tank_commands (tank_id, cmd, payload, requested_by)
            VALUES (%s, %s, %s, %s)
            RETURNING id, tank_id, cmd, status, ts_created;
        """, (tank_id, body.cmd, Json(body.payload) if body.payload else None, body.user))
        return cur.fetchone()

# ----- Alarms -----
@router.get("/alarms")
def list_alarms(active: Optional[bool] = None):
    with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        if active is True:
            cur.execute("SELECT * FROM alarms WHERE is_active=TRUE ORDER BY ts_raised DESC LIMIT 500;")
        elif active is False:
            cur.execute("SELECT * FROM alarms WHERE is_active=FALSE ORDER BY ts_raised DESC LIMIT 500;")
        else:
            cur.execute("SELECT * FROM alarms ORDER BY ts_raised DESC LIMIT 500;")
        return cur.fetchall()
