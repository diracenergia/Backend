# app/routes/graph_api.py
from fastapi import APIRouter, HTTPException
from typing import Optional, Literal
from psycopg.rows import dict_row
from psycopg.types.json import Json
from pydantic import BaseModel

# usa el mismo helper que /health/db
from app.core.db import get_conn

router = APIRouter()

# ----- Topología -----
@router.get("/graph/nodes")
def graph_nodes():
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT type, asset_id, name, code
                FROM v_asset_nodes
                ORDER BY type, name;
            """)
            return cur.fetchall()
    except Exception as e:
        # Log visible en Render logs y mensaje claro al cliente
        print(f"[graph_nodes] {e}")
        raise HTTPException(status_code=500, detail=f"graph_nodes failed: {e}")

@router.get("/graph/edges")
def graph_edges():
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT id, from_type, from_id, from_name, from_code,
                       to_type,   to_id,   to_name,   to_code,
                       pipe_diameter_mm, length_m, is_active
                FROM v_topology_edges
                ORDER BY id;
            """)
            return cur.fetchall()
    except Exception as e:
        print(f"[graph_edges] {e}")
        raise HTTPException(status_code=500, detail=f"graph_edges failed: {e}")

# (Opcional) diagnóstico rápido de DB/vistas:
# @router.get("/graph/_diag")
# def graph_diag():
#     try:
#         with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
#             cur.execute("SELECT current_database() AS db, current_schema() AS schema;")
#             info = cur.fetchone()
#             cur.execute("""
#                 SELECT to_regclass('public.v_asset_nodes') AS v_nodes,
#                        to_regclass('public.v_topology_edges') AS v_edges;
#             """)
#             exists = cur.fetchone()
#             return {"ok": True, "db": info, "views": exists}
#     except Exception as e:
#         raise HTTPException(500, f"diag failed: {e}")

# ----- Pumps -----
@router.get("/pumps")
def list_pumps():
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT id, name FROM pumps ORDER BY id;")
        return cur.fetchall()

@router.get("/pumps/{pump_id}/latest")
def pump_latest(pump_id: int):
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
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

    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            INSERT INTO pump_commands (pump_id, cmd, payload, requested_by)
            VALUES (%s, %s, %s, %s)
            RETURNING id, pump_id, cmd, status, ts_created;
        """, (pump_id, body.cmd, Json(payload) if payload else None, body.user))
        return cur.fetchone()

# ----- Tanks -----
@router.get("/tanks")
def list_tanks():
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT id, name FROM tanks ORDER BY id;")
        return cur.fetchall()

@router.get("/tanks/{tank_id}/latest")
def tank_latest(tank_id: int):
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
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
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            INSERT INTO tank_commands (tank_id, cmd, payload, requested_by)
            VALUES (%s, %s, %s, %s)
            RETURNING id, tank_id, cmd, status, ts_created;
        """, (tank_id, body.cmd, Json(body.payload) if body.payload else None, body.user))
        return cur.fetchone()

# ----- Alarms -----
@router.get("/alarms")
def list_alarms(active: Optional[bool] = None):
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        if active is True:
            cur.execute("SELECT * FROM alarms WHERE is_active=TRUE ORDER BY ts_raised DESC LIMIT 500;")
        elif active is False:
            cur.execute("SELECT * FROM alarms WHERE is_active=FALSE ORDER BY ts_raised DESC LIMIT 500;")
        else:
            cur.execute("SELECT * FROM alarms ORDER BY ts_raised DESC LIMIT 500;")
        return cur.fetchall()
