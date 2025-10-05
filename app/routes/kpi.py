# app/routes/kpi.py
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from typing import Any, List, Optional
from datetime import datetime

from app.db import get_conn  # usa tu conexión existente (psycopg/psycopg2)

router = APIRouter(prefix="/kpi", tags=["kpi"])

# -------- Helpers (sync con psycopg) --------
def fetch_all(sql: str, params: tuple = ()) -> List[dict]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

def fetch_one(sql: str, params: tuple = ()) -> Optional[dict]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))

# -------- Endpoints --------

# 1) Bombas con estado actual (todas)
@router.get("/pumps/status")
def pumps_status() -> List[dict]:
    sql = "SELECT * FROM public.v_pumps_with_status"
    return fetch_all(sql)

# 1.b) Bomba específica por id
@router.get("/pumps/{pump_id}/status")
def pump_status(pump_id: int) -> dict:
    sql = "SELECT * FROM public.v_pumps_with_status WHERE pump_id = %s"
    row = fetch_one(sql, (pump_id,))
    if not row:
        raise HTTPException(status_code=404, detail="Pump not found")
    return row

# 2) Tanques con config y último nivel (todos)
@router.get("/tanks/latest")
def tanks_latest() -> List[dict]:
    sql = "SELECT * FROM public.v_tanks_with_config"
    return fetch_all(sql)

# 2.b) Tanque específico
@router.get("/tanks/{tank_id}/latest")
def tank_latest(tank_id: int) -> dict:
    sql = "SELECT * FROM public.v_tanks_with_config WHERE tank_id = %s"
    row = fetch_one(sql, (tank_id,))
    if not row:
        raise HTTPException(status_code=404, detail="Tank not found")
    return row

# 3) Serie temporal para gráficos (opcional)
@router.get("/tanks/{tank_id}/levels")
def tank_levels_timeseries(
    tank_id: int,
    date_from: datetime = Query(..., alias="from"),
    date_to:   datetime = Query(..., alias="to"),
) -> List[dict]:
    if date_from >= date_to:
        raise HTTPException(status_code=400, detail="'from' must be before 'to'")
    sql = """
        SELECT tank_id, tank_name, level_pct, ts
        FROM public.v_tank_levels_timeseries
        WHERE tank_id = %s AND ts >= %s AND ts < %s
        ORDER BY ts ASC
    """
    return fetch_all(sql, (tank_id, date_from, date_to))
