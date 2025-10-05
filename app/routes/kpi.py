# app/routes/kpi.py
from fastapi import APIRouter, HTTPException, Query
from psycopg.rows import dict_row
from typing import List, Optional
from datetime import datetime

from app.db import get_conn  # mismo helper que usás en tanks.py

router = APIRouter(prefix="/kpi", tags=["kpi"])

def fetch_all(sql: str, params: tuple = ()) -> List[dict]:
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def fetch_one(sql: str, params: tuple = ()) -> Optional[dict]:
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchone()

@router.get("/pumps/status")
def pumps_status() -> List[dict]:
    sql = "SELECT * FROM public.v_pumps_with_status"
    rows = fetch_all(sql)
    # normalizaciones opcionales (ej.: numeric->float) si tu vista devuelve numeric
    for r in rows:
        # si tu vista trae numeric en alguna columna extra, casteá acá
        pass
    return rows

@router.get("/pumps/{pump_id}/status")
def pump_status(pump_id: int) -> dict:
    sql = "SELECT * FROM public.v_pumps_with_status WHERE pump_id = %s"
    row = fetch_one(sql, (pump_id,))
    if not row:
        raise HTTPException(status_code=404, detail="Pump not found")
    return row

@router.get("/tanks/latest")
def tanks_latest() -> List[dict]:
    sql = "SELECT * FROM public.v_tanks_with_config"
    rows = fetch_all(sql)
    # Opcional: castear numeric a float como en tu /tanks/config
    def as_float(x): return float(x) if x is not None else None
    out = []
    for r in rows:
        out.append({
            **r,
            "low_pct":       as_float(r.get("low_pct")),
            "low_low_pct":   as_float(r.get("low_low_pct")),
            "high_pct":      as_float(r.get("high_pct")),
            "high_high_pct": as_float(r.get("high_high_pct")),
            "level_pct":     as_float(r.get("level_pct")),
            "age_sec": int(r["age_sec"]) if r.get("age_sec") is not None else None,
            "online": bool(r["online"]) if r.get("online") is not None else None,
            "alarma": str(r.get("alarma")) if r.get("alarma") is not None else None,
        })
    return out

@router.get("/tanks/{tank_id}/latest")
def tank_latest(tank_id: int) -> dict:
    sql = "SELECT * FROM public.v_tanks_with_config WHERE tank_id = %s"
    r = fetch_one(sql, (tank_id,))
    if not r:
        raise HTTPException(status_code=404, detail="Tank not found")
    def as_float(x): return float(x) if x is not None else None
    return {
        **r,
        "low_pct":       as_float(r.get("low_pct")),
        "low_low_pct":   as_float(r.get("low_low_pct")),
        "high_pct":      as_float(r.get("high_pct")),
        "high_high_pct": as_float(r.get("high_high_pct")),
        "level_pct":     as_float(r.get("level_pct")),
        "age_sec": int(r["age_sec"]) if r.get("age_sec") is not None else None,
        "online": bool(r["online"]) if r.get("online") is not None else None,
        "alarma": str(r.get("alarma")) if r.get("alarma") is not None else None,
    }

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
    rows = fetch_all(sql, (tank_id, date_from, date_to))
    # numeric -> float
    for r in rows:
        if r.get("level_pct") is not None:
            r["level_pct"] = float(r["level_pct"])
    return rows
