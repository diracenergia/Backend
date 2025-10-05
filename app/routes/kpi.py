# app/routes/kpi.py
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime

from psycopg.rows import dict_row
from psycopg.errors import UndefinedColumn, UndefinedTable

from app.db import get_conn

router = APIRouter(prefix="/kpi", tags=["kpi"])

# ---------------------------
# Helpers
# ---------------------------

def _fetch_all(sql: str, params: tuple = ()) -> List[dict]:
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def _fetch_one(sql: str, params: tuple = ()) -> Optional[dict]:
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchone()

def _as_float(x):
    return float(x) if x is not None else None

def _as_int(x):
    try:
        return int(x) if x is not None else None
    except Exception:
        return None

def _as_bool(x):
    return bool(x) if x is not None else None

# ---------------------------
# PUMPS
# ---------------------------

@router.get("/pumps/status")
def pumps_status() -> List[dict]:
    """
    Devuelve el estado de todas las bombas desde la vista v_pumps_with_status.
    Si faltan columnas numéricas/booleanas las normaliza.
    """
    sql = "SELECT * FROM public.v_pumps_with_status"
    rows = _fetch_all(sql)

    out = []
    for r in rows:
        out.append({
            "pump_id":       r.get("pump_id"),
            "name":          r.get("name"),
            "location_id":   r.get("location_id"),
            "location_name": r.get("location_name"),
            "state":         r.get("state"),
            "latest_event_id": r.get("latest_event_id"),
            "age_sec":       _as_int(r.get("age_sec")),
            "online":        _as_bool(r.get("online")),
            "event_ts":      r.get("event_ts"),
            "latest_hb_id":  r.get("latest_hb_id"),
            "hb_ts":         r.get("hb_ts"),
        })
    return out

@router.get("/pumps/{pump_id}/status")
def pump_status(pump_id: int) -> dict:
    """
    Devuelve el estado de una bomba específica.
    """
    sql = "SELECT * FROM public.v_pumps_with_status WHERE pump_id = %s"
    r = _fetch_one(sql, (pump_id,))
    if not r:
        raise HTTPException(status_code=404, detail="Pump not found")
    return {
        "pump_id":       r.get("pump_id"),
        "name":          r.get("name"),
        "location_id":   r.get("location_id"),
        "location_name": r.get("location_name"),
        "state":         r.get("state"),
        "latest_event_id": r.get("latest_event_id"),
        "age_sec":       _as_int(r.get("age_sec")),
        "online":        _as_bool(r.get("online")),
        "event_ts":      r.get("event_ts"),
        "latest_hb_id":  r.get("latest_hb_id"),
        "hb_ts":         r.get("hb_ts"),
    }

# ---------------------------
# TANKS
# ---------------------------

@router.get("/tanks/latest")
def tanks_latest() -> List[dict]:
    """
    Lee v_tanks_with_config.
    Es tolerante a vistas que hoy traigan 'pump_id' en vez de 'tank_id' (mapea a tank_id).
    """
    sql = "SELECT * FROM public.v_tanks_with_config"
    rows = _fetch_all(sql)

    out = []
    for r in rows:
        # Detectar clave
        tank_id = r.get("tank_id", r.get("pump_id"))
        out.append({
            "tank_id":        tank_id,
            "name":           r.get("name"),
            "location_id":    r.get("location_id"),
            "location_name":  r.get("location_name"),

            "low_pct":        _as_float(r.get("low_pct")),
            "low_low_pct":    _as_float(r.get("low_low_pct")),
            "high_pct":       _as_float(r.get("high_pct")),
            "high_high_pct":  _as_float(r.get("high_high_pct")),
            "updated_by":     r.get("updated_by"),
            "updated_at":     r.get("updated_at"),

            "level_pct":      _as_float(r.get("level_pct")),
            "age_sec":        _as_int(r.get("age_sec")),
            "online":         _as_bool(r.get("online")),
            "alarma":         (str(r["alarma"]) if r.get("alarma") is not None else None),
        })
    return out

@router.get("/tanks/{tank_id}/latest")
def tank_latest(tank_id: int) -> dict:
    """
    Intenta WHERE tank_id; si falla por columna inexistente o no hay fila,
    reintenta WHERE pump_id (para vistas legacy).
    """
    r = None
    try:
        r = _fetch_one("SELECT * FROM public.v_tanks_with_config WHERE tank_id = %s", (tank_id,))
    except UndefinedColumn:
        r = None

    if not r:
        try:
            r = _fetch_one("SELECT * FROM public.v_tanks_with_config WHERE pump_id = %s", (tank_id,))
        except UndefinedColumn:
            r = None

    if not r:
        raise HTTPException(status_code=404, detail="Tank not found")

    return {
        "tank_id":        r.get("tank_id", r.get("pump_id")),
        "name":           r.get("name"),
        "location_id":    r.get("location_id"),
        "location_name":  r.get("location_name"),

        "low_pct":        _as_float(r.get("low_pct")),
        "low_low_pct":    _as_float(r.get("low_low_pct")),
        "high_pct":       _as_float(r.get("high_pct")),
        "high_high_pct":  _as_float(r.get("high_high_pct")),
        "updated_by":     r.get("updated_by"),
        "updated_at":     r.get("updated_at"),

        "level_pct":      _as_float(r.get("level_pct")),
        "age_sec":        _as_int(r.get("age_sec")),
        "online":         _as_bool(r.get("online")),
        "alarma":         (str(r["alarma"]) if r.get("alarma") is not None else None),
    }

@router.get("/tanks/{tank_id}/levels")
def tank_levels_timeseries(
    tank_id: int,
    date_from: datetime = Query(..., alias="from"),
    date_to:   datetime = Query(..., alias="to"),
) -> List[dict]:
    """
    Serie temporal para gráficos.
    Si no existe la vista v_tank_levels_timeseries, hace fallback a tank_ingest + tanks.
    """
    if date_from >= date_to:
        raise HTTPException(status_code=400, detail="'from' must be before 'to'")

    # 1) Intentar la vista
    rows = None
    try:
        sql_view = """
            SELECT tank_id, tank_name, level_pct, ts
            FROM public.v_tank_levels_timeseries
            WHERE tank_id = %s AND ts >= %s AND ts < %s
            ORDER BY ts ASC
        """
        rows = _fetch_all(sql_view, (tank_id, date_from, date_to))
    except UndefinedTable:
        rows = None

    # 2) Fallback a tablas reales si la vista no existe
    if rows is None:
        sql_base = """
            SELECT ti.tank_id,
                   t.name AS tank_name,
                   ti.level_pct,
                   ti.created_at AS ts
            FROM public.tank_ingest ti
            JOIN public.tanks t ON t.id = ti.tank_id
            WHERE ti.tank_id = %s
              AND ti.created_at >= %s
              AND ti.created_at <  %s
            ORDER BY ti.created_at ASC
        """
        rows = _fetch_all(sql_base, (tank_id, date_from, date_to))

    for r in rows:
        r["level_pct"] = _as_float(r.get("level_pct"))
    return rows
