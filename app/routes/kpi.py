# app/routes/kpi.py
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from psycopg.rows import dict_row
from psycopg.errors import UndefinedTable, UndefinedColumn
from typing import List, Optional
from datetime import datetime

from app.db import get_conn

router = APIRouter(prefix="/kpi", tags=["kpi"])

# -----------------------
# Utilidades
# -----------------------
def q_all(sql: str, params: tuple = ()) -> List[dict]:
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def q_one(sql: str, params: tuple = ()) -> Optional[dict]:
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchone()

# -----------------------
# 0) Vistas disponibles / definiciones
# -----------------------

@router.get("/views")
def list_views() -> List[dict]:
    """
    Lista si existen las vistas KPI en el esquema public.
    """
    sql = """
    SELECT schemaname, viewname
    FROM pg_catalog.pg_views
    WHERE schemaname = 'public'
      AND viewname IN ('v_pumps_with_status','v_tanks_with_config','v_tank_levels_timeseries')
    ORDER BY viewname
    """
    return q_all(sql)

@router.get("/views/definitions")
def view_definitions() -> List[dict]:
    """
    Devuelve la definición SQL (texto) de las vistas, si existen.
    """
    sql = """
    SELECT viewname, definition
    FROM pg_views
    WHERE schemaname='public'
      AND viewname IN ('v_pumps_with_status','v_tanks_with_config','v_tank_levels_timeseries')
    ORDER BY viewname
    """
    return q_all(sql)

# -----------------------
# 1) Bombas (v_pumps_with_status)
# -----------------------

@router.get("/pumps/status")
def pumps_status() -> List[dict]:
    """
    Trae TODO lo que expone v_pumps_with_status.
    """
    try:
        return q_all("SELECT * FROM public.v_pumps_with_status ORDER BY 1")
    except UndefinedTable:
        raise HTTPException(status_code=404, detail="La vista public.v_pumps_with_status no existe")

@router.get("/pumps/{pump_id}/status")
def pump_status(pump_id: int) -> dict:
    """
    Filtro por ID sobre v_pumps_with_status (devuelve todas las columnas de la vista).
    """
    try:
        row = q_one("SELECT * FROM public.v_pumps_with_status WHERE pump_id = %s", (pump_id,))
    except UndefinedTable:
        raise HTTPException(status_code=404, detail="La vista public.v_pumps_with_status no existe")

    if not row:
        raise HTTPException(status_code=404, detail="Pump not found")
    return row

# -----------------------
# 2) Tanques (v_tanks_with_config)
# -----------------------

@router.get("/tanks/latest")
def tanks_latest() -> List[dict]:
    """
    Trae TODO lo que expone v_tanks_with_config (sin tocar nombres ni tipos).
    """
    try:
        return q_all("SELECT * FROM public.v_tanks_with_config ORDER BY 1")
    except UndefinedTable:
        raise HTTPException(status_code=404, detail="La vista public.v_tanks_with_config no existe")

@router.get("/tanks/{id}/latest")
def tank_latest(id: int) -> dict:
    """
    Filtro por ID. Intentamos primero tank_id; si la vista es legacy y usa pump_id, hacemos fallback.
    Se devuelve la fila tal cual la vista (todas las columnas).
    """
    try:
        row = q_one("SELECT * FROM public.v_tanks_with_config WHERE tank_id = %s", (id,))
    except UndefinedColumn:
        row = None
    except UndefinedTable:
        raise HTTPException(status_code=404, detail="La vista public.v_tanks_with_config no existe")

    if not row:
        # fallback por si la vista usa pump_id como clave
        try:
            row = q_one("SELECT * FROM public.v_tanks_with_config WHERE pump_id = %s", (id,))
        except UndefinedColumn:
            row = None

    if not row:
        raise HTTPException(status_code=404, detail="Tank not found")
    return row

# -----------------------
# 3) Serie temporal (v_tank_levels_timeseries)
# -----------------------

@router.get("/tanks/{tank_id}/levels")
def tank_levels_timeseries(
    tank_id: int,
    date_from: Optional[datetime] = Query(None, alias="from"),
    date_to:   Optional[datetime] = Query(None, alias="to"),
) -> List[dict]:
    """
    Trae TODO lo que expone v_tank_levels_timeseries para un tank_id.
    Si 'from' y 'to' vienen, filtra por 'ts' en ese rango [from, to).
    Devuelve las columnas exactas de la vista.
    """
    try:
        if date_from is not None and date_to is not None:
            if date_from >= date_to:
                raise HTTPException(status_code=400, detail="'from' debe ser menor que 'to'")
            sql = """
              SELECT * FROM public.v_tank_levels_timeseries
              WHERE tank_id = %s AND ts >= %s AND ts < %s
              ORDER BY ts ASC
            """
            return q_all(sql, (tank_id, date_from, date_to))
        else:
            sql = """
              SELECT * FROM public.v_tank_levels_timeseries
              WHERE tank_id = %s
              ORDER BY ts ASC
            """
            return q_all(sql, (tank_id,))
    except UndefinedTable:
        raise HTTPException(status_code=404, detail="La vista public.v_tank_levels_timeseries no existe")
