# app/routes/kpi.py
from fastapi import APIRouter, HTTPException, Query
from app.db import get_conn
from psycopg.rows import dict_row
from datetime import datetime
from typing import List, Optional

router = APIRouter(prefix="/kpi", tags=["kpi"])

# ---------------------------
# Helpers
# ---------------------------

def _as_float(x):
    return float(x) if x is not None else None

def _as_int(x):
    return int(x) if x is not None else None

def _as_bool(x):
    return bool(x) if x is not None else None

def _compute_alarm(level_pct, low_low, low, high, high_high) -> str:
    # Devuelve "normal" | "alerta" | "critico"
    if level_pct is None:
        return "normal"
    low_low   = float(low_low)   if low_low   is not None else 10.0
    low       = float(low)       if low       is not None else 25.0
    high      = float(high)      if high      is not None else 80.0
    high_high = float(high_high) if high_high is not None else 90.0
    x = float(level_pct)
    if x <= low_low or x >= high_high: return "critico"
    if x <= low     or x >= high:      return "alerta"
    return "normal"

# ---------------------------
# PUMPS (v_pumps_with_status)
# ---------------------------

@router.get("/pumps/status")
def list_pumps_status():
    sql = """
    SELECT
      pump_id,
      name,
      location_id,
      location_name,
      state,
      latest_event_id,
      age_sec,
      online,
      event_ts,
      latest_hb_id,
      hb_ts
    FROM public.v_pumps_with_status
    ORDER BY pump_id
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    out = []
    for r in rows:
        out.append({
            "pump_id":         r["pump_id"],
            "name":            r["name"],
            "location_id":     r["location_id"],
            "location_name":   r["location_name"],
            "state":           r["state"],
            "latest_event_id": r["latest_event_id"],
            "age_sec":         _as_int(r.get("age_sec")),
            "online":          _as_bool(r.get("online")),
            "event_ts":        r["event_ts"],
            "latest_hb_id":    r["latest_hb_id"],
            "hb_ts":           r["hb_ts"],
        })
    return out

@router.get("/pumps/{pump_id}/status")
def get_pump_status(pump_id: int):
    sql = """
    SELECT
      pump_id, name, location_id, location_name, state,
      latest_event_id, age_sec, online, event_ts, latest_hb_id, hb_ts
    FROM public.v_pumps_with_status
    WHERE pump_id = %s
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (pump_id,))
        r = cur.fetchone()

    if not r:
        raise HTTPException(status_code=404, detail="Pump not found")

    return {
        "pump_id":         r["pump_id"],
        "name":            r["name"],
        "location_id":     r["location_id"],
        "location_name":   r["location_name"],
        "state":           r["state"],
        "latest_event_id": r["latest_event_id"],
        "age_sec":         _as_int(r.get("age_sec")),
        "online":          _as_bool(r.get("online")),
        "event_ts":        r["event_ts"],
        "latest_hb_id":    r["latest_hb_id"],
        "hb_ts":           r["hb_ts"],
    }

# ---------------------------
# TANKS (v_tanks_with_config)
# ---------------------------

@router.get("/tanks/latest")
def list_tanks_latest():
    sql = """
    SELECT
      tank_id,
      name,
      location_id,
      location_name,
      low_pct,
      low_low_pct,
      high_pct,
      high_high_pct,
      updated_by,
      updated_at,
      level_pct,   -- último nivel
      age_sec,     -- antigüedad (segundos)
      online,      -- true/false (umbral, p.ej. 60s)
      alarma       -- puede venir NULL
    FROM public.v_tanks_with_config
    ORDER BY tank_id
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    out = []
    for r in rows:
        alarm_txt = r.get("alarma")
        if alarm_txt is None:
            alarm_txt = _compute_alarm(
                r.get("level_pct"),
                r.get("low_low_pct"),
                r.get("low_pct"),
                r.get("high_pct"),
                r.get("high_high_pct"),
            )

        out.append({
            "tank_id":        r["tank_id"],
            "name":           r["name"],
            "location_id":    r["location_id"],
            "location_name":  r["location_name"],
            "low_pct":        _as_float(r.get("low_pct")),
            "low_low_pct":    _as_float(r.get("low_low_pct")),
            "high_pct":       _as_float(r.get("high_pct")),
            "high_high_pct":  _as_float(r.get("high_high_pct")),
            "updated_by":     r["updated_by"],
            "updated_at":     r["updated_at"],
            "level_pct":      _as_float(r.get("level_pct")),
            "age_sec":        _as_int(r.get("age_sec")),
            "online":         _as_bool(r.get("online")),
            "alarma":         str(alarm_txt),
        })
    return out

@router.get("/tanks/{tank_id}/latest")
def get_tank_latest(tank_id: int):
    sql = """
    SELECT
      tank_id,
      name,
      location_id,
      location_name,
      low_pct,
      low_low_pct,
      high_pct,
      high_high_pct,
      updated_by,
      updated_at,
      level_pct,
      age_sec,
      online,
      alarma
    FROM public.v_tanks_with_config
    WHERE tank_id = %s
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (tank_id,))
        r = cur.fetchone()

    if not r:
        raise HTTPException(status_code=404, detail="Tank not found")

    alarm_txt = r.get("alarma")
    if alarm_txt is None:
        alarm_txt = _compute_alarm(
            r.get("level_pct"),
            r.get("low_low_pct"),
            r.get("low_pct"),
            r.get("high_pct"),
            r.get("high_high_pct"),
        )

    return {
        "tank_id":        r["tank_id"],
        "name":           r["name"],
        "location_id":    r["location_id"],
        "location_name":  r["location_name"],
        "low_pct":        _as_float(r.get("low_pct")),
        "low_low_pct":    _as_float(r.get("low_low_pct")),
        "high_pct":       _as_float(r.get("high_pct")),
        "high_high_pct":  _as_float(r.get("high_high_pct")),
        "updated_by":     r["updated_by"],
        "updated_at":     r["updated_at"],
        "level_pct":      _as_float(r.get("level_pct")),
        "age_sec":        _as_int(r.get("age_sec")),
        "online":         _as_bool(r.get("online")),
        "alarma":         str(alarm_txt),
    }

# ---------------------------
# Timeseries (v_tank_levels_timeseries)
# ---------------------------

@router.get("/tanks/{tank_id}/levels")
def get_tank_levels(
    tank_id: int,
    date_from: Optional[datetime] = Query(None, alias="from"),
    date_to:   Optional[datetime] = Query(None, alias="to"),
):
    """
    Devuelve la serie de niveles desde v_tank_levels_timeseries.
    Con rango opcional [from, to). Si no se envía rango, devuelve todo el histórico del tanque.
    """
    base = """
      SELECT tank_id, tank_name, level_pct, ts
      FROM public.v_tank_levels_timeseries
      WHERE tank_id = %s
    """
    params: List = [tank_id]

    if date_from is not None and date_to is not None:
        if date_from >= date_to:
            raise HTTPException(status_code=400, detail="'from' debe ser menor que 'to'")
        base += " AND ts >= %s AND ts < %s"
        params.extend([date_from, date_to])

    base += " ORDER BY ts ASC"

    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(base, tuple(params))
        rows = cur.fetchall()

    for r in rows:
        r["level_pct"] = _as_float(r.get("level_pct"))
    return rows
