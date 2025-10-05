# app/routes/kpi.py
from fastapi import APIRouter, Query, HTTPException
from psycopg.rows import dict_row
from app.db import get_conn
from datetime import datetime, timedelta, timezone
from typing import Optional

# Un solo router para todo: /kpi/*
router = APIRouter(prefix="/kpi", tags=["kpi"])

# =========================
# Helpers
# =========================
LOCAL_TZ = "America/Argentina/Buenos_Aires"

def _ft_defaults(date_from: Optional[datetime], date_to: Optional[datetime]):
    """
    Normaliza fechas a UTC aware y aplica defaults:
      - to: ahora (UTC)
      - from: to - 24h
    Valida from < to.
    """
    if date_to is None:
        date_to = datetime.now(timezone.utc)
    if date_from is None:
        date_from = date_to - timedelta(hours=24)

    # Normalizamos a UTC aware
    if date_to.tzinfo is None:
        date_to = date_to.replace(tzinfo=timezone.utc)
    else:
        date_to = date_to.astimezone(timezone.utc)

    if date_from.tzinfo is None:
        date_from = date_from.replace(tzinfo=timezone.utc)
    else:
        date_from = date_from.astimezone(timezone.utc)

    if date_from >= date_to:
        raise HTTPException(status_code=400, detail="'from' debe ser menor que 'to'")
    return date_from, date_to

def _as_float(x):
    return float(x) if x is not None else None

def _as_int(x):
    return int(x) if x is not None else None

def _as_bool(x):
    return bool(x) if x is not None else None

def _compute_alarm(level_pct, low_low, low, high, high_high) -> str:
    # "normal" | "alerta" | "critico"
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

# =========================
# Ping/diagnóstico
# =========================

@router.get("/ping", summary="Ping KPI (sin DB)")
def kpi_ping():
    return {"ok": True, "module": "kpi", "tz": LOCAL_TZ}

# =========================
# ESTADO
# =========================

@router.get("/pumps/status", summary="Estado de bombas (vista kpi.v_pumps_with_status)")
def list_pumps_status():
    sql = """
    select
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
    from kpi.v_pumps_with_status
    order by pump_id
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


@router.get("/tanks/latest", summary="Últimos niveles y config de tanques (kpi.v_tanks_with_config)")
def list_tanks_latest():
    sql = """
    select
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
    from kpi.v_tanks_with_config
    order by tank_id
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

# =========================
# GRÁFICOS (con from/to)
# =========================

@router.get("/graphs/buckets", summary="Devuelve buckets hora local entre from/to (default: últimas 24h)")
def buckets(
    date_from: Optional[datetime] = Query(None, alias="from"),
    date_to:   Optional[datetime] = Query(None, alias="to"),
):
    """Buckets horarios en hora local. Rango **[from,to)**."""
    df, dt = _ft_defaults(date_from, date_to)
    sql = f"""
    WITH bounds AS (
      SELECT %s::timestamptz AS from_utc, %s::timestamptz AS to_utc
    ),
    hours AS (
      SELECT generate_series(
        date_trunc('hour', (from_utc AT TIME ZONE '{LOCAL_TZ}')),
        date_trunc('hour', (to_utc   AT TIME ZONE '{LOCAL_TZ}')) - interval '1 hour',
        interval '1 hour'
      ) AS local_hour_ts
      FROM bounds
    )
    SELECT to_char(local_hour_ts, 'HH24:00') AS local_hour
    FROM hours
    ORDER BY local_hour_ts;
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (df, dt))
        return cur.fetchall()


@router.get("/graphs/pumps/active", summary="Bombas activas por hora en [from,to). Devuelve buckets completos")
def pumps_active(
    date_from: Optional[datetime] = Query(None, alias="from"),
    date_to:   Optional[datetime] = Query(None, alias="to"),
    location_id: Optional[int] = None,
):
    df, dt = _ft_defaults(date_from, date_to)
    sql = f"""
    WITH bounds AS (
      SELECT %s::timestamptz AS from_ts, %s::timestamptz AS to_ts
    ),
    hours AS (
      SELECT generate_series(
        date_trunc('hour', (SELECT from_ts FROM bounds)),
        date_trunc('hour', (SELECT to_ts   FROM bounds)) - interval '1 hour',
        interval '1 hour'
      ) AS hour_utc
    ),
    ev AS (
      SELECT entity_id AS pump_id, ts, value,
             lead(ts) OVER (PARTITION BY entity_id ORDER BY ts) AS next_ts,
             location_id
      FROM kpi.v_kpi_stream
      WHERE kind='pump' AND metric='state'
        AND ts >= (SELECT from_ts FROM bounds) - interval '6 hours'
        AND ts <  (SELECT to_ts   FROM bounds) + interval '6 hours'
        { "AND location_id = %s" if location_id is not None else "" }
    ),
    intervals AS (
      SELECT pump_id, ts AS start_ts, COALESCE(next_ts, now()) AS end_ts
      FROM ev WHERE value = '1' OR value = 1
    ),
    counts AS (
      SELECT h.hour_utc, count(DISTINCT i.pump_id) AS pumps_count
      FROM hours h
      LEFT JOIN intervals i
        ON i.start_ts < h.hour_utc + interval '1 hour'
       AND i.end_ts   > h.hour_utc
      GROUP BY h.hour_utc
    )
    SELECT to_char((hour_utc AT TIME ZONE '{LOCAL_TZ}'), 'HH24:00') AS local_hour,
           COALESCE(pumps_count, 0) AS pumps_count
    FROM counts
    ORDER BY 1;
    """
    params = [df, dt] + ([location_id] if location_id is not None else [])
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


@router.get("/graphs/pumps/starts", summary="Arranques por hora en [from,to), buckets completos")
def pumps_starts(
    date_from: Optional[datetime] = Query(None, alias="from"),
    date_to:   Optional[datetime] = Query(None, alias="to"),
    location_id: Optional[int] = None,
    entity_id:  Optional[int] = None,
):
    df, dt = _ft_defaults(date_from, date_to)
    sql = f"""
    WITH bounds AS (
      SELECT %s::timestamptz AS from_ts, %s::timestamptz AS to_ts
    ),
    hours AS (
      SELECT generate_series(
        date_trunc('hour', (SELECT from_ts FROM bounds)),
        date_trunc('hour', (SELECT to_ts   FROM bounds)) - interval '1 hour',
        interval '1 hour'
      ) AS hour_utc
    ),
    starts AS (
      SELECT date_trunc('hour', ts) AS hour_utc, 1 AS one
      FROM kpi.v_kpi_stream
      WHERE kind='pump' AND metric='state' AND event='start'
        AND ts >= (SELECT from_ts FROM bounds) AND ts < (SELECT to_ts FROM bounds)
        { "AND location_id = %s" if location_id is not None else "" }
        { "AND entity_id   = %s" if entity_id   is not None else "" }
    ),
    agg AS (
      SELECT hour_utc, COALESCE(sum(one),0) AS starts FROM starts GROUP BY hour_utc
    )
    SELECT to_char((h.hour_utc AT TIME ZONE '{LOCAL_TZ}'), 'HH24:00') AS local_hour,
           COALESCE(a.starts,0) AS starts
    FROM hours h
    LEFT JOIN agg a USING (hour_utc)
    ORDER BY 1;
    """
    params: list = [df, dt]
    if location_id is not None: params.append(location_id)
    if entity_id   is not None: params.append(entity_id)
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


@router.get("/graphs/tanks/level_avg", summary="Promedio horario de nivel en [from,to), buckets completos")
def tanks_level_avg(
    date_from: Optional[datetime] = Query(None, alias="from"),
    date_to:   Optional[datetime] = Query(None, alias="to"),
    location_id: Optional[int] = None,
    entity_id:  Optional[int] = None,
):
    df, dt = _ft_defaults(date_from, date_to)
    sql = f"""
    WITH bounds AS (
      SELECT %s::timestamptz AS from_ts, %s::timestamptz AS to_ts
    ),
    hours AS (
      SELECT generate_series(
        date_trunc('hour', (SELECT from_ts FROM bounds)),
        date_trunc('hour', (SELECT to_ts   FROM bounds)) - interval '1 hour',
        interval '1 hour'
      ) AS hour_utc
    ),
    levels AS (
      SELECT date_trunc('hour', ts) AS hour_utc, (value)::float AS val
      FROM kpi.v_kpi_stream
      WHERE kind='tank' AND metric='level_pct'
        AND ts >= (SELECT from_ts FROM bounds) AND ts < (SELECT to_ts FROM bounds)
        { "AND location_id = %s" if location_id is not None else "" }
        { "AND entity_id   = %s" if entity_id   is not None else "" }
    ),
    agg AS (
      SELECT hour_utc, avg(val)::float AS avg_level_pct
      FROM levels
      GROUP BY hour_utc
    )
    SELECT to_char((h.hour_utc AT TIME ZONE '{LOCAL_TZ}'), 'HH24:00') AS local_hour,
           a.avg_level_pct
    FROM hours h
    LEFT JOIN agg a USING (hour_utc)
    ORDER BY 1;
    """
    params: list = [df, dt]
    if location_id is not None: params.append(location_id)
    if entity_id   is not None: params.append(entity_id)
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()
