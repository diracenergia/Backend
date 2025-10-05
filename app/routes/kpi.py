# app/routes/kpi_graphs.py
from fastapi import APIRouter, Query
from psycopg.rows import dict_row
from app.db import get_conn
from datetime import datetime, timedelta

router = APIRouter(prefix="/kpi/graphs", tags=["kpi-graphs"])
LOCAL_TZ = "America/Argentina/Buenos_Aires"

def _ft_defaults(date_from: datetime | None, date_to: datetime | None):
    if date_to is None: date_to = datetime.utcnow()
    if date_from is None: date_from = date_to - timedelta(hours=24)
    if date_from >= date_to:
        raise ValueError("'from' debe ser menor que 'to'")
    return date_from, date_to

@router.get("/buckets")
def buckets(
    date_from: datetime | None = Query(None, alias="from"),
    date_to:   datetime | None = Query(None, alias="to"),
):
    """Devuelve buckets hora local entre from/to (default: últimas 24h)."""
    df, dt = _ft_defaults(date_from, date_to)
    sql = f"""
    WITH bounds AS (
      SELECT %s::timestamptz AS from_utc, %s::timestamptz AS to_utc
    ),
    hours AS (
      SELECT generate_series(
        date_trunc('hour', (from_utc AT TIME ZONE '{LOCAL_TZ}')),
        date_trunc('hour', (to_utc   AT TIME ZONE '{LOCAL_TZ}')),
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

@router.get("/pumps/active")
def pumps_active(
    date_from: datetime | None = Query(None, alias="from"),
    date_to:   datetime | None = Query(None, alias="to"),
    location_id: int | None = None,
):
    """Bombas activas por hora en [from,to). Devuelve todos los buckets con 0 si no hay actividad."""
    df, dt = _ft_defaults(date_from, date_to)
    sql = f"""
    WITH bounds AS (
      SELECT %s::timestamptz AS from_ts, %s::timestamptz AS to_ts
    ),
    hours AS (
      SELECT generate_series(
        date_trunc('hour', (SELECT from_ts FROM bounds)),
        date_trunc('hour', (SELECT to_ts   FROM bounds)),
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
      FROM ev WHERE value = 1
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

@router.get("/pumps/starts")
def pumps_starts(
    date_from: datetime | None = Query(None, alias="from"),
    date_to:   datetime | None = Query(None, alias="to"),
    location_id: int | None = None,
    entity_id:  int | None = None,
):
    """Arranques por hora en [from,to), con buckets completos."""
    df, dt = _ft_defaults(date_from, date_to)
    sql = f"""
    WITH bounds AS (
      SELECT %s::timestamptz AS from_ts, %s::timestamptz AS to_ts
    ),
    hours AS (
      SELECT generate_series(
        date_trunc('hour', (SELECT from_ts FROM bounds)),
        date_trunc('hour', (SELECT to_ts   FROM bounds)),
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

@router.get("/tanks/level_avg")
def tanks_level_avg(
    date_from: datetime | None = Query(None, alias="from"),
    date_to:   datetime | None = Query(None, alias="to"),
    location_id: int | None = None,
    entity_id:  int | None = None,
):
    """Promedio horario de nivel en [from,to), con buckets completos (null si no hubo lecturas)."""
    df, dt = _ft_defaults(date_from, date_to)
    sql = f"""
    WITH bounds AS (
      SELECT %s::timestamptz AS from_ts, %s::timestamptz AS to_ts
    ),
    hours AS (
      SELECT generate_series(
        date_trunc('hour', (SELECT from_ts FROM bounds)),
        date_trunc('hour', (SELECT to_ts   FROM bounds)),
        interval '1 hour'
      ) AS hour_utc
    ),
    levels AS (
      SELECT date_trunc('hour', ts) AS hour_utc, value::float AS val
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
