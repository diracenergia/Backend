# app/routes/kpi_graphs.py
from fastapi import APIRouter, Query
from psycopg.rows import dict_row
from app.db import get_conn

router = APIRouter(prefix="/kpi/graphs", tags=["kpi-graphs"])

# Constante de TZ local para bucketing/rotulación
LOCAL_TZ = "America/Argentina/Buenos_Aires"

@router.get("/buckets24h")
def buckets_24h():
    """
    Devuelve 24 buckets locales (HH:00) de las últimas 24 horas.
    shape: [{ local_hour: "HH:00" }, ...]
    """
    sql = f"""
    WITH hours AS (
      SELECT generate_series(
        date_trunc('hour', (now() AT TIME ZONE '{LOCAL_TZ}') - interval '23 hours'),
        date_trunc('hour', (now() AT TIME ZONE '{LOCAL_TZ}')),
        interval '1 hour'
      ) AS local_hour_ts
    )
    SELECT to_char(local_hour_ts, 'HH24:00') AS local_hour
    FROM hours
    ORDER BY local_hour_ts;
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        return cur.fetchall()


@router.get("/pumps/active24h")
def pumps_active_24h(
    location_id: int | None = Query(None, description="Filtra por location_id"),
):
    """
    Bombas activas por hora (últimas 24 h).
    shape: [{ local_hour: 'HH:00', pumps_count: number }, ...]
    """
    sql = f"""
    WITH bounds AS (
      SELECT (now() - interval '24 hours')::timestamptz AS from_ts,
             now()::timestamptz                          AS to_ts
    ),
    ev AS (
      -- Eventos de bombas (state) con siguiente timestamp (para armar intervalos RUN)
      SELECT
        entity_id AS pump_id,
        ts,
        value,
        lead(ts) OVER (PARTITION BY entity_id ORDER BY ts) AS next_ts,
        location_id
      FROM kpi.v_kpi_stream
      WHERE kind='pump' AND metric='state'
        AND ts >= (SELECT from_ts FROM bounds) - interval '6 hours'
        AND ts <  (SELECT to_ts   FROM bounds) + interval '6 hours'
        { "AND location_id = %s" if location_id is not None else "" }
    ),
    intervals AS (
      SELECT pump_id,
             ts AS start_ts,
             COALESCE(next_ts, now()) AS end_ts
      FROM ev
      WHERE value = 1                        -- RUN
    ),
    hours AS (
      SELECT generate_series(
        date_trunc('hour', (SELECT from_ts FROM bounds)),
        date_trunc('hour', (SELECT to_ts   FROM bounds)),
        interval '1 hour'
      ) AS hour_utc
    ),
    expanded AS (
      SELECT h.hour_utc, i.pump_id
      FROM hours h
      JOIN intervals i
        ON i.start_ts < h.hour_utc + interval '1 hour'
       AND i.end_ts   > h.hour_utc
    )
    SELECT
      to_char((hour_utc AT TIME ZONE '{LOCAL_TZ}'), 'HH24:00') AS local_hour,
      count(DISTINCT pump_id) AS pumps_count
    FROM expanded
    GROUP BY 1
    ORDER BY 1;
    """
    params = ([location_id] if location_id is not None else [])
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


@router.get("/pumps/starts24h")
def pumps_starts_24h(
    location_id: int | None = Query(None, description="Filtra por location_id"),
    entity_id: int | None   = Query(None, description="Filtra por pump_id específico"),
):
    """
    Arranques (RUN) por hora (últimas 24 h).
    shape: [{ local_hour: 'HH:00', starts: number }, ...]
    """
    sql = f"""
    SELECT
      to_char(date_trunc('hour', (ts AT TIME ZONE '{LOCAL_TZ}')), 'HH24:00') AS local_hour,
      count(*) AS starts
    FROM kpi.v_kpi_stream
    WHERE kind='pump' AND metric='state' AND event='start'
      AND ts >= now() - interval '24 hours'
      { "AND location_id = %s" if location_id is not None else "" }
      { "AND entity_id   = %s" if entity_id   is not None else "" }
    GROUP BY 1
    ORDER BY 1;
    """
    params: list = []
    if location_id is not None: params.append(location_id)
    if entity_id   is not None: params.append(entity_id)
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


@router.get("/tanks/level_avg24h")
def tanks_level_avg_24h(
    location_id: int | None = Query(None, description="Filtra por location_id"),
    entity_id: int | None   = Query(None, description="Filtra por tank_id específico"),
):
    """
    Promedio horario de nivel de tanques (últimas 24 h).
    shape: [{ local_hour: 'HH:00', avg_level_pct: number }, ...]
    """
    sql = f"""
    SELECT
      to_char(date_trunc('hour', (ts AT TIME ZONE '{LOCAL_TZ}')), 'HH24:00') AS local_hour,
      avg(value)::float AS avg_level_pct
    FROM kpi.v_kpi_stream
    WHERE kind='tank' AND metric='level_pct'
      AND ts >= now() - interval '24 hours'
      { "AND location_id = %s" if location_id is not None else "" }
      { "AND entity_id   = %s" if entity_id   is not None else "" }
    GROUP BY 1
    ORDER BY 1;
    """
    params: list = []
    if location_id is not None: params.append(location_id)
    if entity_id   is not None: params.append(entity_id)
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()
