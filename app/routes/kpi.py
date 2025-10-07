# app/routes/kpi.py
from fastapi import APIRouter, Query
from psycopg.rows import dict_row
from datetime import datetime, timezone, timedelta
from app.db import get_conn

router = APIRouter(prefix="/kpi", tags=["kpi"])
TZ = "America/Argentina/Buenos_Aires"

def _range_24h(from_iso: str | None, to_iso: str | None) -> tuple[str, str]:
    if from_iso and to_iso:
        return from_iso, to_iso
    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(hours=24)
    return from_dt.replace(microsecond=0).isoformat(), to_dt.replace(microsecond=0).isoformat()

# ------------------------ Buckets HH:00 -------------------------
@router.get("/graphs/buckets")
def graph_buckets(from_: str | None = Query(None, alias="from"), to: str | None = None):
    fr, tt = _range_24h(from_, to)
    sql = """
    WITH params AS (
      SELECT %(from)s::timestamptz AS ts_from, %(to)s::timestamptz AS ts_to
    )
    SELECT to_char(
      generate_series(
        date_trunc('hour', (ts_from AT TIME ZONE %(tz)s)),
        date_trunc('hour', (ts_to   AT TIME ZONE %(tz)s)),
        interval '1 hour'
      ), 'HH24:00'
    ) AS local_hour
    FROM params;
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, {"from": fr, "to": tt, "tz": TZ})
        return cur.fetchall()

# ------------------- Bombas activas por hora --------------------
@router.get("/graphs/pumps/active")
def graph_pumps_active(
    from_: str | None = Query(None, alias="from"),
    to: str | None = None,
    location_id: int | None = None,
):
    fr, tt = _range_24h(from_, to)
    sql = """
    WITH params AS (
      SELECT %(from)s::timestamptz AS ts_from, %(to)s::timestamptz AS ts_to
    ),
    hours AS (
      SELECT generate_series(
        date_trunc('hour', (ts_from AT TIME ZONE %(tz)s)),
        date_trunc('hour', (ts_to   AT TIME ZONE %(tz)s)),
        interval '1 hour'
      ) AS bucket_local
      FROM params
    ),
    hb AS (
      SELECT date_trunc('hour', (h.hb_ts AT TIME ZONE %(tz)s)) AS bucket_local,
             h.pump_id,
             bool_or(COALESCE(h.aux, h.relay) IS TRUE)         AS on_any
      FROM kpi.pump_heartbeat_parsed h, params p
      WHERE h.hb_ts >= p.ts_from AND h.hb_ts <= p.ts_to
      GROUP BY 1, 2
    ),
    hb_filtered AS (
      SELECT hb.bucket_local, hb.pump_id, hb.on_any
      FROM hb
      LEFT JOIN public.pumps p ON p.id = hb.pump_id
      WHERE %(loc)s::int IS NULL OR p.location_id = %(loc)s
    ),
    agg AS (
      SELECT bucket_local, count(*) FILTER (WHERE on_any) AS pumps_count
      FROM hb_filtered
      GROUP BY 1
    )
    SELECT to_char(h.bucket_local, 'HH24:00') AS local_hour,
           COALESCE(a.pumps_count, 0)         AS pumps_count
    FROM hours h
    LEFT JOIN agg a ON a.bucket_local = h.bucket_local
    ORDER BY h.bucket_local;
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, {"from": fr, "to": tt, "tz": TZ, "loc": location_id})
        return cur.fetchall()

# ------------------- Nivel promedio por hora (tanques) ----------
@router.get("/graphs/tanks/level_avg")
def graph_tanks_level_avg(
    from_: str | None = Query(None, alias="from"),
    to: str | None = None,
    location_id: int | None = None,
    entity_id: int | None = None,
):
    fr, tt = _range_24h(from_, to)
    sql = """
    WITH params AS (
      SELECT %(from)s::timestamptz AS ts_from, %(to)s::timestamptz AS ts_to
    ),
    hours AS (
      SELECT generate_series(
        date_trunc('hour', (ts_from AT TIME ZONE %(tz)s)),
        date_trunc('hour', (ts_to   AT TIME ZONE %(tz)s)),
        interval '1 hour'
      ) AS bucket_local
      FROM params
    ),
    lv AS (
      SELECT date_trunc('hour', (r.ts AT TIME ZONE %(tz)s)) AS bucket_local,
             r.tank_id,
             avg(r.level_pct)::float                        AS avg_level
      FROM public.tank_readings r, params p
      WHERE r.ts >= p.ts_from AND r.ts <= p.ts_to
        AND (%(entity)s::int IS NULL OR r.tank_id = %(entity)s)
      GROUP BY 1, 2
    ),
    lv_f AS (
      SELECT lv.bucket_local, lv.avg_level
      FROM lv
      LEFT JOIN public.tanks t ON t.id = lv.tank_id
      WHERE (%(loc)s::int IS NULL OR t.location_id = %(loc)s)
    ),
    agg AS (
      SELECT bucket_local, avg(avg_level) AS avg_level_pct
      FROM lv_f
      GROUP BY 1
    )
    SELECT to_char(h.bucket_local, 'HH24:00') AS local_hour,
           agg.avg_level_pct
    FROM hours h
    LEFT JOIN agg ON agg.bucket_local = h.bucket_local
    ORDER BY h.bucket_local;
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, {"from": fr, "to": tt, "tz": TZ, "loc": location_id, "entity": entity_id})
        return cur.fetchall()

# ------------------- Snapshot /kpi/pumps/status ------------------
@router.get("/pumps/status")
def pumps_status():
    sql = """
    SELECT pump_id,
           name,
           location_id,
           location_name,
           state,
           NULL::int AS latest_event_id,
           age_sec,
           online,
           hb_ts      AS event_ts,
           latest_hb_id,
           hb_ts
    FROM kpi.v_pumps_with_status
    ORDER BY location_name NULLS LAST, name;
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        return cur.fetchall()

# ------------------- Snapshot /kpi/tanks/latest ------------------
@router.get("/tanks/latest")
def tanks_latest(location_id: int | None = None):
    sql = """
    SELECT *
    FROM kpi.v_tanks_with_config
    WHERE %(loc)s::int IS NULL OR location_id = %(loc)s
    ORDER BY location_name NULLS LAST, name;
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, {"loc": location_id})
        return cur.fetchall()
