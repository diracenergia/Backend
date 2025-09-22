# app/routes/kpi.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Header, HTTPException, Query
from psycopg.rows import dict_row

from app.core.db import get_conn

router = APIRouter(prefix="/kpi", tags=["kpi"])

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _require_org_id(x_org_id: Optional[int]) -> int:
    if x_org_id is None:
        raise HTTPException(status_code=400, detail="X-Org-Id header is required")
    return int(x_org_id)


# ------------------------------------------------------------------------------
# 0) Time buckets (eje común: últimas 24h, por hora, TZ America/Argentina/Buenos_Aires)
# ------------------------------------------------------------------------------
@router.get("/time-buckets/hourly-24h")
def kpi_time_buckets_hourly_24h() -> List[Dict[str, Any]]:
    """
    Devuelve los buckets horarios locales de las últimas 24 horas (incluida la hora actual).
    Útil para alinear los gráficos de bombas y tanques.
    """
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT local_hour
            FROM public.v_time_spine_hour_24h_local
            ORDER BY local_hour;
            """
        )
        return [dict(r) for r in cur.fetchall()]


# ------------------------------------------------------------------------------
# 1) Bombas — actividad = "tuvo lectura" (sin usar is_on)
# ------------------------------------------------------------------------------
@router.get("/pumps/activity/hourly-24h")
def kpi_pumps_activity_hourly_24h(
    x_org_id: Optional[int] = Header(default=None, convert_underscores=False),
    location_id: Optional[int] = Query(None),
) -> List[Dict[str, Any]]:
    """
    Conteo de bombas con lectura por hora y por localidad, últimas 24h.
    """
    org_id = _require_org_id(x_org_id)
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.v_pumps_activity_hour_24h_loc_sync
            WHERE org_id = %s
              AND (%s::bigint IS NULL OR location_id = %s)
            ORDER BY location_name, local_hour;
            """,
            (org_id, location_id, location_id),
        )
        return [dict(r) for r in cur.fetchall()]


# ------------------------------------------------------------------------------
# 2) Tanques — nivel promedio (horario sincronizado)
# ------------------------------------------------------------------------------
@router.get("/tanks/level-avg/hourly-24h/by-location")
def kpi_tanks_level_avg_hourly_24h_by_location(
    x_org_id: Optional[int] = Header(default=None, convert_underscores=False),
    location_id: Optional[int] = Query(None),
) -> List[Dict[str, Any]]:
    """
    Nivel promedio de tanques agregado por localidad, por hora, últimas 24h.
    """
    org_id = _require_org_id(x_org_id)
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.v_tank_level_avg_hour_24h_loc_sync
            WHERE org_id = %s
              AND (%s::bigint IS NULL OR location_id = %s)
            ORDER BY location_name, local_hour;
            """,
            (org_id, location_id, location_id),
        )
        return [dict(r) for r in cur.fetchall()]


@router.get("/tanks/level-avg/hourly-24h/by-tank")
def kpi_tanks_level_avg_hourly_24h_by_tank(
    x_org_id: Optional[int] = Header(default=None, convert_underscores=False),
    location_id: Optional[int] = Query(None),
) -> List[Dict[str, Any]]:
    """
    Nivel promedio de tanques por tanque (detalle), por hora, últimas 24h.
    """
    org_id = _require_org_id(x_org_id)
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.v_tank_level_avg_hour_24h_tank_sync
            WHERE org_id = %s
              AND (%s::bigint IS NULL OR location_id = %s)
            ORDER BY location_name, tank_name, local_hour;
            """,
            (org_id, location_id, location_id),
        )
        return [dict(r) for r in cur.fetchall()]


# ------------------------------------------------------------------------------
# 3) Inventario por localidad
# ------------------------------------------------------------------------------
@router.get("/totals/by-location")
def kpi_totals_by_location(
    x_org_id: Optional[int] = Header(default=None, convert_underscores=False),
    location_id: Optional[int] = Query(None),
) -> List[Dict[str, Any]]:
    """
    Totales de bombas y tanques por localidad.
    """
    org_id = _require_org_id(x_org_id)
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.v_totals_by_location
            WHERE org_id = %s
              AND (%s::bigint IS NULL OR location_id = %s)
            ORDER BY location_name;
            """,
            (org_id, location_id, location_id),
        )
        return [dict(r) for r in cur.fetchall()]


# ------------------------------------------------------------------------------
# 4) Uptime de bombas (30 días)
# ------------------------------------------------------------------------------
@router.get("/uptime/pumps/30d/by-location")
def kpi_uptime_pumps_30d_by_location(
    x_org_id: Optional[int] = Header(default=None, convert_underscores=False),
    location_id: Optional[int] = Query(None),
) -> List[Dict[str, Any]]:
    """
    Uptime promedio 30 días por localidad (promedio de bombas).
    """
    org_id = _require_org_id(x_org_id)
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.v_pump_uptime_30d_loc
            WHERE org_id = %s
              AND (%s::bigint IS NULL OR location_id = %s)
            ORDER BY location_name;
            """,
            (org_id, location_id, location_id),
        )
        return [dict(r) for r in cur.fetchall()]


@router.get("/uptime/pumps/30d/by-pump")
def kpi_uptime_pumps_30d_by_pump(
    x_org_id: Optional[int] = Header(default=None, convert_underscores=False),
    location_id: Optional[int] = Query(None),
) -> List[Dict[str, Any]]:
    """
    Uptime 30 días por bomba (detalle).
    """
    org_id = _require_org_id(x_org_id)
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.v_pump_uptime_30d_pump
            WHERE org_id = %s
              AND (%s::bigint IS NULL OR location_id = %s)
            ORDER BY location_name, pump_name;
            """,
            (org_id, location_id, location_id),
        )
        return [dict(r) for r in cur.fetchall()]


# ------------------------------------------------------------------------------
# 5) Alarmas activas por severidad y localidad (en vivo)
# ------------------------------------------------------------------------------
@router.get("/alarms/active/by-severity")
def kpi_alarms_active_by_severity(
    x_org_id: Optional[int] = Header(default=None, convert_underscores=False),
    location_id: Optional[int] = Query(None),
) -> List[Dict[str, Any]]:
    """
    Alarmas activas agrupadas por severidad y localidad.
    """
    org_id = _require_org_id(x_org_id)
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.v_alarms_active_by_sev_loc
            WHERE org_id = %s
              AND (%s::bigint IS NULL OR location_id = %s)
            ORDER BY location_name, severity;
            """,
            (org_id, location_id, location_id),
        )
        return [dict(r) for r in cur.fetchall()]


# ------------------------------------------------------------------------------
# 6) Frescura de datos (últimas lecturas por localidad)
# ------------------------------------------------------------------------------
@router.get("/latest-ts/by-location")
def kpi_latest_ts_by_location(
    x_org_id: Optional[int] = Header(default=None, convert_underscores=False),
    location_id: Optional[int] = Query(None),
) -> List[Dict[str, Any]]:
    """
    Última lectura de bombas y tanques por localidad.
    """
    org_id = _require_org_id(x_org_id)
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.v_latest_ts_by_location
            WHERE org_id = %s
              AND (%s::bigint IS NULL OR location_id = %s)
            ORDER BY location_name;
            """,
            (org_id, location_id, location_id),
        )
        return [dict(r) for r in cur.fetchall()]


# ------------------------------------------------------------------------------
# 7) Cobertura de lecturas de tanques (30 días)
# ------------------------------------------------------------------------------
@router.get("/tanks/coverage/30d")
def kpi_tanks_coverage_30d(
    x_org_id: Optional[int] = Header(default=None, convert_underscores=False),
    location_id: Optional[int] = Query(None),
) -> List[Dict[str, Any]]:
    """
    Por tanque: horas con lectura en 30 días y % de cobertura respecto de 720 horas.
    """
    org_id = _require_org_id(x_org_id)
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.v_tank_coverage_30d
            WHERE org_id = %s
              AND (%s::bigint IS NULL OR location_id = %s)
            ORDER BY location_name, tank_name;
            """,
            (org_id, location_id, location_id),
        )
        return [dict(r) for r in cur.fetchall()]


# ------------------------------------------------------------------------------
# 8) Potencia nominal por bomba (para eficiencia en el front)
# ------------------------------------------------------------------------------
@router.get("/pumps/rated-kw")
def kpi_pumps_rated_kw(
    x_org_id: Optional[int] = Header(default=None, convert_underscores=False),
    location_id: Optional[int] = Query(None),
) -> List[Dict[str, Any]]:
    """
    Potencia nominal (rated_kw) por bomba y localidad.
    """
    org_id = _require_org_id(x_org_id)
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.v_pumps_rated_kw_loc
            WHERE org_id = %s
              AND (%s::bigint IS NULL OR location_id = %s)
            ORDER BY location_name, pump_name;
            """,
            (org_id, location_id, location_id),
        )
        return [dict(r) for r in cur.fetchall()]


# ------------------------------------------------------------------------------
# 9) Locations (para combos en el front)
# ------------------------------------------------------------------------------
@router.get("/locations")
def kpi_locations(
    x_org_id: Optional[int] = Header(default=None, convert_underscores=False),
) -> List[Dict[str, Any]]:
    """
    Lista de localidades visibles (no filtra por lecturas).
    """
    org_id = _require_org_id(x_org_id)
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id, code, name
            FROM public.locations
            WHERE org_id = %s
            ORDER BY name;
            """,
            (org_id,),
        )
        return [dict(r) for r in cur.fetchall()]
