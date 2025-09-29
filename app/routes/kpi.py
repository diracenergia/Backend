# app/routes/kpi.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from psycopg.rows import dict_row

from app.core.security import device_id_dep
from app.core.db import get_conn
from app.core.tenancy import require_org

router = APIRouter(prefix="/kpi", tags=["kpi"])


# ------------------------------------------------------------------------------
# 0) Time buckets (eje común: últimas 24h, por hora, TZ America/Argentina/Buenos_Aires)
# ------------------------------------------------------------------------------
@router.get("/time-buckets/hourly-24h")
def kpi_time_buckets_hourly_24h(
    _=Depends(device_id_dep),
) -> List[Dict[str, Any]]:
    """
    Devuelve los buckets horarios locales de las últimas 24 horas (incluida la hora actual).
    Útil para alinear los gráficos de bombas y tanques.
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT local_hour
            FROM public.v_time_spine_hour_24h_local
            ORDER BY local_hour;
            """
        )
        return [dict(r) for r in cur.fetchall() or []]


# ------------------------------------------------------------------------------
# 1) Bombas — actividad = "tuvo lectura" (sin usar is_on)
# ------------------------------------------------------------------------------
@router.get("/pumps/activity/hourly-24h")
def kpi_pumps_activity_hourly_24h(
    location_id: Optional[int] = Query(None),
    _=Depends(device_id_dep),
) -> List[Dict[str, Any]]:
    """
    Conteo de bombas con lectura por hora y por localidad, últimas 24h.
    """
    org_id = require_org()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
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
        return [dict(r) for r in cur.fetchall() or []]


# ------------------------------------------------------------------------------
# 2) Tanques — nivel promedio (horario sincronizado)
# ------------------------------------------------------------------------------
@router.get("/tanks/level-avg/hourly-24h/by-location")
def kpi_tanks_level_avg_hourly_24h_by_location(
    location_id: Optional[int] = Query(None),
    _=Depends(device_id_dep),
) -> List[Dict[str, Any]]:
    """
    Nivel promedio de tanques agregado por localidad, por hora, últimas 24h.
    """
    org_id = require_org()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
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
        return [dict(r) for r in cur.fetchall() or []]


@router.get("/tanks/level-avg/hourly-24h/by-tank")
def kpi_tanks_level_avg_hourly_24h_by_tank(
    location_id: Optional[int] = Query(None),
    _=Depends(device_id_dep),
) -> List[Dict[str, Any]]:
    """
    Nivel promedio de tanques por tanque (detalle), por hora, últimas 24h.
    """
    org_id = require_org()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
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
        return [dict(r) for r in cur.fetchall() or []]


# ------------------------------------------------------------------------------
# 3) Inventario por localidad
# ------------------------------------------------------------------------------
@router.get("/totals/by-location")
def kpi_totals_by_location(
    location_id: Optional[int] = Query(None),
    _=Depends(device_id_dep),
) -> List[Dict[str, Any]]:
    """
    Totales de activos por localidad (tanques, bombas, válvulas, manifolds).
    Fuente: v_location_summary_30d + filtro por org_id desde locations.
    """
    org_id = require_org()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
              v.location_id,
              v.location_code,
              v.location_name,
              COALESCE(v.tanks_count, 0)      AS tanks_count,
              COALESCE(v.pumps_count, 0)      AS pumps_count,
              COALESCE(v.valves_count, 0)     AS valves_count,
              COALESCE(v.manifolds_count, 0)  AS manifolds_count
            FROM public.v_location_summary_30d v
            JOIN public.locations l ON l.id = v.location_id
            WHERE l.org_id = %s
              AND (%s::bigint IS NULL OR v.location_id = %s)
            ORDER BY v.location_name;
            """,
            (org_id, location_id, location_id),
        )
        return [dict(r) for r in cur.fetchall() or []]


# ------------------------------------------------------------------------------
# 4) Uptime de bombas (30 días)
# ------------------------------------------------------------------------------
@router.get("/uptime/pumps/30d/by-location")
def kpi_uptime_pumps_30d_by_location(
    location_id: Optional[int] = Query(None),
    _=Depends(device_id_dep),
) -> List[Dict[str, Any]]:
    """
    Uptime promedio 30 días por localidad (promedio de bombas).
    """
    org_id = require_org()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
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
        return [dict(r) for r in cur.fetchall() or []]


@router.get("/uptime/pumps/30d/by-pump")
def kpi_uptime_pumps_30d_by_pump(
    location_id: Optional[int] = Query(None),
    _=Depends(device_id_dep),
) -> List[Dict[str, Any]]:
    """
    Uptime 30 días por bomba (detalle).
    """
    org_id = require_org()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
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
        return [dict(r) for r in cur.fetchall() or []]


# ------------------------------------------------------------------------------
# 5) Alarmas activas por severidad y localidad (en vivo)
# ------------------------------------------------------------------------------
@router.get("/alarms/active/by-severity")
def kpi_alarms_active_by_severity(
    location_id: Optional[int] = Query(None),
    _=Depends(device_id_dep),
) -> List[Dict[str, Any]]:
    """
    Alarmas activas agrupadas por severidad y localidad.
    """
    org_id = require_org()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
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
        return [dict(r) for r in cur.fetchall() or []]


# ------------------------------------------------------------------------------
# 6) Frescura de datos (últimas lecturas por localidad)
# ------------------------------------------------------------------------------
@router.get("/latest-ts/by-location")
def kpi_latest_ts_by_location(
    location_id: Optional[int] = Query(None),
    _=Depends(device_id_dep),
) -> List[Dict[str, Any]]:
    """
    Última lectura de bombas y tanques por localidad.
    """
    org_id = require_org()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
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
        return [dict(r) for r in cur.fetchall() or []]


# ------------------------------------------------------------------------------
# 7) Cobertura de lecturas de tanques (30 días)
# ------------------------------------------------------------------------------
@router.get("/tanks/coverage/30d")
def kpi_tanks_coverage_30d(
    location_id: Optional[int] = Query(None),
    _=Depends(device_id_dep),
) -> List[Dict[str, Any]]:
    """
    Por tanque: horas con lectura en 30 días y % de cobertura respecto de 720 horas.
    """
    org_id = require_org()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
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
        return [dict(r) for r in cur.fetchall() or []]


# ------------------------------------------------------------------------------
# 8) Potencia nominal por bomba (para eficiencia en el front)
# ------------------------------------------------------------------------------
@router.get("/pumps/rated-kw")
def kpi_pumps_rated_kw(
    location_id: Optional[int] = Query(None),
    _=Depends(device_id_dep),
) -> List[Dict[str, Any]]:
    """
    Potencia nominal (rated_kw) por bomba y localidad.
    """
    org_id = require_org()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
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
        return [dict(r) for r in cur.fetchall() or []]


# ------------------------------------------------------------------------------
# 9) Locations (para combos en el front)
# ------------------------------------------------------------------------------
@router.get("/locations")
def kpi_locations(
    _=Depends(device_id_dep),
) -> List[Dict[str, Any]]:
    """
    Lista de localidades visibles (no filtra por lecturas).
    """
    org_id = require_org()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id, code, name
            FROM public.locations
            WHERE org_id = %s
            ORDER BY name;
            """,
            (org_id,),
        )
        return [dict(r) for r in cur.fetchall() or []]
