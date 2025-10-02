# app/routes/ops.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, Query
from psycopg import OperationalError, errors as psy_errors
from psycopg.rows import dict_row

from app.core.db import get_conn
from app.core.security import device_id_dep
from app.core.tenancy import require_org

router = APIRouter(prefix="/ops", tags=["ops"])

# Estructura base de respuesta (aún si la DB falla devolvemos 200 con arrays vacíos)
def _empty(org_id: int) -> Dict[str, Any]:
    return {
        "org_id": org_id,
        "locations": [],
        "tanks": [],
        "pumps": [],
    }

@router.get("/overview")
def ops_overview(
    user_id: Optional[int] = Query(default=None),
    _=Depends(device_id_dep),
) -> Dict[str, Any]:
    """
    Devuelve en un solo payload:
      - locations [{id, code, name, pumps_count, tanks_count}]
      - tanks [{tank_id, tank_name, location_id, location_code, location_name, low_pct, ..., last_ts, level_percent, ...}]
      - pumps [{pump_id, pump_name, location_id, ..., drive_type, vfd_*, last_ts, is_on, flow_lpm, pressure_bar, ...}]

    Rápido, con JOINs simples + LATERAL para la última lectura.
    Si la DB falla, devuelve 200 con arrays vacíos para que el front no quede "cargando".
    """
    org_id = require_org()
    out = _empty(org_id)

    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            # 🔐 RLS: seteo explícito por si las policies lo requieren
            cur.execute("select set_config('app.org_id', %s, true)", (str(int(org_id)),))

            # ------- LOCATIONS (resumen) -------
            cur.execute(
                """
                WITH pump_counts AS (
                  SELECT al.location_id, COUNT(*)::int AS pumps_count
                  FROM public.asset_locations al
                  JOIN public.locations l2 ON l2.id = al.location_id
                  WHERE al.asset_type = 'pump' AND l2.org_id = %s
                  GROUP BY al.location_id
                ),
                tank_counts AS (
                  SELECT al.location_id, COUNT(*)::int AS tanks_count
                  FROM public.asset_locations al
                  JOIN public.locations l2 ON l2.id = al.location_id
                  WHERE al.asset_type = 'tank' AND l2.org_id = %s
                  GROUP BY al.location_id
                )
                SELECT
                  l.id, l.code, l.name,
                  COALESCE(pc.pumps_count, 0) AS pumps_count,
                  COALESCE(tc.tanks_count, 0) AS tanks_count
                FROM public.locations l
                LEFT JOIN pump_counts pc ON pc.location_id = l.id
                LEFT JOIN tank_counts tc ON tc.location_id = l.id
                WHERE l.org_id = %s
                ORDER BY l.name
                """,
                (org_id, org_id, org_id),
            )
            out["locations"] = cur.fetchall()

            # ------- TANKS (config + última lectura + ubicación) -------
            cur.execute(
                """
                SELECT
                  t.id    AS tank_id,
                  t.name  AS tank_name,
                  t.org_id,
                  al.location_id,
                  l.code  AS location_code,
                  l.name  AS location_name,
                  tc.low_pct, tc.low_low_pct, tc.high_pct, tc.high_high_pct,
                  r.ts    AS last_ts,
                  r.level_percent,
                  r.volume_l,
                  r.temperature_c
                FROM public.tanks t
                LEFT JOIN public.tank_config tc
                  ON tc.tank_id = t.id
                LEFT JOIN public.asset_locations al
                  ON al.asset_type = 'tank' AND al.asset_id = t.id
                LEFT JOIN public.locations l
                  ON l.id = al.location_id
                LEFT JOIN LATERAL (
                  SELECT tr.ts, tr.level_percent, tr.volume_l, tr.temperature_c
                  FROM public.tank_readings tr
                  WHERE tr.tank_id = t.id
                  ORDER BY tr.ts DESC
                  LIMIT 1
                ) r ON TRUE
                WHERE t.org_id = %s
                ORDER BY t.id
                """,
                (org_id,),
            )
            out["tanks"] = cur.fetchall()

            # ------- PUMPS (config efectivo + última lectura + ubicación) -------
            cur.execute(
                """
                SELECT
                  p.id    AS pump_id,
                  p.name  AS pump_name,
                  p.org_id,
                  al.location_id,
                  l.code  AS location_code,
                  l.name  AS location_name,
                  COALESCE(pc.remote_enabled, p.remote_enabled)              AS remote_enabled,
                  COALESCE(pc.drive_type,     p.drive_type::text)            AS drive_type,
                  COALESCE(pc.vfd_min_speed_pct,     p.vfd_min_speed_pct)    AS vfd_min_speed_pct,
                  COALESCE(pc.vfd_max_speed_pct,     p.vfd_max_speed_pct)    AS vfd_max_speed_pct,
                  COALESCE(pc.vfd_default_speed_pct, p.vfd_default_speed_pct) AS vfd_default_speed_pct,
                  r.ts    AS last_ts,
                  r.is_on,
                  r.flow_lpm,
                  r.pressure_bar,
                  r.voltage_v,
                  r.current_a,
                  r.control_mode,
                  r.manual_lockout
                FROM public.pumps p
                LEFT JOIN public.pump_configs pc
                  ON pc.pump_id = p.id
                  AND (pc.org_id = p.org_id OR pc.org_id IS NULL)
                LEFT JOIN public.asset_locations al
                  ON al.asset_type = 'pump' AND al.asset_id = p.id
                LEFT JOIN public.locations l
                  ON l.id = al.location_id
                LEFT JOIN LATERAL (
                  SELECT pr.ts, pr.is_on, pr.flow_lpm, pr.pressure_bar,
                         pr.voltage_v, pr.current_a, pr.control_mode, pr.manual_lockout
                  FROM public.pump_readings pr
                  WHERE pr.pump_id = p.id
                  ORDER BY pr.ts DESC
                  LIMIT 1
                ) r ON TRUE
                WHERE p.org_id = %s
                ORDER BY p.id
                """,
                (org_id,),
            )
            out["pumps"] = cur.fetchall()

        return out

    except (OperationalError, psy_errors.AdminShutdown, psy_errors.CannotConnectNow, TimeoutError):
        # Nunca 500 para la UI de operaciones
        return out
    except Exception:
        return out
