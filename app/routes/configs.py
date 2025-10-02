from __future__ import annotations
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from psycopg.rows import dict_row
from psycopg.errors import UndefinedTable, OperationalError, InsufficientPrivilege

from app.core.security import device_id_dep
from app.auth.deps import conn_with_rls

router = APIRouter(prefix="/tanks", tags=["tank-config"])

@router.get("/config")
def list_configs(
    location_id: Optional[int] = Query(None, description="Filtra por location_id"),
    _=Depends(device_id_dep),
    conn=Depends(conn_with_rls),
) -> List[Dict[str, Any]]:
    """
    Devuelve config de umbrales por tanque + datos mínimos (multi-tenant).
    Usa public.tank_config (singular) y asset_locations para la ubicación.
    """
    sql = """
    SELECT
      t.id AS tank_id,
      t.name AS tank_name,
      COALESCE(t.capacity_liters, t.capacity_m3 * 1000) AS capacity_liters,
      t.height_m,
      t.diameter_m,
      t.material,
      t.fluid,
      t.install_year,
      COALESCE(tc.low_pct,       15)::numeric   AS low_pct,
      COALESCE(tc.low_low_pct,    5)::numeric   AS low_low_pct,
      COALESCE(tc.high_pct,      85)::numeric   AS high_pct,
      COALESCE(tc.high_high_pct, 95)::numeric   AS high_high_pct,
      al.location_id,
      l.code  AS location_code,
      l.name  AS location_name,
      t.org_id
    FROM public.tanks t
    LEFT JOIN public.tank_config tc
      ON tc.tank_id = t.id
    LEFT JOIN public.asset_locations al
      ON al.asset_type = 'tank' AND al.asset_id = t.id
    LEFT JOIN public.locations l
      ON l.id = al.location_id
    WHERE
      (
        COALESCE(current_setting('app.org_id', true), '') = ''
        OR t.org_id::text = current_setting('app.org_id', true)
      )
      AND (%s::bigint IS NULL OR al.location_id = %s)
    ORDER BY l.name NULLS LAST, t.name;
    """
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (location_id, location_id))
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    except (UndefinedTable, InsufficientPrivilege, OperationalError):
        # Si falta la tabla/vista o hay problema transitorio de DB/pool, devolvemos vacío
        return []
    except Exception as e:
        raise HTTPException(500, f"tanks.config error: {e.__class__.__name__}")
