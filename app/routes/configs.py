from __future__ import annotations
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from psycopg.rows import dict_row

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
    Devuelve config de umbrales por tanque + datos mínimos, scopeado por org actual.
    Usa public.tank_config (singular) y no asume t.location_id (se apoya en asset_locations).
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
              t.id                AS tank_id,
              t.name              AS tank_name,
              t.capacity_m3,
              c.low_pct,
              c.low_low_pct,
              c.high_pct,
              c.high_high_pct,
              al.location_id,
              l.code              AS location_code,
              l.name              AS location_name
            FROM public.tanks t
            LEFT JOIN public.tank_config c
              ON c.tank_id = t.id
            LEFT JOIN public.asset_locations al
              ON al.asset_type = 'tank' AND al.asset_id = t.id
            LEFT JOIN public.locations l
              ON l.id = al.location_id
            WHERE t.org_id = current_setting('app.org_id')::bigint
              AND (%s::bigint IS NULL OR al.location_id = %s)
            ORDER BY l.name NULLS LAST, t.name;
            """,
            (location_id, location_id),
        )
        return [dict(r) for r in cur.fetchall()]
