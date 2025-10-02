# app/routes/configs_pump.py
from __future__ import annotations
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from psycopg.rows import dict_row
import psycopg

from app.core.security import device_id_dep
from app.auth.deps import conn_with_rls

router = APIRouter(prefix="/pumps", tags=["pump-config"])

@router.get("/config")
def list_configs(
    location_id: Optional[int] = Query(None, description="Filtra por location_id"),
    _=Depends(device_id_dep),
    conn=Depends(conn_with_rls),
) -> List[Dict[str, Any]]:
    """
    Devuelve config de bombas (un row por bomba) combinando:
      - public.pumps (datos básicos)
      - public.pump_config (umbrales/config; si falta, se usa lo que haya en pumps)
      - public.asset_locations -> public.locations (ubicación)
    Scopeado por org actual via current_setting('app.org_id').
    """
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                  p.id   AS pump_id,
                  p.name AS pump_name,
                  p.model,
                  p.max_flow_lpm,

                  -- Preferimos valores de pump_config; si no hay, caemos a columnas de pumps
                  COALESCE(pc.remote_enabled, p.remote_enabled)                 AS remote_enabled,
                  COALESCE(pc.drive_type,     p.drive_type::text)              AS drive_type,
                  COALESCE(pc.vfd_min_speed_pct,     p.vfd_min_speed_pct)      AS vfd_min_speed_pct,
                  COALESCE(pc.vfd_max_speed_pct,     p.vfd_max_speed_pct)      AS vfd_max_speed_pct,
                  COALESCE(pc.vfd_default_speed_pct, p.vfd_default_speed_pct)  AS vfd_default_speed_pct,

                  al.location_id,
                  l.code AS location_code,
                  l.name AS location_name
                FROM public.pumps p
                LEFT JOIN public.pump_config pc
                  ON pc.pump_id = p.id
                LEFT JOIN public.asset_locations al
                  ON al.asset_type = 'pump' AND al.asset_id = p.id
                LEFT JOIN public.locations l
                  ON l.id = al.location_id
                WHERE p.org_id = current_setting('app.org_id')::bigint
                  AND (%s::bigint IS NULL OR al.location_id = %s)
                ORDER BY l.name NULLS LAST, p.name;
                """,
                (location_id, location_id),
            )
            return [dict(r) for r in cur.fetchall()]
    except psycopg.OperationalError:
        # mismo mensaje que estás viendo en el front, pero más claro
        raise HTTPException(status_code=500, detail="internal error: OperationalError")
