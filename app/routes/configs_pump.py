# app/routes/configs_pump.py
from typing import List, Dict, Any
from fastapi import APIRouter, Depends, Request
from app.deps import get_db            # conexión psycopg
from app.auth import require_auth      # setea request.state.org_id

router = APIRouter(prefix="/pumps", tags=["pumps"])

@router.get("/config")
def list_pumps_config(request: Request, db = Depends(get_db), _user = Depends(require_auth)) -> List[Dict[str, Any]]:
    # org del JWT; si no hay, modo legacy por header
    org_id = int(getattr(request.state, "org_id", 0) or (request.headers.get("x-org-id") or 0))
    if not org_id:
        # Sin organización -> no devolvemos nada (evita 500/CORS fantasma en el front)
        return []

    with db.cursor() as cur:
        # Importante:
        # - Usamos la tabla SINGULAR public.pump_config (no 'pump_configs'), que existe en tu schema.
        # - Filtramos pertenencia a la organización SOLO por locations -> asset_locations.
        # - DISTINCT ON para evitar duplicados si una bomba aparece en varias locations.
        cur.execute(
            """
            SELECT DISTINCT ON (p.id)
                p.id                              AS pump_id,
                COALESCE(p.name, p.code)          AS pump_name,
                p.model,
                p.max_flow_lpm,

                cfg.drive_type,
                cfg.remote_enabled,
                cfg.vfd_min_speed_pct,
                cfg.vfd_max_speed_pct,
                cfg.vfd_default_speed_pct,

                l.id                               AS location_id,
                l.code                             AS location_code,
                l.name                             AS location_name
            FROM public.pumps p
            LEFT JOIN public.pump_config cfg
                   ON cfg.pump_id = p.id
            LEFT JOIN public.asset_locations al
                   ON al.asset_type = 'pump'
                  AND al.asset_id   = p.id
            LEFT JOIN public.locations l
                   ON l.id = al.location_id
            WHERE EXISTS (
                SELECT 1
                FROM public.asset_locations al2
                JOIN public.locations l2 ON l2.id = al2.location_id
                WHERE al2.asset_type = 'pump'
                  AND al2.asset_id   = p.id
                  AND l2.org_id      = %(org_id)s
            )
            -- DISTINCT ON requiere que el primer ORDER BY sea la/s misma/s expr/s del DISTINCT
            ORDER BY p.id, l.name NULLS FIRST, pump_name NULLS LAST
            """,
            {"org_id": org_id},
        )
        rows = cur.fetchall()

    # Map a JSON
    result: List[Dict[str, Any]] = []
    for r in rows:
        result.append({
            "pump_id": r[0],
            "pump_name": r[1],
            "model": r[2],
            "max_flow_lpm": r[3],
            "drive_type": r[4],
            "remote_enabled": r[5],
            "vfd_min_speed_pct": r[6],
            "vfd_max_speed_pct": r[7],
            "vfd_default_speed_pct": r[8],
            "location_id": r[9],
            "location_code": r[10],
            "location_name": r[11],
        })
    return result
