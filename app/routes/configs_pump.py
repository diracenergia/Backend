# app/routes/configs_pump.py
from fastapi import APIRouter, Depends, Request
from app.deps import get_db  # tu helper para obtener conexión
from app.auth import require_auth  # middleware/dep que setea request.state.org_id

router = APIRouter(prefix="/pumps", tags=["pumps"])

@router.get("/config")
def list_pumps_config(request: Request, db = Depends(get_db), user=Depends(require_auth)):
    org_id = int(getattr(request.state, "org_id", 0))  # viene del JWT (payload.org_id)
    if not org_id:
        # si querés soportar modo legacy sin login:
        org_id = int(request.headers.get("x-org-id", "0") or 0)
    if not org_id:
        return []  # o lanzar 401/403

    with db.cursor() as cur:
        cur.execute("""
            SELECT
                p.id AS pump_id,
                COALESCE(p.name, p.code) AS pump_name,
                p.model,
                p.max_flow_lpm,

                cfg.remote_enabled,
                cfg.drive_type,
                cfg.vfd_min_speed_pct,
                cfg.vfd_max_speed_pct,
                cfg.vfd_default_speed_pct,

                al.location_id,
                l.code AS location_code,
                l.name AS location_name
            FROM public.pumps p
            -- config (opcional, por org)
            LEFT JOIN public.pump_configs cfg
                   ON cfg.pump_id = p.id
                  AND cfg.org_id  = %(org_id)s
            -- ubicación (si existe)
            LEFT JOIN public.asset_locations al
                   ON al.asset_type = 'pump'
                  AND al.asset_id   = p.id
            LEFT JOIN public.locations l
                   ON l.id = al.location_id

            WHERE
              -- Si tu tabla pumps tiene org_id, esta línea basta:
              (p.org_id = %(org_id)s)
              OR
              -- si NO lo tiene, filtramos por pertenencia a una location de la org:
              EXISTS (
                SELECT 1
                FROM public.asset_locations al2
                JOIN public.locations l2 ON l2.id = al2.location_id
                WHERE al2.asset_type = 'pump'
                  AND al2.asset_id   = p.id
                  AND l2.org_id      = %(org_id)s
              )

            ORDER BY l.name NULLS FIRST, p.name NULLS LAST, p.id
        """, {"org_id": org_id})

        rows = cur.fetchall()

    # mapeo simple a JSON
    result = []
    for r in rows:
        result.append({
            "pump_id": r[0],
            "pump_name": r[1],
            "model": r[2],
            "max_flow_lpm": r[3],
            "remote_enabled": r[4],
            "drive_type": r[5],
            "vfd_min_speed_pct": r[6],
            "vfd_max_speed_pct": r[7],
            "vfd_default_speed_pct": r[8],
            "location_id": r[9],
            "location_code": r[10],
            "location_name": r[11],
        })
    return result
