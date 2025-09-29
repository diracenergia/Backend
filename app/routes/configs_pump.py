# app/routes/configs_pump.py
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Request

# Compat: en tu repo existe get_conn en app.core.db
try:
    from app.core.db import get_conn  # psycopg connection helper (context manager)
except Exception as e:  # pragma: no cover
    raise RuntimeError("No se pudo importar app.core.db.get_conn") from e

# Compat: si tenés require_auth, lo usamos; si no, seguimos igual (modo legacy)
try:
    from fastapi import Depends
    from app.auth import require_auth  # debe setear request.state.org_id si hay JWT
    _AUTH_DEP = Depends(require_auth)
except Exception:
    _AUTH_DEP = None  # sin dependencia, seguiremos leyendo X-Org-Id

router = APIRouter(prefix="/pumps", tags=["pumps"])


def _get_org_id(request: Request) -> int:
    """
    Prioridad:
    1) request.state.org_id (seteado por auth/JWT)
    2) Header X-Org-Id (modo legacy sin login)
    """
    org_id: Optional[int] = getattr(request.state, "org_id", None)
    if not org_id:
        hdr = request.headers.get("x-org-id") or request.headers.get("X-Org-Id")
        try:
            org_id = int(hdr) if hdr is not None else 0
        except Exception:
            org_id = 0
    return int(org_id or 0)


@router.get("/config")
def list_pumps_config(request: Request, _user=(_AUTH_DEP if _AUTH_DEP is not None else None)) -> List[Dict[str, Any]]:
    """
    Devuelve bombas visibles para la organización del request.
    - Filtra por pertenencia a locations de esa org (asset_locations -> locations.org_id).
    - Toma config desde tabla SINGULAR public.pump_config (coincide con tu pump.py).
    - Evita duplicados con DISTINCT ON (p.id).
    """
    org_id = _get_org_id(request)
    if not org_id:
        # Sin org no devolvemos nada (evita 500 y CORS bloqueado en el front)
        return []

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (p.id)
                p.id                                           AS pump_id,
                COALESCE(NULLIF(p.name, ''), 'Bomba ' || p.id) AS pump_name,
                p.model,
                p.max_flow_lpm,

                cfg.drive_type,
                cfg.remote_enabled,
                cfg.vfd_min_speed_pct,
                cfg.vfd_max_speed_pct,
                cfg.vfd_default_speed_pct,

                l.id    AS location_id,
                l.code  AS location_code,
                l.name  AS location_name
            FROM public.pumps p
            -- Config (tu schema usa 'pump_config' en singular)
            LEFT JOIN public.pump_config cfg
                   ON cfg.pump_id = p.id
            -- Ubicación
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
            -- DISTINCT ON requiere que la primera expresión de ORDER BY coincida
            ORDER BY p.id, l.name NULLS FIRST, pump_name NULLS LAST
            """,
            {"org_id": org_id},
        )
        rows = cur.fetchall()

    result: List[Dict[str, Any]] = []
    for (
        pump_id,
        pump_name,
        model,
        max_flow_lpm,
        drive_type,
        remote_enabled,
        vfd_min_speed_pct,
        vfd_max_speed_pct,
        vfd_default_speed_pct,
        location_id,
        location_code,
        location_name,
    ) in rows:
        result.append(
            {
                "pump_id": pump_id,
                "pump_name": pump_name,
                "model": model,
                "max_flow_lpm": max_flow_lpm,
                "drive_type": drive_type,
                "remote_enabled": remote_enabled,
                "vfd_min_speed_pct": vfd_min_speed_pct,
                "vfd_max_speed_pct": vfd_max_speed_pct,
                "vfd_default_speed_pct": vfd_default_speed_pct,
                "location_id": location_id,
                "location_code": location_code,
                "location_name": location_name,
            }
        )
    return result
