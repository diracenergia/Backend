from __future__ import annotations

from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Path
from psycopg.rows import dict_row

from app.auth.deps import conn_with_rls

router = APIRouter(prefix="/pumps", tags=["pump-config"])

# ------------------------------------------------------------------------------
# LIST: /pumps/config
# Devuelve SOLO bombas de la org actual (RLS) + su config + location
# El scope se garantiza por locations.org_id (vía asset_locations)
# ------------------------------------------------------------------------------
@router.get("/config")
def list_pumps_config(conn = Depends(conn_with_rls)) -> List[Dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
              p.id                              AS pump_id,
              COALESCE(n.name, p.name, CONCAT('Bomba ', p.id)) AS pump_name,
              p.model,
              p.max_flow_lpm,
              p.drive_type,
              cfg.remote_enabled,
              cfg.vfd_min_speed_pct,
              cfg.vfd_max_speed_pct,
              cfg.vfd_default_speed_pct,
              l.id                               AS location_id,
              l.code                             AS location_code,
              l.name                             AS location_name
            FROM public.pumps p
            -- nombre/código opcional desde el grafo (si existe)
            LEFT JOIN public.v_asset_nodes n
              ON n.type = 'pump' AND n.asset_id = p.id
            -- mapeo asset → location
            JOIN public.asset_locations al
              ON al.asset_type = 'pump' AND al.asset_id = p.id
            JOIN public.locations l
              ON l.id = al.location_id
            -- config (puede no existir)
            LEFT JOIN public.pump_configs cfg
              ON cfg.pump_id = p.id
            WHERE l.org_id = current_setting('app.org_id')::bigint
            ORDER BY l.name, pump_name, p.id;
            """
        )
        return [dict(r) for r in cur.fetchall()]

# ------------------------------------------------------------------------------
# GET ONE: /pumps/{pump_id}/config
# Valida pertenencia del asset a la org; si no, 404
# ------------------------------------------------------------------------------
@router.get("/{pump_id}/config")
def get_pump_config(
    pump_id: int = Path(..., ge=1),
    conn = Depends(conn_with_rls),
) -> Dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        # valida org
        cur.execute(
            """
            SELECT 1
            FROM public.pumps p
            JOIN public.asset_locations al
              ON al.asset_type='pump' AND al.asset_id=p.id
            JOIN public.locations l
              ON l.id = al.location_id
            WHERE p.id = %s
              AND l.org_id = current_setting('app.org_id')::bigint;
            """,
            (pump_id,),
        )
        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail="pump not found")

        # trae payload unificado
        cur.execute(
            """
            SELECT
              p.id                              AS pump_id,
              COALESCE(n.name, p.name, CONCAT('Bomba ', p.id)) AS pump_name,
              p.model,
              p.max_flow_lpm,
              p.drive_type,
              cfg.remote_enabled,
              cfg.vfd_min_speed_pct,
              cfg.vfd_max_speed_pct,
              cfg.vfd_default_speed_pct,
              l.id                               AS location_id,
              l.code                             AS location_code,
              l.name                             AS location_name
            FROM public.pumps p
            LEFT JOIN public.v_asset_nodes n
              ON n.type = 'pump' AND n.asset_id = p.id
            JOIN public.asset_locations al
              ON al.asset_type='pump' AND al.asset_id=p.id
            JOIN public.locations l
              ON l.id = al.location_id
            LEFT JOIN public.pump_configs cfg
              ON cfg.pump_id = p.id
            WHERE p.id = %s
              AND l.org_id = current_setting('app.org_id')::bigint;
            """,
            (pump_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="pump not found")
        return dict(row)

# ------------------------------------------------------------------------------
# UPSERT: /pumps/{pump_id}/config (PUT/POST)
# Si llega drive_type, lo guardamos en pumps; el resto en pump_configs
# ------------------------------------------------------------------------------
def _bool(v: Optional[Any]) -> Optional[bool]:
    if v is None: return None
    if isinstance(v, bool): return v
    s = str(v).strip().lower()
    if s in {"true","1","t","yes","y","on"}: return True
    if s in {"false","0","f","no","n","off"}: return False
    return None

def _num(v: Optional[Any]) -> Optional[float]:
    if v is None: return None
    try: return float(v)
    except: return None

def _upsert_pump_config(conn, pump_id: int, body: Dict[str, Any]) -> Dict[str, Any]:
    rem = _bool(body.get("remote_enabled"))
    dtyp = body.get("drive_type")
    vmin = _num(body.get("vfd_min_speed_pct"))
    vmax = _num(body.get("vfd_max_speed_pct"))
    vdef = _num(body.get("vfd_default_speed_pct"))

    with conn.cursor(row_factory=dict_row) as cur:
        # valida org (mismo check que GET)
        cur.execute(
            """
            SELECT l.org_id
            FROM public.pumps p
            JOIN public.asset_locations al
              ON al.asset_type='pump' AND al.asset_id=p.id
            JOIN public.locations l
              ON l.id = al.location_id
            WHERE p.id = %s
              AND l.org_id = current_setting('app.org_id')::bigint;
            """,
            (pump_id,),
        )
        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail="pump not found")

        # si vino drive_type, actualizamos la bomba
        if dtyp is not None:
            cur.execute(
                "UPDATE public.pumps SET drive_type=%s WHERE id=%s;",
                (dtyp, pump_id)
            )

        # upsert en pump_configs
        cur.execute(
            """
            INSERT INTO public.pump_configs
              (pump_id, remote_enabled, vfd_min_speed_pct, vfd_max_speed_pct, vfd_default_speed_pct)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (pump_id) DO UPDATE SET
              remote_enabled       = EXCLUDED.remote_enabled,
              vfd_min_speed_pct    = EXCLUDED.vfd_min_speed_pct,
              vfd_max_speed_pct    = EXCLUDED.vfd_max_speed_pct,
              vfd_default_speed_pct= EXCLUDED.vfd_default_speed_pct
            RETURNING pump_id, remote_enabled, vfd_min_speed_pct, vfd_max_speed_pct, vfd_default_speed_pct;
            """,
            (pump_id, rem, vmin, vmax, vdef),
        )
        cfg = dict(cur.fetchone())

        conn.commit()

    return {"ok": True, "config": cfg}

@router.put("/{pump_id}/config")
def put_pump_config(
    pump_id: int = Path(..., ge=1),
    body: Dict[str, Any] = None,
    conn = Depends(conn_with_rls),
):
    return _upsert_pump_config(conn, pump_id, body or {})

@router.post("/{pump_id}/config")
def post_pump_config(
    pump_id: int = Path(..., ge=1),
    body: Dict[str, Any] = None,
    conn = Depends(conn_with_rls),
):
    return _upsert_pump_config(conn, pump_id, body or {})
