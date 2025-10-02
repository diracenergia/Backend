# app/routes/configs_pump.py
from __future__ import annotations

from fastapi import APIRouter, Depends, Path, Body, HTTPException
from psycopg.rows import dict_row

from app.schemas.pumps import PumpConfigIn
from app.repos import pumps as repo
from app.core.security import device_id_dep
from app.core.db import get_conn

router = APIRouter(prefix="/pumps", tags=["config"])


@router.get("")
def list_pumps(_=Depends(device_id_dep)):
    """
    Lista bombas visibles para la org actual (vía RLS).
    """
    return repo.list_pumps()


@router.get("/config")
def list_pumps_with_config(_=Depends(device_id_dep)):
    """
    Lista bombas con su configuración (vista/consulta del repo).
    """
    return repo.list_pumps_with_config()


@router.get("/{pump_id}/config")
def get_pump_config(pump_id: int = Path(..., ge=1), _=Depends(device_id_dep)):
    """
    Devuelve la config de una bomba. Si no existe, el repo puede
    devolver valores nulos/por defecto.
    """
    # Si querés validar pertenencia a la org antes de consultar:
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT p.id
            FROM public.pumps p
            JOIN public.locations l ON l.id = p.location_id
            WHERE p.id = %s
              AND l.org_id = current_setting('app.org_id')::bigint
            """,
            (pump_id,),
        )
        if not cur.fetchone():
            raise HTTPException(404, "pump not found")

    # Delegamos al repo (que ya usa RLS vía get_conn)
    return repo.get_pump_config(pump_id)


@router.put("/{pump_id}/config")
def upsert_pump_config_put(
    pump_id: int = Path(..., ge=1),
    body: PumpConfigIn = Body(...),
    _=Depends(device_id_dep),
):
    """
    Upsert de configuración (método preferido por el front).
    Valida que la bomba pertenezca a la org actual.
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT p.id
            FROM public.pumps p
            JOIN public.locations l ON l.id = p.location_id
            WHERE p.id = %s
              AND l.org_id = current_setting('app.org_id')::bigint
            """,
            (pump_id,),
        )
        if not cur.fetchone():
            raise HTTPException(404, "pump not found")

    cfg = repo.upsert_pump_config(pump_id, body)
    return {"ok": True, "config": cfg}


# Fallback por compatibilidad (el front intenta PUT y, si recibe 405, hace POST)
@router.post("/{pump_id}/config")
def upsert_pump_config_post(
    pump_id: int = Path(..., ge=1),
    body: PumpConfigIn = Body(...),
    _=Depends(device_id_dep),
):
    return upsert_pump_config_put(pump_id, body)  # reutilizamos la misma lógica
