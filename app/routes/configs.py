# app/routes/configs.py
from __future__ import annotations

from fastapi import APIRouter, Depends, Path, Body, HTTPException
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from psycopg.rows import dict_row

from app.core.security import device_id_dep
from app.core.db import get_conn
from app.core.tenancy import require_org, get_user_id
from app.schemas.configs import TankConfigIn, TankConfigOut

router = APIRouter(prefix="/tanks", tags=["config"])

# 1) LISTA TODAS LAS CONFIGS (scopeadas por org)
@router.get("/config")
def list_configs(
    _=Depends(device_id_dep),
) -> List[Dict[str, Any]]:
    """
    Devuelve todas las configs visibles para la organización actual.
    Incluye: tank_id, name, capacity_m3 y los umbrales (si existen).
    """
    org_id = require_org()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                t.id               AS tank_id,
                t.name             AS name,
                t.capacity_m3      AS capacity_m3,
                c.low_pct,
                c.low_low_pct,
                c.high_pct,
                c.high_high_pct,
                c.updated_by,
                c.updated_at
            FROM public.tanks t
            JOIN public.locations l
              ON l.id = t.location_id
            LEFT JOIN public.tank_configs c
              ON c.tank_id = t.id
            WHERE l.org_id = %s
            ORDER BY t.id
            """,
            (org_id,),
        )
        rows = cur.fetchall() or []
    return rows

# 2) LEE UNA CONFIG (valida org del tanque)
@router.get("/{tank_id}/config", response_model=TankConfigOut)
def get_config(
    tank_id: int = Path(..., ge=1),
    _=Depends(device_id_dep),
):
    org_id = require_org()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        # Validar pertenencia del tanque a la org
        cur.execute(
            """
            SELECT t.id, t.capacity_m3
            FROM public.tanks t
            JOIN public.locations l ON l.id = t.location_id
            WHERE t.id = %s
              AND l.org_id = %s
            """,
            (tank_id, org_id),
        )
        trow = cur.fetchone()
        if not trow:
            raise HTTPException(status_code=404, detail="tank not found")

        # Traer config (si existe)
        cur.execute(
            """
            SELECT tank_id, low_pct, low_low_pct, high_pct, high_high_pct, updated_by, updated_at
            FROM public.tank_configs
            WHERE tank_id = %s
            """,
            (tank_id,),
        )
        cfg = cur.fetchone()

    if not cfg:
        # devolver shape completo, con updated_at presente (schema lo requiere)
        return {
            "tank_id": tank_id,
            "low_pct": None,
            "low_low_pct": None,
            "high_pct": None,
            "high_high_pct": None,
            "updated_by": None,
            "updated_at": datetime.now(timezone.utc),
        }
    return cfg

# 3) UPSERT (valida org del tanque + ON CONFLICT)
@router.put("/{tank_id}/config", response_model=TankConfigOut)
def upsert_config(
    tank_id: int = Path(..., ge=1),
    payload: TankConfigIn = Body(...),
    _=Depends(device_id_dep),
):
    org_id = require_org()
    # si tenés usuario autenticado, usalo; si no, respetá el del payload si viene
    updated_by = get_user_id() or getattr(payload, "updated_by", None)

    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        # Validar que el tanque sea de la org actual
        cur.execute(
            """
            SELECT 1
            FROM public.tanks t
            JOIN public.locations l ON l.id = t.location_id
            WHERE t.id = %s
              AND l.org_id = %s
            """,
            (tank_id, org_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="tank not found")

        # Upsert de la config
        cur.execute(
            """
            INSERT INTO public.tank_configs
                (tank_id, low_pct, low_low_pct, high_pct, high_high_pct, updated_by, updated_at)
            VALUES
                (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (tank_id) DO UPDATE SET
                low_pct       = EXCLUDED.low_pct,
                low_low_pct   = EXCLUDED.low_low_pct,
                high_pct      = EXCLUDED.high_pct,
                high_high_pct = EXCLUDED.high_high_pct,
                updated_by    = EXCLUDED.updated_by,
                updated_at    = now()
            RETURNING tank_id, low_pct, low_low_pct, high_pct, high_high_pct, updated_by, updated_at
            """,
            (
                tank_id,
                payload.low_pct,
                payload.low_low_pct,
                payload.high_pct,
                payload.high_high_pct,
                updated_by,
            ),
        )
        row = cur.fetchone()

    return row
