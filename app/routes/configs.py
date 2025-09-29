from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Path, Body, HTTPException
from psycopg.rows import dict_row

from app.core.security import device_id_dep
from app.core.db import get_conn
from app.core.tenancy import require_org
from app.schemas.configs import TankConfigIn, TankConfigOut

router = APIRouter(prefix="/tanks", tags=["config"])

@router.get("/config")
def list_configs(_=Depends(device_id_dep)):
    """
    Devuelve todas las configs visibles para la organización actual.
    """
    org_id = require_org()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                t.id          AS tank_id,
                t.name        AS name,
                t.capacity_m3 AS capacity_m3,
                c.low_pct,
                c.low_low_pct,
                c.high_pct,
                c.high_high_pct,
                c.updated_by,
                c.updated_at
            FROM public.tanks t
            JOIN public.asset_locations al
              ON al.asset_type = 'tank' AND al.asset_id = t.id
            JOIN public.locations l
              ON l.id = al.location_id
            LEFT JOIN public.tank_configs c
              ON c.tank_id = t.id
            WHERE l.org_id = %s
            ORDER BY t.id;
            """,
            (org_id,),
        )
        return cur.fetchall() or []

@router.get("/{tank_id}/config", response_model=TankConfigOut)
def get_config(
    tank_id: int = Path(..., ge=1),
    _=Depends(device_id_dep),
):
    org_id = require_org()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        # validar pertenencia por org usando asset_locations
        cur.execute(
            """
            SELECT t.id, t.capacity_m3
            FROM public.tanks t
            JOIN public.asset_locations al
              ON al.asset_type='tank' AND al.asset_id=t.id
            JOIN public.locations l
              ON l.id = al.location_id
            WHERE t.id = %s AND l.org_id = %s
            """,
            (tank_id, org_id),
        )
        trow = cur.fetchone()
        if not trow:
            raise HTTPException(status_code=404, detail="tank not found")

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

@router.put("/{tank_id}/config", response_model=TankConfigOut)
def upsert_config(
    tank_id: int = Path(..., ge=1),
    payload: TankConfigIn = Body(...),
    _=Depends(device_id_dep),
):
    org_id = require_org()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        # validar pertenencia por org
        cur.execute(
            """
            SELECT 1
            FROM public.tanks t
            JOIN public.asset_locations al
              ON al.asset_type='tank' AND al.asset_id=t.id
            JOIN public.locations l
              ON l.id = al.location_id
            WHERE t.id = %s AND l.org_id = %s
            """,
            (tank_id, org_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="tank not found")

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
                getattr(payload, "updated_by", None),
            ),
        )
        return cur.fetchone()
