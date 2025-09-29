from __future__ import annotations

from fastapi import APIRouter, Depends, Path, Query, HTTPException
from typing import Optional, Dict, Any
from decimal import Decimal

from psycopg.rows import dict_row

from app.core.security import device_id_dep
from app.core.db import get_conn
from app.core.tenancy import require_org

router = APIRouter(prefix="/tanks", tags=["latest"])

def _to_float(v: Optional[Any]) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except Exception:
        return None

def _estimate_volume_l(capacity_m3: Optional[float], level_percent: Optional[float]) -> Optional[float]:
    if capacity_m3 is None or level_percent is None:
        return None
    pct = max(0.0, min(100.0, float(level_percent)))
    return round(capacity_m3 * 1000.0 * (pct / 100.0), 3)

@router.get("/{tank_id}/latest")
def latest_tank(
    tank_id: int = Path(..., ge=1),
    include_capacity: bool = Query(True, description="Incluir capacity_m3 en la respuesta"),
    _=Depends(device_id_dep),
):
    """
    Última lectura de un tanque **scopeada por organización**.
    """
    org_id = require_org()
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        # validar pertenencia por org y traer capacity_m3
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

        capacity_m3: Optional[float] = _to_float(trow.get("capacity_m3")) if include_capacity else None

        # intentar vista; si no existe, fallback a tabla
        row: Optional[Dict[str, Any]] = None
        try:
            cur.execute(
                """
                SELECT id, tank_id, ts, level_percent, volume_l, temperature_c, device_id, raw_json
                FROM public.v_tank_latest
                WHERE tank_id = %s
                """,
                (tank_id,),
            )
            r = cur.fetchone()
            row = dict(r) if r else None
        except Exception:
            cur.execute(
                """
                SELECT id, tank_id, ts, level_percent, volume_l, temperature_c, device_id, raw_json
                FROM public.tank_readings
                WHERE tank_id = %s
                ORDER BY ts DESC
                LIMIT 1
                """,
                (tank_id,),
            )
            r = cur.fetchone()
            row = dict(r) if r else None

    if not row:
        out: Dict[str, Any] = {
            "id": None,
            "tank_id": tank_id,
            "ts": None,
            "level_percent": None,
            "volume_l": None,
            "volume_source": None,
            "temperature_c": None,
            "device_id": None,
            "raw_json": None,
            "has_data": False,
        }
        if include_capacity:
            out["capacity_m3"] = capacity_m3
        return out

    level_percent = _to_float(row.get("level_percent"))
    volume_l_measured = _to_float(row.get("volume_l"))
    temperature_c = _to_float(row.get("temperature_c"))

    volume_l = volume_l_measured
    volume_source = "measured" if volume_l_measured is not None else None
    if volume_l is None:
        est = _estimate_volume_l(capacity_m3, level_percent)
        if est is not None:
            volume_l = est
            volume_source = "estimated"

    out: Dict[str, Any] = {
        "id": row.get("id"),
        "tank_id": row.get("tank_id"),
        "ts": row.get("ts"),
        "level_percent": level_percent,
        "volume_l": volume_l,
        "volume_source": volume_source,
        "temperature_c": temperature_c,
        "device_id": row.get("device_id"),
        "raw_json": row.get("raw_json"),
        "has_data": True,
    }
    if include_capacity:
        out["capacity_m3"] = capacity_m3
    return out
