from __future__ import annotations

from fastapi import APIRouter, Depends, Path, Query, HTTPException
from typing import Optional, Dict, Any
from decimal import Decimal
from psycopg.rows import dict_row

from app.core.security import device_id_dep
from app.auth.deps import conn_with_rls

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
    conn=Depends(conn_with_rls),
):
    """
    Última lectura de un tanque, scopeado por organización.
    - 200 con has_data=false si no hay lecturas (usa v_tank_latest_full si existe).
    """
    with conn.cursor(row_factory=dict_row) as cur:
        # 1) Validar pertenencia por org_id directo (tanks tiene org_id)
        cur.execute(
            """
            SELECT id, capacity_m3
            FROM public.tanks
            WHERE id = %s
              AND org_id = current_setting('app.org_id')::bigint
            """,
            (tank_id,),
        )
        trow = cur.fetchone()
        if not trow:
            raise HTTPException(status_code=404, detail="tank not found")

        capacity_m3: Optional[float] = _to_float(trow.get("capacity_m3")) if include_capacity else None

        # 2) Intentar desde la vista "full"; fallback a tabla
        row = None
        try:
            cur.execute(
                """
                SELECT tank_id, tank_name, ts, level_percent, volume_l, temperature_c, raw_json, has_data
                FROM public.v_tank_latest_full
                WHERE tank_id = %s
                LIMIT 1
                """,
                (tank_id,),
            )
            r = cur.fetchone()
            if r:
                row = dict(r)
        except Exception:
            pass

        if not row:
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
            if r:
                row = dict(r)

    # 3) Sin lecturas
    if not row:
        out: Dict[str, Any] = {
            "tank_id": tank_id,
            "tank_name": None,
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

    # 4) Con lecturas: normalizar y estimar volumen si hace falta
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
        "tank_id": row.get("tank_id") or tank_id,
        "tank_name": row.get("tank_name"),
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
