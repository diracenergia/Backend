# app/routes/latest.py
from fastapi import APIRouter, Depends, Path, Query
from typing import Optional, Dict, Any
from decimal import Decimal

from app.repos import tanks as repo
from app.core.security import device_id_dep

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
    # clamp 0..100
    pct = max(0.0, min(100.0, float(level_percent)))
    return round(capacity_m3 * 1000.0 * (pct / 100.0), 3)

@router.get("/{tank_id}/latest")
def latest_tank(
    tank_id: int = Path(..., ge=1),
    include_capacity: bool = Query(True, description="Incluir capacity_m3 en la respuesta"),
    _=Depends(device_id_dep),
):
    # Intentamos traer la última lectura
    row = repo.latest_tank_row(tank_id)

    # Traemos capacity una sola vez (sirve para estimar volumen y para el front)
    capacity_m3 = repo.get_tank_capacity_m3(tank_id) if include_capacity else None

    # Si no hay lecturas, devolvemos payload vacío y has_data=false (200 OK)
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

    # Hay lectura: normalizamos y calculamos volumen si es necesario
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
