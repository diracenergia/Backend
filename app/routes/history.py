# app/routes/history.py
from fastapi import APIRouter, Depends, Path, Query
from typing import Optional, Dict, Any, List, Literal
from decimal import Decimal

from app.repos import tanks as repo
from app.core.security import device_id_dep

router = APIRouter(prefix="/tanks", tags=["history"])

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

@router.get("/{tank_id}/history")
def history_tank(
    tank_id: int = Path(..., ge=1),

    # Aceptamos ambas variantes para compatibilidad:
    date_from: Optional[str] = Query(None, description="ISO 8601 o YYYY-MM-DD (alias de 'since')"),
    date_to:   Optional[str] = Query(None, description="ISO 8601 o YYYY-MM-DD (alias de 'until')"),
    since:     Optional[str] = Query(None, description="ISO 8601 o YYYY-MM-DD (preferido)"),
    until:     Optional[str] = Query(None, description="ISO 8601 o YYYY-MM-DD"),

    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),

    order: Literal["asc", "desc"] = Query("asc", description="Orden temporal deseado en la respuesta"),
    include_capacity: bool = Query(True, description="Incluir capacity_m3 en la respuesta"),
    estimate_missing_volume: bool = Query(True, description="Estimar volume_l cuando no hay medición"),
    flat: bool = Query(True, description="Si true, devuelve solo el array de lecturas (compat con front)"),

    _=Depends(device_id_dep),
):
    # Resolver alias de rangos
    df = since or date_from
    dt = until or date_to

    # Traer filas desde el repo (el repo hoy ordena DESC por defecto)
    rows = repo.history_tank_rows(
        tank_id=tank_id,
        date_from=df,
        date_to=dt,
        limit=limit,
        offset=offset,
    ) or []

    # Si piden asc y el repo entregó desc, invertimos acá
    # (Si más adelante actualizás el repo para soportar 'order', podés quitar este reverse)
    if order == "asc":
        rows = list(reversed(rows))

    capacity_m3 = repo.get_tank_capacity_m3(tank_id) if include_capacity else None

    items: List[Dict[str, Any]] = []
    for r in rows:
        lvl = _to_float(r.get("level_percent"))
        vol_measured = _to_float(r.get("volume_l"))
        tmp = _to_float(r.get("temperature_c"))

        vol = vol_measured
        vsrc = "measured" if vol_measured is not None else None
        if vol is None and estimate_missing_volume:
            est = _estimate_volume_l(capacity_m3, lvl)
            if est is not None:
                vol = est
                vsrc = "estimated"

        items.append({
            "id": r.get("id"),
            "tank_id": r.get("tank_id"),
            "ts": r.get("ts"),  # timestamptz → ISO8601; el front ya lo normaliza a ms
            "level_percent": lvl,
            "volume_l": vol,
            "volume_source": vsrc,
            "temperature_c": tmp,
            "device_id": r.get("device_id"),
            "raw_json": r.get("raw_json"),
        })

    # flat=true → devolvemos array directo (lo que espera api.tankHistory)
    if flat:
        return items

    # flat=false → devolvemos metadatos y items
    out: Dict[str, Any] = {
        "tank_id": tank_id,
        "count": len(items),
        "limit": limit,
        "offset": offset,
        "order": order,
        "items": items,
    }
    if include_capacity:
        out["capacity_m3"] = capacity_m3
    if df:
        out["since"] = df
    if dt:
        out["until"] = dt
    return out
