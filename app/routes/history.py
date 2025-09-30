from __future__ import annotations

from fastapi import APIRouter, Depends, Path, Query, HTTPException
from typing import Optional, Dict, Any, List, Literal
from decimal import Decimal

from psycopg.rows import dict_row

from app.core.security import device_id_dep
from app.core.db import get_conn
from app.core.tenancy import require_org

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
    date_from: Optional[str] = Query(None, description="ISO 8601 o YYYY-MM-DD (alias de 'since')"),
    date_to:   Optional[str] = Query(None, description="ISO 8601 o YYYY-MM-DD (alias de 'until')"),
    since:     Optional[str] = Query(None, description="ISO 8601 o YYYY-MM-DD (preferido)"),
    until:     Optional[str] = Query(None, description="ISO 8601 o YYYY-MM-DD"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    order: Literal["asc", "desc"] = Query("asc", description="Orden temporal en la respuesta"),
    include_capacity: bool = Query(True, description="Incluir capacity_m3 en la respuesta"),
    estimate_missing_volume: bool = Query(True, description="Estimar volume_l cuando no hay medición"),
    flat: bool = Query(True, description="Si true, devuelve solo el array de lecturas"),
    _=Depends(device_id_dep),
):
    org_id = require_org()
    df = since or date_from
    dt = until or date_to
    ord_sql = "ASC" if str(order).lower() == "asc" else "DESC"

    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        # validar acceso del tanque por org y (opcionalmente) obtener capacity_m3
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

        # historial
        params: List[Any] = [tank_id]
        where = ["tank_id = %s"]
        if df:
            where.append("ts >= %s::timestamptz")
            params.append(df)
        if dt:
            where.append("ts < %s::timestamptz")
            params.append(dt)
        params.extend([limit, offset])

        sql = f"""
            SELECT id, tank_id, ts, level_percent, volume_l, temperature_c, device_id, raw_json
            FROM public.tank_readings
            WHERE {' AND '.join(where)}
            ORDER BY ts {ord_sql}
            LIMIT %s OFFSET %s
        """
        cur.execute(sql, tuple(params))
        rows = cur.fetchall() or []

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
            "ts": r.get("ts"),
            "level_percent": lvl,
            "volume_l": vol,
            "volume_source": vsrc,
            "temperature_c": tmp,
            "device_id": r.get("device_id"),
            "raw_json": r.get("raw_json"),
        })

    if flat:
        return items

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
