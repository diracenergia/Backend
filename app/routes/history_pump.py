# app/routes/history_pump.py
from __future__ import annotations

from fastapi import APIRouter, Depends, Path, Query, HTTPException
from typing import Optional, Dict, Any, List, Literal
from decimal import Decimal
from psycopg.rows import dict_row

from app.core.security import device_id_dep
from app.auth.deps import conn_with_rls

router = APIRouter(prefix="/pumps", tags=["history"])


def _to_float(v: Optional[Any]) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except Exception:
        return None


@router.get("/{pump_id}/history")
def pump_history(
    pump_id: int = Path(..., ge=1),

    # rangos de tiempo (cadenas ISO 8601 / 'YYYY-MM-DD' o timestamptz parseable por PG)
    since: Optional[str] = Query(None, description="ISO 8601 o YYYY-MM-DD"),
    until: Optional[str] = Query(None, description="ISO 8601 o YYYY-MM-DD"),

    # paginación y orden
    limit: int = Query(200, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    order: Literal["asc", "desc"] = Query("asc"),

    # salida
    flat: bool = Query(True, description="Si true, devuelve solo el array de lecturas"),

    _=Depends(device_id_dep),
    conn=Depends(conn_with_rls),
):
    """
    Devuelve el historial de lecturas de una bomba.
    - 404 si la bomba no existe o no pertenece a la org actual (RLS/tenant).
    - Soporta filtros de tiempo, orden, paginación y salida 'flat'.
    """
    with conn.cursor(row_factory=dict_row) as cur:
        # 1) validar existencia + pertenencia a la org del token
        cur.execute(
            """
            SELECT p.id, p.name
            FROM public.pumps p
            JOIN public.locations l ON l.id = p.location_id
            WHERE p.id = %s
              AND l.org_id = current_setting('app.org_id')::bigint
            """,
            (pump_id,),
        )
        pump = cur.fetchone()
        if not pump:
            raise HTTPException(404, "pump not found")

        # 2) armar query con filtros opcionales
        sql = [
            """
            SELECT
              r.id, r.pump_id, r.ts,
              r.is_on, r.flow_lpm, r.pressure_bar,
              r.voltage_v, r.current_a,
              r.control_mode, r.manual_lockout,
              r.device_id, r.raw_json
            FROM public.pump_readings r
            WHERE r.pump_id = %s
            """
        ]
        params: List[Any] = [pump_id]

        if since:
            sql.append("AND r.ts >= %s")
            params.append(since)
        if until:
            sql.append("AND r.ts <= %s")
            params.append(until)

        sql.append(f"ORDER BY r.ts {'ASC' if order == 'asc' else 'DESC'}")
        sql.append("LIMIT %s OFFSET %s")
        params.extend([limit, offset])

        cur.execute(" ".join(sql), params)
        rows = cur.fetchall() or []

    # 3) normalizar numéricos -> float
    items: List[Dict[str, Any]] = []
    for r in rows:
        items.append({
            "id": r.get("id"),
            "pump_id": r.get("pump_id"),
            "ts": r.get("ts"),
            "is_on": r.get("is_on"),
            "flow_lpm": _to_float(r.get("flow_lpm")),
            "pressure_bar": _to_float(r.get("pressure_bar")),
            "voltage_v": _to_float(r.get("voltage_v")),
            "current_a": _to_float(r.get("current_a")),
            "control_mode": r.get("control_mode"),
            "manual_lockout": r.get("manual_lockout"),
            "device_id": r.get("device_id"),
            "raw_json": r.get("raw_json"),
        })

    if flat:
        return items

    return {
        "pump_id": pump_id,
        "count": len(items),
        "limit": limit,
        "offset": offset,
        "order": order,
        "since": since,
        "until": until,
        "items": items,
    }
