# app/routes/history_pump.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, Path, Query, HTTPException
from psycopg.rows import dict_row

from app.core.security import device_id_dep
from app.core.db import get_conn
from app.core.tenancy import require_org

router = APIRouter(prefix="/pumps", tags=["history"])


@router.get("/{pump_id}/history")
def pump_history(
    pump_id: int = Path(..., ge=1),
    since: Optional[datetime] = Query(None, description="ISO8601, por defecto ahora-48h (UTC)"),
    until: Optional[datetime] = Query(None, description="ISO8601, por defecto ahora (UTC)"),
    limit: int = Query(2000, ge=1, le=10000),
    _=Depends(device_id_dep),
) -> List[Dict[str, Any]]:
    """
    Lecturas históricas de la bomba (scopeadas por organización).
    - Valida que la bomba pertenezca a la org actual.
    - Devuelve a lo sumo `limit` filas ordenadas por ts DESC.
    """
    org_id = require_org()
    now = datetime.now(timezone.utc)
    if until is None:
        until = now
    if since is None:
        since = until - timedelta(hours=48)

    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        # Validar pertenencia
        cur.execute(
            """
            SELECT 1
            FROM public.pumps p
            JOIN public.locations l ON l.id = p.location_id
            WHERE p.id = %s AND l.org_id = %s
            """,
            (pump_id, org_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="pump not found")

        # Traer historia
        cur.execute(
            """
            SELECT
              id, pump_id, ts,
              is_on, flow_lpm, pressure_bar, voltage_v, current_a,
              control_mode, manual_lockout,
              extra AS raw_json
            FROM public.pump_readings
            WHERE pump_id = %s
              AND ts BETWEEN %s AND %s
            ORDER BY ts DESC
            LIMIT %s
            """,
            (pump_id, since, until, limit),
        )
        rows = cur.fetchall() or []

    return rows
