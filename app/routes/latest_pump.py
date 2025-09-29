# app/routes/latest_pump.py
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Depends, Path
from psycopg.rows import dict_row

from app.core.security import device_id_dep
from app.auth.deps import conn_with_rls

router = APIRouter(prefix="/pumps", tags=["latest"])


@router.get("/{pump_id}/latest")
def latest_pump(
    pump_id: int = Path(..., ge=1),
    _=Depends(device_id_dep),
    conn=Depends(conn_with_rls),
):
    """
    - 200 con una fila SIEMPRE (has_data=false si no hay lecturas)
    - 404 solo si la bomba NO existe o no pertenece a la org actual
    """
    with conn.cursor(row_factory=dict_row) as cur:
        # 1) validar que la bomba exista y sea de la org del token
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

        # 2) intentar traer la última lectura desde la vista "full"
        try:
            cur.execute(
                "SELECT * FROM public.v_pump_latest_full WHERE pump_id = %s LIMIT 1",
                (pump_id,),
            )
            row = cur.fetchone()
            if row:
                return row
        except Exception:
            # si la vista no existe, seguimos al fallback
            pass

    # 3) fallback defensivo (sin lecturas / sin vista)
    return {
        "pump_id": pump["id"],
        "pump_name": pump["name"],
        "ts": None,
        "is_on": None,
        "flow_lpm": None,
        "pressure_bar": None,
        "voltage_v": None,
        "current_a": None,
        "control_mode": None,
        "manual_lockout": None,
        "raw_json": None,
        "has_data": False,
    }
