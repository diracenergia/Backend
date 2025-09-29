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
    - 404 si la bomba no pertenece a la org actual
    """
    with conn.cursor(row_factory=dict_row) as cur:
        # 1) Validar org_id directo (pumps tiene org_id)
        cur.execute(
            """
            SELECT id, name
            FROM public.pumps
            WHERE id = %s
              AND org_id = current_setting('app.org_id')::bigint
            """,
            (pump_id,),
        )
        pump = cur.fetchone()
        if not pump:
            raise HTTPException(404, "pump not found")

        # 2) Vista "full" si existe
        try:
            cur.execute(
                """
                SELECT pump_id, pump_name, ts, is_on, flow_lpm, pressure_bar, voltage_v, current_a,
                       control_mode, manual_lockout, raw_json, has_data
                FROM public.v_pump_latest_full
                WHERE pump_id = %s
                LIMIT 1
                """,
                (pump_id,),
            )
            row = cur.fetchone()
            if row:
                return row
        except Exception:
            pass

    # 3) Fallback defensivo
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
