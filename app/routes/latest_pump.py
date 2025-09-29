# app/routes/latest_pump.py
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Depends, Path
from psycopg.rows import dict_row

from app.core.security import device_id_dep
from app.core.db import get_conn
from app.core.tenancy import require_org

router = APIRouter(prefix="/pumps", tags=["latest"])


@router.get("/{pump_id}/latest")
def latest_pump(
    pump_id: int = Path(..., ge=1),
    _=Depends(device_id_dep),
):
    """
    - 200 con una fila SIEMPRE (has_data=false si no hay lecturas)
    - 404 solo si la bomba NO existe o no pertenece a la org actual
    """
    org_id = require_org()

    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        # 1) Validar que la bomba exista y sea de la org actual
        cur.execute(
            """
            SELECT p.id, p.name
            FROM public.pumps p
            JOIN public.locations l ON l.id = p.location_id
            WHERE p.id = %s
              AND l.org_id = %s
            """,
            (pump_id, org_id),
        )
        pump = cur.fetchone()
        if not pump:
            raise HTTPException(status_code=404, detail="pump not found")

        # 2) Intentar traer la última lectura desde la vista "full" (si existe)
        row = None
        try:
            cur.execute(
                "SELECT * FROM public.v_pump_latest_full WHERE pump_id = %s LIMIT 1",
                (pump_id,),
            )
            row = cur.fetchone()
        except Exception:
            # si la vista no existe o falla, seguimos al fallback
            row = None

        # 3) Fallback directo a pump_readings (con org_id parametrizado)
        if not row:
            cur.execute(
                """
                SELECT
                    pr.pump_id,
                    p.name          AS pump_name,
                    pr.ts,
                    pr.is_on,
                    pr.flow_lpm,
                    pr.pressure_bar,
                    pr.voltage_v,
                    pr.current_a,
                    pr.control_mode,
                    pr.manual_lockout,
                    pr.extra        AS raw_json
                FROM public.pump_readings pr
                JOIN public.pumps p      ON p.id = pr.pump_id
                JOIN public.locations l  ON l.id = p.location_id
                WHERE pr.pump_id = %s
                  AND l.org_id = %s
                ORDER BY pr.ts DESC
                LIMIT 1
                """,
                (pump_id, org_id),
            )
            row = cur.fetchone()

    # 4) Respuesta consistente (si no hay lecturas, has_data=false)
    if row:
        row["has_data"] = True
        # asegurar campos presentes aunque la vista no los tenga
        row.setdefault("raw_json", None)
        row.setdefault("control_mode", None)
        row.setdefault("manual_lockout", None)
        row.setdefault("flow_lpm", None)
        row.setdefault("pressure_bar", None)
        row.setdefault("voltage_v", None)
        row.setdefault("current_a", None)
        return row

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
