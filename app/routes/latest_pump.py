# app/routes/latest_pump.py
from fastapi import APIRouter, HTTPException
from psycopg.rows import dict_row
from app.core.db import get_conn

router = APIRouter(tags=["latest"])

@router.get("/pumps/{pump_id}/latest")
def latest_pump(pump_id: int):
    """
    - 200 con una fila SIEMPRE (has_data=false si no hay lecturas)
    - 404 solo si la bomba NO existe
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        # 1) ¿existe la bomba?
        cur.execute("SELECT id, name FROM pumps WHERE id=%s;", (pump_id,))
        pump = cur.fetchone()
        if not pump:
            raise HTTPException(404, "Pump not found")

        # 2) Última lectura desde la vista "full"
        #    (devuelve una fila por bomba; ts y demás pueden ser NULL)
        try:
            cur.execute("SELECT * FROM v_pump_latest_full WHERE pump_id=%s;", (pump_id,))
            row = cur.fetchone()
            if row:
                return row
        except Exception:
            # Si la vista no existe, seguimos con el fallback de abajo
            pass

        # 3) Fallback defensivo (sin lecturas / sin vista)
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
