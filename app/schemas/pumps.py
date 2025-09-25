# app/repos/pumps.py
from typing import Optional, Any
from app.core.db import get_conn
import json

def insert_pump_reading(
    device_id: Optional[int],
    payload: Any,
    org_id: Optional[int] = None,
) -> int:
    with get_conn() as conn, conn.cursor() as cur:
        # Seteamos la GUC por si algún trigger/DEFAULT la usa (no hace daño)
        if org_id is not None:
            cur.execute("SELECT set_config('app.org_id', %s, true)", (str(org_id),))

        # Si tenemos org_id válido => lo incluimos como columna explícita (sin ::bigint)
        if org_id is not None:
            cur.execute(
                """
                INSERT INTO public.pump_readings (
                    pump_id, device_id, org_id, ts,
                    is_on, flow_lpm, pressure_bar, voltage_v, current_a,
                    control_mode, manual_lockout, raw_json
                )
                VALUES (%s, %s, %s, COALESCE(%s, now()),
                        %s, %s, %s, %s, %s,
                        %s, %s, %s)
                RETURNING id
                """,
                (
                    payload.pump_id,
                    device_id,
                    org_id,  # 👈 va como parámetro normal, sin cast
                    getattr(payload, "ts", None),
                    getattr(payload, "is_on", None),
                    getattr(payload, "flow_lpm", None),
                    getattr(payload, "pressure_bar", None),
                    getattr(payload, "voltage_v", None),
                    getattr(payload, "current_a", None),
                    getattr(payload, "control_mode", None),
                    getattr(payload, "manual_lockout", None),
                    json.dumps(getattr(payload, "extra", None)) if getattr(payload, "extra", None) else None,
                ),
            )
        else:
            # Sin org_id: dejamos que corra DEFAULT/trigger
            cur.execute(
                """
                INSERT INTO public.pump_readings (
                    pump_id, device_id, ts,
                    is_on, flow_lpm, pressure_bar, voltage_v, current_a,
                    control_mode, manual_lockout, raw_json
                )
                VALUES (%s, %s, COALESCE(%s, now()),
                        %s, %s, %s, %s, %s,
                        %s, %s, %s)
                RETURNING id
                """,
                (
                    payload.pump_id,
                    device_id,
                    getattr(payload, "ts", None),
                    getattr(payload, "is_on", None),
                    getattr(payload, "flow_lpm", None),
                    getattr(payload, "pressure_bar", None),
                    getattr(payload, "voltage_v", None),
                    getattr(payload, "current_a", None),
                    getattr(payload, "control_mode", None),
                    getattr(payload, "manual_lockout", None),
                    json.dumps(getattr(payload, "extra", None)) if getattr(payload, "extra", None) else None,
                ),
            )

        rid = cur.fetchone()[0]
        conn.commit()
        return rid
