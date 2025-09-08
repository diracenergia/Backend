from app.core.db import get_conn
import json

def insert_pump_reading(device_id: int, payload) -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pump_readings (
                pump_id, device_id, ts,
                is_on, flow_lpm, pressure_bar, voltage_v, current_a,
                control_mode, manual_lockout, raw_json
            )
            VALUES (%s, %s, COALESCE(%s, now()),
                    %s, %s, %s, %s, %s,
                    %s, %s, %s)
            RETURNING id
            """,
            (payload.pump_id, device_id, payload.ts,
             payload.is_on, payload.flow_lpm, payload.pressure_bar,
             payload.voltage_v, payload.current_a,
             payload.control_mode, payload.manual_lockout,
             json.dumps(payload.extra) if payload.extra else None)
        )
        new_id = cur.fetchone()[0]
        conn.commit()
    return new_id

def latest_pump_row(pump_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, ts, is_on, flow_lpm, pressure_bar, voltage_v, current_a,
                   control_mode, manual_lockout, raw_json
            FROM pump_readings
            WHERE pump_id=%s
            ORDER BY ts DESC
            LIMIT 1
            """,
            (pump_id,)
        )
        row = cur.fetchone()
    if not row:
        return None
    rid, ts, is_on, flow, pres, volt, curr, mode, lockout, raw = row
    return {
        "id": pump_id,
        "ts": ts,
        "is_on": is_on,
        "flow_lpm": flow,
        "pressure_bar": pres,
        "voltage_v": volt,
        "current_a": curr,
        "control_mode": mode,
        "manual_lockout": lockout,
        "extra": raw,
        "reading_id": rid,
    }

def pump_history_rows(pump_id: int, limit: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT ts, is_on, flow_lpm, pressure_bar, voltage_v, current_a,
                   control_mode, manual_lockout
            FROM pump_readings
            WHERE pump_id=%s
            ORDER BY ts DESC
            LIMIT %s
            """,
            (pump_id, limit)
        )
        rows = cur.fetchall()
    rows = rows[::-1]
    return [
        {"ts": r[0], "is_on": r[1], "flow_lpm": r[2], "pressure_bar": r[3],
         "voltage_v": r[4], "current_a": r[5], "control_mode": r[6], "manual_lockout": r[7]}
        for r in rows
    ]

def list_pumps():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id, name, model, max_flow_lpm FROM pumps ORDER BY id")
        rows = cur.fetchall()
    return [{"id": r[0], "name": r[1], "model": r[2], "max_flow_lpm": r[3]} for r in rows]

def list_pumps_with_config():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, name, model, max_flow_lpm,
                   drive_type, remote_enabled,
                   vfd_min_speed_pct, vfd_max_speed_pct, vfd_default_speed_pct
            FROM v_pumps_with_config
            ORDER BY id
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]

def upsert_pump_config(pump_id: int, body) -> dict:
    from fastapi import HTTPException
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pumps WHERE id=%s", (pump_id,))
        if cur.fetchone() is None:
            raise HTTPException(404, "Bomba no encontrada")

        if body.vfd_min_speed_pct is not None and body.vfd_max_speed_pct is not None:
            if body.vfd_min_speed_pct > body.vfd_max_speed_pct:
                raise HTTPException(400, "vfd_min_speed_pct debe ser <= vfd_max_speed_pct")

        cur.execute("""
            INSERT INTO pump_config(
                pump_id, drive_type, remote_enabled,
                vfd_min_speed_pct, vfd_max_speed_pct, vfd_default_speed_pct
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (pump_id) DO UPDATE SET
                drive_type = COALESCE(EXCLUDED.drive_type, pump_config.drive_type),
                remote_enabled = COALESCE(EXCLUDED.remote_enabled, pump_config.remote_enabled),
                vfd_min_speed_pct = COALESCE(EXCLUDED.vfd_min_speed_pct, pump_config.vfd_min_speed_pct),
                vfd_max_speed_pct = COALESCE(EXCLUDED.vfd_max_speed_pct, pump_config.vfd_max_speed_pct),
                vfd_default_speed_pct = COALESCE(EXCLUDED.vfd_default_speed_pct, pump_config.vfd_default_speed_pct),
                updated_at = now()
            RETURNING pump_id, drive_type, remote_enabled,
                      vfd_min_speed_pct, vfd_max_speed_pct, vfd_default_speed_pct, updated_at
        """, (
            pump_id,
            body.drive_type, body.remote_enabled,
            body.vfd_min_speed_pct, body.vfd_max_speed_pct, body.vfd_default_speed_pct
        ))
        row = cur.fetchone(); conn.commit()

    cols = ["pump_id","drive_type","remote_enabled",
            "vfd_min_speed_pct","vfd_max_speed_pct","vfd_default_speed_pct","updated_at"]
    return dict(zip(cols, row))

def get_normalized_pump_config(pump_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
          SELECT id, drive_type, remote_enabled,
                 vfd_min_speed_pct, vfd_max_speed_pct, vfd_default_speed_pct
          FROM v_pumps_with_config
          WHERE id = %s
        """, (pump_id,))
        return cur.fetchone()

def get_last_pump_reading(pump_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
          SELECT ts, control_mode, manual_lockout, raw_json
          FROM pump_readings
          WHERE pump_id=%s
          ORDER BY ts DESC
          LIMIT 1
        """, (pump_id,))
        return cur.fetchone()
