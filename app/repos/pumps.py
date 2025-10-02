from app.core.db import get_conn
import json

# =========================
# Lecturas de bombas (R/W)
# =========================

def insert_pump_reading(device_id: int, payload) -> int:
    """
    Inserta una lectura para una bomba *de la organización actual*.
    - Valida que la bomba exista en la org.
    - Coloca org_id derivado de la propia bomba (no depende de GUC).
    """
    from fastapi import HTTPException

    with get_conn() as conn, conn.cursor() as cur:
        # 1) Validar pertenencia de la bomba a la org actual
        cur.execute(
            """
            SELECT org_id
            FROM public.pumps
            WHERE id = %s
              AND org_id = COALESCE(current_setting('app.org_id', true), '-1')::bigint
            """,
            (payload.pump_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Bomba no encontrada en esta organización")
        org_id = row[0]

        # 2) Insertar lectura asegurando org_id consistente con la bomba
        cur.execute(
            """
            INSERT INTO public.pump_readings (
                org_id, pump_id, device_id, ts,
                is_on, flow_lpm, pressure_bar, voltage_v, current_a,
                control_mode, manual_lockout, raw_json
            )
            VALUES (%s, %s, %s, COALESCE(%s, now()),
                    %s, %s, %s, %s, %s,
                    %s, %s, %s)
            RETURNING id
            """,
            (
                org_id,
                payload.pump_id, device_id, payload.ts,
                payload.is_on, payload.flow_lpm, payload.pressure_bar,
                payload.voltage_v, payload.current_a,
                payload.control_mode, payload.manual_lockout,
                json.dumps(payload.extra) if getattr(payload, "extra", None) else None,
            ),
        )
        new_id = cur.fetchone()[0]
        conn.commit()
    return new_id


def latest_pump_row(pump_id: int):
    """
    Última lectura de una bomba, restringida a la organización actual.
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.id, r.ts, r.is_on, r.flow_lpm, r.pressure_bar, r.voltage_v, r.current_a,
                   r.control_mode, r.manual_lockout, r.raw_json
            FROM public.pump_readings r
            JOIN public.pumps p ON p.id = r.pump_id
            WHERE r.pump_id = %s
              AND p.org_id = COALESCE(current_setting('app.org_id', true), '-1')::bigint
            ORDER BY r.ts DESC
            LIMIT 1
            """,
            (pump_id,),
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
    """
    Historial (últimos N) de una bomba dentro de la organización actual.
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.ts, r.is_on, r.flow_lpm, r.pressure_bar, r.voltage_v, r.current_a,
                   r.control_mode, r.manual_lockout
            FROM public.pump_readings r
            JOIN public.pumps p ON p.id = r.pump_id
            WHERE r.pump_id = %s
              AND p.org_id = COALESCE(current_setting('app.org_id', true), '-1')::bigint
            ORDER BY r.ts DESC
            LIMIT %s
            """,
            (pump_id, limit),
        )
        rows = cur.fetchall()

    # Se invierte para devolver cronológico ascendente
    rows = rows[::-1]
    return [
        {
            "ts": r[0],
            "is_on": r[1],
            "flow_lpm": r[2],
            "pressure_bar": r[3],
            "voltage_v": r[4],
            "current_a": r[5],
            "control_mode": r[6],
            "manual_lockout": r[7],
        }
        for r in rows
    ]


# =========================
# Bombas (catálogo / config)
# =========================

def list_pumps():
    """
    Lista de bombas SOLO de la organización actual.
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.id, p.name, p.model, p.max_flow_lpm
            FROM public.pumps p
            WHERE p.org_id = COALESCE(current_setting('app.org_id', true), '-1')::bigint
            ORDER BY p.id
            """
        )
        rows = cur.fetchall()
    return [{"id": r[0], "name": r[1], "model": r[2], "max_flow_lpm": r[3]} for r in rows]


def list_pumps_with_config():
    """
    Lista bombas + configuración SOLO de la organización actual.
    (Evita depender de una vista que no filtre por org.)
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              p.id, p.name, p.model, p.max_flow_lpm,
              c.drive_type, c.remote_enabled,
              c.vfd_min_speed_pct, c.vfd_max_speed_pct, c.vfd_default_speed_pct
            FROM public.pumps p
            LEFT JOIN public.pump_config c ON c.pump_id = p.id
            WHERE p.org_id = COALESCE(current_setting('app.org_id', true), '-1')::bigint
            ORDER BY p.id
            """
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]


def upsert_pump_config(pump_id: int, body) -> dict:
    """
    Upsert de configuración de bomba, validando pertenencia a la organización.
    """
    from fastapi import HTTPException

    with get_conn() as conn, conn.cursor() as cur:
        # Validar que la bomba existe en la org actual
        cur.execute(
            """
            SELECT 1
            FROM public.pumps
            WHERE id = %s
              AND org_id = COALESCE(current_setting('app.org_id', true), '-1')::bigint
            """,
            (pump_id,),
        )
        if cur.fetchone() is None:
            raise HTTPException(404, "Bomba no encontrada en esta organización")

        # Validación simple de rangos VFD
        if body.vfd_min_speed_pct is not None and body.vfd_max_speed_pct is not None:
            if body.vfd_min_speed_pct > body.vfd_max_speed_pct:
                raise HTTPException(400, "vfd_min_speed_pct debe ser <= vfd_max_speed_pct")

        # Hacer UPSERT; si tu tabla pump_config tiene org_id, se recomienda setearlo del pump.
        # Aun si no lo tuviera, la pertenencia ya quedó validada arriba.
        cur.execute(
            """
            INSERT INTO public.pump_config (
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
            """,
            (
                pump_id,
                body.drive_type, body.remote_enabled,
                body.vfd_min_speed_pct, body.vfd_max_speed_pct, body.vfd_default_speed_pct,
            ),
        )
        row = cur.fetchone()
        conn.commit()

    cols = [
        "pump_id",
        "drive_type",
        "remote_enabled",
        "vfd_min_speed_pct",
        "vfd_max_speed_pct",
        "vfd_default_speed_pct",
        "updated_at",
    ]
    return dict(zip(cols, row))


def get_normalized_pump_config(pump_id: int):
    """
    Devuelve config normalizada de una bomba en la organización actual.
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.id, c.drive_type, c.remote_enabled,
                   c.vfd_min_speed_pct, c.vfd_max_speed_pct, c.vfd_default_speed_pct
            FROM public.pumps p
            LEFT JOIN public.pump_config c ON c.pump_id = p.id
            WHERE p.id = %s
              AND p.org_id = COALESCE(current_setting('app.org_id', true), '-1')::bigint
            """,
            (pump_id,),
        )
        return cur.fetchone()


def get_last_pump_reading(pump_id: int):
    """
    Atajo: última lectura (subset de columnas) para una bomba de la organización actual.
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.ts, r.control_mode, r.manual_lockout, r.raw_json
            FROM public.pump_readings r
            JOIN public.pumps p ON p.id = r.pump_id
            WHERE r.pump_id = %s
              AND p.org_id = COALESCE(current_setting('app.org_id', true), '-1')::bigint
            ORDER BY r.ts DESC
            LIMIT 1
            """,
            (pump_id,),
        )
        return cur.fetchone()
