# app/repos/pumps.py
from typing import Optional, List, Dict, Any
from app.core.db import get_conn
from fastapi import HTTPException
import json


# ---------- helpers de acceso por organización ----------

def _pump_belongs_to_org(cur, pump_id: int, org_id: int) -> bool:
    """
    Verifica que la bomba esté en alguna location de la organización dada.
    Si tu tabla pumps tiene p.org_id, podés reemplazar por: SELECT 1 FROM pumps WHERE id=%s AND org_id=%s
    """
    cur.execute(
        """
        SELECT 1
        FROM public.asset_locations al
        JOIN public.locations l ON l.id = al.location_id
        WHERE al.asset_type = 'pump'
          AND al.asset_id   = %s
          AND l.org_id      = %s
        LIMIT 1
        """,
        (pump_id, org_id),
    )
    return cur.fetchone() is not None


def _org_filter_sql() -> str:
    """
    Devuelve un WHERE que asegura pertenencia a la org por medio de locations.
    Úsalo si NO tenés p.org_id en la tabla pumps.
    """
    return """
      EXISTS (
        SELECT 1
        FROM public.asset_locations al2
        JOIN public.locations l2 ON l2.id = al2.location_id
        WHERE al2.asset_type = 'pump'
          AND al2.asset_id   = p.id
          AND l2.org_id      = %(org_id)s
      )
    """


# ---------- lecturas / escritura ----------

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


def latest_pump_row(pump_id: int, org_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    with get_conn() as conn, conn.cursor() as cur:
        if org_id:
            if not _pump_belongs_to_org(cur, pump_id, org_id):
                # Podés devolver None y que la ruta haga 404 si preferís
                raise HTTPException(status_code=404, detail="Pump not found")

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


def pump_history_rows(pump_id: int, limit: int, org_id: Optional[int] = None) -> List[Dict[str, Any]]:
    with get_conn() as conn, conn.cursor() as cur:
        if org_id:
            if not _pump_belongs_to_org(cur, pump_id, org_id):
                raise HTTPException(status_code=404, detail="Pump not found")

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


def list_pumps(org_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Lista plana de bombas. Si se pasa org_id, filtra estrictamente a esa organización.
    """
    with get_conn() as conn, conn.cursor() as cur:
        if org_id:
            cur.execute(
                f"""
                SELECT p.id,
                       COALESCE(p.name, p.code) AS name,
                       p.model,
                       p.max_flow_lpm
                FROM public.pumps p
                WHERE {_org_filter_sql()}
                ORDER BY name NULLS LAST, p.id
                """,
                {"org_id": org_id},
            )
        else:
            cur.execute("""
                SELECT p.id, COALESCE(p.name, p.code) AS name, p.model, p.max_flow_lpm
                FROM public.pumps p
                ORDER BY name NULLS LAST, p.id
            """)
        rows = cur.fetchall()

    return [{"id": r[0], "name": r[1], "model": r[2], "max_flow_lpm": r[3]} for r in rows]


def list_pumps_with_config(org_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Devuelve bombas + config + ubicación. Evita depender de la vista y del plural 'pump_configs'.
    """
    with get_conn() as conn, conn.cursor() as cur:
        if org_id:
            cur.execute(
                f"""
                SELECT
                    p.id                   AS pump_id,
                    COALESCE(p.name, p.code) AS pump_name,
                    p.model,
                    p.max_flow_lpm,

                    cfg.drive_type,
                    cfg.remote_enabled,
                    cfg.vfd_min_speed_pct,
                    cfg.vfd_max_speed_pct,
                    cfg.vfd_default_speed_pct,

                    l.id   AS location_id,
                    l.code AS location_code,
                    l.name AS location_name
                FROM public.pumps p
                LEFT JOIN public.pump_config cfg
                       ON cfg.pump_id = p.id
                LEFT JOIN public.asset_locations al
                       ON al.asset_type = 'pump'
                      AND al.asset_id   = p.id
                LEFT JOIN public.locations l
                       ON l.id = al.location_id
                WHERE {_org_filter_sql()}
                ORDER BY l.name NULLS FIRST, pump_name NULLS LAST, p.id
                """,
                {"org_id": org_id},
            )
        else:
            # Sin filtro (solo para uso interno/admin)
            cur.execute(
                """
                SELECT
                    p.id                   AS pump_id,
                    COALESCE(p.name, p.code) AS pump_name,
                    p.model,
                    p.max_flow_lpm,

                    cfg.drive_type,
                    cfg.remote_enabled,
                    cfg.vfd_min_speed_pct,
                    cfg.vfd_max_speed_pct,
                    cfg.vfd_default_speed_pct,

                    l.id   AS location_id,
                    l.code AS location_code,
                    l.name AS location_name
                FROM public.pumps p
                LEFT JOIN public.pump_config cfg
                       ON cfg.pump_id = p.id
                LEFT JOIN public.asset_locations al
                       ON al.asset_type = 'pump'
                      AND al.asset_id   = p.id
                LEFT JOIN public.locations l
                       ON l.id = al.location_id
                ORDER BY l.name NULLS FIRST, pump_name NULLS LAST, p.id
                """
            )

        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

    return [dict(zip(cols, r)) for r in rows]


def upsert_pump_config(pump_id: int, body, org_id: Optional[int] = None) -> dict:
    with get_conn() as conn, conn.cursor() as cur:
        # ¿Existe?
        cur.execute("SELECT 1 FROM public.pumps WHERE id=%s", (pump_id,))
        if cur.fetchone() is None:
            raise HTTPException(404, "Bomba no encontrada")

        # ¿Pertenece a la org?
        if org_id and not _pump_belongs_to_org(cur, pump_id, org_id):
            raise HTTPException(404, "Bomba no encontrada")

        # Validaciones VFD
        if body.vfd_min_speed_pct is not None and body.vfd_max_speed_pct is not None:
            if body.vfd_min_speed_pct > body.vfd_max_speed_pct:
                raise HTTPException(400, "vfd_min_speed_pct debe ser <= vfd_max_speed_pct")

        # Nota: mantenemos la tabla singular pump_config que ya usa tu código
        cur.execute("""
            INSERT INTO public.pump_config(
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
        row = cur.fetchone()
        conn.commit()

    cols = ["pump_id","drive_type","remote_enabled",
            "vfd_min_speed_pct","vfd_max_speed_pct","vfd_default_speed_pct","updated_at"]
    return dict(zip(cols, row))


def get_normalized_pump_config(pump_id: int):
    """
    Si querés seguir usando la vista v_pumps_with_config para un fetch puntual, mantené esta función.
    No depende de org_id; úsala solo cuando ya validaste acceso.
    """
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
