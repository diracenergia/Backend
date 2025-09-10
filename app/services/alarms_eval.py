import datetime
from app.services.alarm_events import publish_raised, publish_cleared

# Severidad por tipo de umbral
_THRESHOLD_MAP = {
    "low_low":  ("LOW_LOW",  "CRITICAL"),
    "low":      ("LOW",      "WARNING"),
    "high":     ("HIGH",     "WARNING"),
    "high_high":("HIGH_HIGH","CRITICAL"),
}

def eval_tank_reading(conn, tank_id: int, value: float, cfg: dict):
    """
    Evalúa una lectura de tanque contra su configuración de thresholds.
    - conn: conexión psycopg/SQLAlchemy
    - tank_id: ID del tanque
    - value: nivel (%)
    - cfg: dict con claves low_low_pct, low_pct, high_pct, high_high_pct
    """
    now = datetime.datetime.utcnow()

    # Determinar estado según thresholds
    alarm_code, severity, threshold = None, None, None
    if value <= cfg["low_low_pct"]:
        alarm_code, severity, threshold = _THRESHOLD_MAP["low_low"]
    elif value <= cfg["low_pct"]:
        alarm_code, severity, threshold = _THRESHOLD_MAP["low"]
    elif value >= cfg["high_high_pct"]:
        alarm_code, severity, threshold = _THRESHOLD_MAP["high_high"]
    elif value >= cfg["high_pct"]:
        alarm_code, severity, threshold = _THRESHOLD_MAP["high"]
    else:
        # valor normal → limpiar alarmas activas de este tanque
        _clear_active_alarms(conn, "tank", tank_id, value)
        return

    # Buscar alarma activa para este tanque/código
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, is_active FROM public.alarms
             WHERE asset_type='tank'
               AND asset_id=%s
               AND code=%s
               AND is_active=true
             ORDER BY ts_raised DESC
             LIMIT 1;
        """, (tank_id, alarm_code))
        row = cur.fetchone()

    if row:
        # Ya hay alarma activa, no duplicamos
        return

    # Crear nueva alarma
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO public.alarms (asset_type, asset_id, code, severity, message, extra)
            VALUES ('tank', %s, %s, %s, %s,
                    jsonb_build_object('value', %s, 'threshold', %s))
            RETURNING id;
        """, (tank_id, alarm_code, severity,
              f"Tank {tank_id} {alarm_code}", value, threshold))
        alarm_id = cur.fetchone()[0]
    conn.commit()

    # Publicar evento RAISED → listener → Telegram
    publish_raised(
        alarm_id=alarm_id,
        asset_type="tank",
        asset_id=tank_id,
        code=alarm_code,
        severity=severity,
        message=f"Tank {tank_id} {alarm_code}",
        value=value,
        threshold=threshold,
    )


def _clear_active_alarms(conn, asset_type: str, asset_id: int, value: float):
    """Limpia alarmas activas de un activo cuando vuelve a rango normal."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, code, severity, message
              FROM public.alarms
             WHERE asset_type=%s AND asset_id=%s
               AND is_active=true
        """, (asset_type, asset_id))
        rows = cur.fetchall()

    for alarm_id, code, severity, message in rows:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE public.alarms
                   SET ts_cleared=now(),
                       is_active=false
                 WHERE id=%s
            """, (alarm_id,))
        conn.commit()

        publish_cleared(
            alarm_id=alarm_id,
            asset_type=asset_type,
            asset_id=asset_id,
            code=code,
            severity=severity,
            message=message,
            value=value,
            threshold=None,
        )
