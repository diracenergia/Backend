# app/services/alarms_eval.py
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional, Tuple

# Repos (coinciden con tu backend)
from app.repos import tanks as tanks_repo
from app.repos import alarms as alarms_repo
from app.repos import audit as audit_repo  # si querés auditar, ya está importado

# Notificador (LISTEN/NOTIFY → listener → Telegram)
# Estas funciones ya las agregaste en app/services/alarm_events.py
from app.services.alarm_events import publish_raised, publish_cleared

# -----------------------------------------------------------------------------
# Config / Mapping
# -----------------------------------------------------------------------------

# Mapa de thresholds → (código_alarma, severidad_publicada)
# OJO: en DB tu CHECK de severity es en minúsculas: 'critical'/'warning'/'info'
#      Para DB guardamos lowercase; para Telegram/publicación usamos UPPER.
_THRESHOLD_MAP = {
    "low_low":   ("LOW_LOW",   "critical"),
    "low":       ("LOW",       "warning"),
    "high":      ("HIGH",      "warning"),
    "high_high": ("HIGH_HIGH", "critical"),
}

# Alias que usa el payload hacia el listener/Telegram
_THRESH_ALIAS = {
    "LOW_LOW": "very_low",
    "LOW":     "low",
    "HIGH":    "high",
    "HIGH_HIGH":"very_high",
}


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _utcnow():
    return datetime.now(timezone.utc)

def _decide_state(level_pct: float, cfg: dict) -> Optional[Tuple[str, str, str]]:
    """
    Devuelve (alarm_code_upper, severity_db_lower, threshold_alias)
    o None si está en rango normal.
    cfg espera: low_low_pct, low_pct, high_pct, high_high_pct
    """
    for k in ("low_low_pct", "low_pct", "high_pct", "high_high_pct"):
        if k not in cfg:
            raise ValueError(f"cfg incompleto: falta {k}")

    if level_pct <= cfg["low_low_pct"]:
        code, sev_db = _THRESHOLD_MAP["low_low"]
        return (code, sev_db, "low_low")
    if level_pct <= cfg["low_pct"]:
        code, sev_db = _THRESHOLD_MAP["low"]
        return (code, sev_db, "low")
    if level_pct >= cfg["high_high_pct"]:
        code, sev_db = _THRESHOLD_MAP["high_high"]
        return (code, sev_db, "high_high")
    if level_pct >= cfg["high_pct"]:
        code, sev_db = _THRESHOLD_MAP["high"]
        return (code, sev_db, "high")

    return None  # normal


def _clear_one(alarm_id: int, *, asset_type: str, asset_id: int, code: str,
               severity_db: str, message: str, value: float) -> None:
    """
    Limpia una alarma activa (si lo está) y publica CLEARED.
    """
    cleared = alarms_repo.clear(alarm_id, ts_cleared=_utcnow())
    if not cleared:
        return  # ya estaba limpia

    # Publicar CLEARED → listener → Telegram
    publish_cleared(
        alarm_id=alarm_id,
        asset_type=asset_type,
        asset_id=asset_id,
        code=code,                         # listener lo normaliza a UPPER
        severity=severity_db,              # listener lo normaliza a UPPER
        message=message or "",
        value=value,
        threshold=None,
    )


def _clear_all_for_tank(tank_id: int, *, value: float) -> None:
    """
    Limpia TODAS las alarmas activas del tanque (cualquier código).
    """
    # Traemos cualquier activa del tanque y vamos limpiando
    # Repos no tiene "list_active_by_tank", así que hacemos un fetch manual mínimo.
    # Si querés, podés extender repos/alarms con un list_active(asset_type, asset_id).
    from psycopg.rows import dict_row
    from app.core.db import get_conn

    sql = """
      SELECT id, code, severity, COALESCE(message,'') AS message
        FROM public.alarms
       WHERE asset_type='tank' AND asset_id=%s AND is_active=true
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (tank_id,))
        rows = cur.fetchall()

    for row in rows:
        _clear_one(
            alarm_id=row["id"],
            asset_type="tank",
            asset_id=tank_id,
            code=row["code"],
            severity_db=row["severity"],
            message=row["message"],
            value=value,
        )


# -----------------------------------------------------------------------------
# API pública que usa ingest.py
# -----------------------------------------------------------------------------

def eval_tank_alarm(tank_id: int, level_pct: Optional[float]) -> Optional[int]:
    """
    Evalúa una lectura de tanque contra thresholds y levanta/limpia alarmas.
    - tank_id: ID del tanque
    - level_pct: nivel (%). Si viene None, no hace nada.
    Retorna alarm_id si levantó una nueva alarma; None si no levantó o si limpió.
    """
    if level_pct is None:
        return None

    # 1) Obtener thresholds (con fallback a defaults si falta alguno)
    cfg = tanks_repo.get_config_by_id(tank_id)  # ya existe en tu repo
    # cfg: {low_low_pct, low_pct, high_pct, high_high_pct, ...}

    # 2) Determinar si está fuera de rango
    state = _decide_state(level_pct, cfg)
    if state is None:
        # Normal → limpiar activas (si las hay)
        _clear_all_for_tank(tank_id, value=level_pct)
        return None

    alarm_code_upper, severity_db_lower, threshold_key = state
    threshold_alias = _THRESH_ALIAS.get(alarm_code_upper, threshold_key)

    # 3) ¿Existe una ACTIVA del mismo código para este tanque?
    active = alarms_repo.get_active(asset_type="tank", asset_id=tank_id, code=alarm_code_upper)
    if active:
        # Ya hay una activa de ese tipo → no duplicamos
        return active.id

    # 4) Crear nueva alarma en DB (severity en lower por constraint)
    message = f"Tank {tank_id} {alarm_code_upper}"
    created = alarms_repo.create(
        asset_type="tank",
        asset_id=tank_id,
        code=alarm_code_upper,              # código almacenado tal cual (UPPER)
        severity=severity_db_lower,         # DB espera lower-case
        message=message,
        ts_raised=_utcnow(),
        is_active=True,
        extra={"value": level_pct, "threshold": threshold_alias},
    )
    alarm_id = created.id

    # 5) Publicar RAISED → listener → Telegram
    publish_raised(
        alarm_id=alarm_id,
        asset_type="tank",
        asset_id=tank_id,
        code=alarm_code_upper,              # listener lo pondrá en UPPER igual
        severity=severity_db_lower,         # listener lo pondrá en UPPER al enviar
        message=message,
        value=level_pct,
        threshold=threshold_alias,
    )

    # 6) (Opcional) si tenés columna tg_notified_at, la marcamos
    try:
        from app.core.db import get_conn
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("UPDATE public.alarms SET tg_notified_at = now() WHERE id=%s", (alarm_id,))
            conn.commit()
    except Exception:
        # Si la columna no existe o falla, no es crítico.
        pass

    return alarm_id
