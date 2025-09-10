# app/services/alarms_eval.py
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional, Tuple

# Repos (coinciden con tu backend)
from app.repos import tanks as tanks_repo
from app.repos import alarms as alarms_repo
from app.repos import audit as audit_repo  # si querés auditar

# Notificador (LISTEN/NOTIFY → listener → Telegram)
# Usamos la firma recomendada:
#   publish_raised(asset_type, asset_id, code, message, severity, value, threshold, ts_raised)
#   publish_cleared(asset_type, asset_id, code, message, severity, value, threshold, ts_cleared)
from app.services.alarm_events import publish_raised, publish_cleared

from psycopg.types.json import Json  # psycopg v3

# -----------------------------------------------------------------------------
# Config / Mapping
# -----------------------------------------------------------------------------

# Mapa de thresholds → (código_alarma, severidad_publicada_en_DB_lowercase)
_THRESHOLD_MAP = {
    "low_low":   ("LOW_LOW",   "critical"),
    "low":       ("LOW",       "warning"),
    "high":      ("HIGH",      "warning"),
    "high_high": ("HIGH_HIGH", "critical"),
}

# Alias que mandamos en el payload (útil para mostrar en Telegram)
_THRESH_ALIAS = {
    "LOW_LOW":  "very_low",
    "LOW":      "low",
    "HIGH":     "high",
    "HIGH_HIGH":"very_high",
}

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _utcnow():
    return datetime.now(timezone.utc)

def _iso(dt: datetime) -> str:
    # ISO8601 con Z
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _decide_state(level_pct: float, cfg: dict) -> Optional[Tuple[str, str, str]]:
    """
    Devuelve (alarm_code_upper, severity_db_lower, threshold_key) o None si normal.
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

    # Publicar CLEARED → listener → Telegram (incluye timestamp)
    publish_cleared(
        asset_type=asset_type,
        asset_id=asset_id,
        code=code,                         # UPPER
        message=message or "",
        severity=severity_db,              # DB en lower; el notifier lo mostrará en UPPER si querés
        value=value,
        threshold=None,                    # opcional al limpiar
        ts_cleared=_iso(_utcnow()),
    )

def _clear_all_for_tank(tank_id: int, *, value: float) -> None:
    """
    Limpia TODAS las alarmas activas del tanque (cualquier código).
    """
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
    Retorna alarm_id si levantó una nueva alarma; None si no levantó o si limpió.
    """
    if level_pct is None:
        return None

    # 1) Obtener thresholds
    cfg = tanks_repo.get_config_by_id(tank_id)  # {low_low_pct, low_pct, high_pct, high_high_pct, ...}

    # 2) Determinar estado
    state = _decide_state(level_pct, cfg)
    if state is None:
        # Normal → limpiar activas (si las hay)
        _clear_all_for_tank(tank_id, value=level_pct)
        return None

    alarm_code_upper, severity_db_lower, threshold_key = state
    threshold_alias = _THRESH_ALIAS.get(alarm_code_upper, threshold_key)

    # 3) ¿Existe una ACTIVA del mismo código?
    active = alarms_repo.get_active(asset_type="tank", asset_id=tank_id, code=alarm_code_upper)
    if active:
        # Ya está levantada → no duplicamos
        return active.id

    # 4) Crear nueva alarma en DB (severity en lower por constraint)
    message = f"Tank {tank_id} {alarm_code_upper}"
    created = alarms_repo.create(
        asset_type="tank",
        asset_id=tank_id,
        code=alarm_code_upper,              # UPPER
        severity=severity_db_lower,         # DB espera lower-case
        message=message,
        ts_raised=_utcnow(),
        is_active=True,
        # ⚠️ Json(...) para evitar “cannot adapt type 'dict'”
        extra=Json({"value": level_pct, "threshold": threshold_alias}),
    )
    alarm_id = created.id

    # 5) Publicar RAISED → listener → Telegram (incluye timestamp)
    publish_raised(
        asset_type="tank",
        asset_id=tank_id,
        code=alarm_code_upper,
        message=message,
        severity=severity_db_lower,
        value=level_pct,
        threshold=threshold_alias,
        ts_raised=_iso(_utcnow()),
    )

    # 6) (Opcional) marcar tg_notified_at
    try:
        from app.core.db import get_conn
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("UPDATE public.alarms SET tg_notified_at = now() WHERE id=%s", (alarm_id,))
            conn.commit()
    except Exception:
        pass

    return alarm_id
