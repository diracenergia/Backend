# app/services/alarms_eval.py
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional

# Repos (ajustá si tus módulos reales se llaman distinto)
from app.repos import tanks as tanks_repo
from app.repos import alarms as alarms_repo
from app.repos import audit as audit_repo

# Notificador (LISTEN/NOTIFY → listener → Telegram)
from app.services.alarm_events import publish_raised, publish_cleared

# Mapeo de códigos → etiquetas de umbral para el payload
_THRESH_ALIAS = {
    "LOW_LOW": "very_low",
    "LOW": "low",
    "HIGH": "high",
    "HIGH_HIGH": "very_high",
}

def _now():
    return datetime.now(timezone.utc)

# -------- Umbrales con fallback (evita el crash por get_config_by_id) --------
def _get_thresholds(tank_id: int):
    """
    Devuelve dict con low_low_pct, low_pct, high_pct, high_high_pct.
    Intenta get_config_by_id; si no existe, usa get_tank_config; si falta algo, defaults.
    """
    defaults = {"low_low_pct": 10.0, "low_pct": 20.0, "high_pct": 80.0, "high_high_pct": 90.0}

    cfg = None
    if hasattr(tanks_repo, "get_config_by_id"):
        try:
            cfg = tanks_repo.get_config_by_id(tank_id)
            print(f"[eval] thresholds via get_config_by_id tank={tank_id}: {cfg}")
        except Exception as e:
            print(f"[eval] get_config_by_id failed: {e}")

    if cfg is None and hasattr(tanks_repo, "get_tank_config"):
        try:
            cfg = tanks_repo.get_tank_config(tank_id)
            print(f"[eval] thresholds via get_tank_config tank={tank_id}: {cfg}")
        except Exception as e:
            print(f"[eval] get_tank_config failed: {e}")

    def _get(obj, key):
        if obj is None:
            return defaults[key]
        if isinstance(obj, dict):
            return float(obj.get(key)) if obj.get(key) is not None else defaults[key]
        return float(getattr(obj, key, defaults[key]))

    th = {
        "low_low_pct":  _get(cfg, "low_low_pct"),
        "low_pct":      _get(cfg, "low_pct"),
        "high_pct":     _get(cfg, "high_pct"),
        "high_high_pct":_get(cfg, "high_high_pct"),
    }
    print(f"[eval] thresholds resolved tank={tank_id}: {th}")
    return th

# -------- Accesos a estado de alarmas --------
def _get_active(asset_type: str, asset_id: int, code: str):
    """
    Devuelve la alarma activa (si existe) para ese asset+code o None.
    Debe existir alarms_repo.get_active(...). Si no existe, implementalo allí.
    """
    return alarms_repo.get_active(asset_type=asset_type, asset_id=asset_id, code=code)

# -------- Helpers de transición con auditoría + NOTIFY --------
def _raise(asset_type: str, asset_id: int, code: str, severity: str, message: str, value: Optional[float] = None):
    a = alarms_repo.create(
        asset_type=asset_type,
        asset_id=asset_id,
        code=code,
        severity=severity,
        message=message,
        ts_raised=_now(),
        is_active=True,
    )
    audit_repo.log(
        ts=_now(),
        asset_type=asset_type,
        asset_id=asset_id,
        code=code,
        severity=severity,
        state="RAISED",
        details={"message": message},
    )
    # NOTIFY → listener → Telegram
    try:
        publish_raised(
            asset_type=asset_type,
            asset_id=asset_id,
            code=code,
            alarm_id=a.id,
            severity=severity,
            threshold=_THRESH_ALIAS.get(code, code.lower()),
            value=value,
        )
    except Exception as e:
        print("[WARN] notify RAISED failed:", e)
    return a

def _clear(a, value: Optional[float] = None):
    alarms_repo.clear(a.id, ts_cleared=_now())
    audit_repo.log(
        ts=_now(),
        asset_type=a.asset_type,
        asset_id=a.asset_id,
        code=a.code,
        severity=a.severity,
        state="CLEARED",
    )
    # NOTIFY → listener → Telegram
    try:
        publish_cleared(
            asset_type=a.asset_type,
            asset_id=a.asset_id,
            code=a.code,
            alarm_id=a.id,
            severity=a.severity,
            threshold=_THRESH_ALIAS.get(a.code, str(a.code).lower()),
            value=value,
        )
    except Exception as e:
        print("[WARN] notify CLEARED failed:", e)

def _escalate(old_alarm, new_code: str, new_severity: str, message: str, value: Optional[float] = None):
    _clear(old_alarm, value=value)
    return _raise(old_alarm.asset_type, old_alarm.asset_id, new_code, new_severity, message, value=value)

# -------- FUNCIÓN PÚBLICA: ¡esta es la que se importa! --------
def eval_tank_alarm(tank_id: int, level_pct: Optional[float]):
    """
    Evaluación de umbrales por lectura. Debe llamarse en cada sample guardado.
    Reglas:
      - LOW/LOW_LOW vs HIGH/HIGH_HIGH (mutuamente excluyentes).
      - Escalado LOW→LOW_LOW y HIGH→HIGH_HIGH.
      - Limpieza al volver a normal.
    """
    print(f"[ingest] eval_tank_alarm tank={tank_id} lvl={level_pct}")
    if level_pct is None:
        # Podrías disparar SENSOR_FAIL si querés contemplarlo
        return

    th = _get_thresholds(tank_id)
    ll = th["low_low_pct"]
    l  = th["low_pct"]
    h  = th["high_pct"]
    hh = th["high_high_pct"]

    lowlow = _get_active("tank", tank_id, "LOW_LOW")
    low    = _get_active("tank", tank_id, "LOW")
    high   = _get_active("tank", tank_id, "HIGH")
    highhigh = _get_active("tank", tank_id, "HIGH_HIGH")

    # --- LOW_LOW ---
    if level_pct <= ll:
        msg = f"Nivel muy bajo ({level_pct:.1f}% <= {ll:.2f}%)"
        if high: _clear(high, value=level_pct)
        if highhigh: _clear(highhigh, value=level_pct)
        if low: _escalate(low, "LOW_LOW", "critical", msg, value=level_pct)
        elif not lowlow: _raise("tank", tank_id, "LOW_LOW", "critical", msg, value=level_pct)
        return

    # --- LOW ---
    if level_pct <= l:
        msg = f"Nivel bajo ({level_pct:.1f}% <= {l:.2f}%)"
        if high: _clear(high, value=level_pct)
        if highhigh: _clear(highhigh, value=level_pct)
        if not low and not lowlow:  # no pises LOW_LOW
            _raise("tank", tank_id, "LOW", "warning", msg, value=level_pct)
        return

    # --- HIGH_HIGH ---
    if level_pct >= hh:
        msg = f"Nivel muy alto ({level_pct:.1f}% >= {hh:.2f}%)"
        if low: _clear(low, value=level_pct)
        if lowlow: _clear(lowlow, value=level_pct)
        if high: _escalate(high, "HIGH_HIGH", "critical", msg, value=level_pct)
        elif not highhigh: _raise("tank", tank_id, "HIGH_HIGH", "critical", msg, value=level_pct)
        return

    # --- HIGH ---
    if level_pct >= h:
        msg = f"Nivel alto ({level_pct:.1f}% >= {h:.2f}%)"
        if low: _clear(low, value=level_pct)
        if lowlow: _clear(lowlow, value=level_pct)
        if not high and not highhigh:
            _raise("tank", tank_id, "HIGH", "warning", msg, value=level_pct)
        return

    # --- NORMAL: limpiar todas si estaban activas ---
    for a in filter(None, [lowlow, low, high, highhigh]):
        _clear(a, value=level_pct)
