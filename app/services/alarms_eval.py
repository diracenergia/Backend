# app/services/alarms_eval.py
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional

# 🔧 Ajustá estos imports a tus repos reales:
from app.repos import tanks as tanks_repo
from app.repos import alarms as alarms_repo       # ← asegurate de tenerlo
from app.repos import audit as audit_repo         # ← o el que uses para auditoría

def _now():
    return datetime.now(timezone.utc)

def _get_thresholds(tank_id: int):
    """
    Devuelve dict con low_low_pct, low_pct, high_pct, high_high_pct
    a partir de la config del tanque.
    """
    cfg = tanks_repo.get_config_by_id(tank_id)  # ← implementá en tu repo si no existe
    # Si ya tenés un método que devuelve exactamente este shape, usalo directo.
    return {
        "low_low_pct": float(cfg["low_low_pct"] if isinstance(cfg, dict) else cfg.low_low_pct),
        "low_pct":     float(cfg["low_pct"]     if isinstance(cfg, dict) else cfg.low_pct),
        "high_pct":    float(cfg["high_pct"]    if isinstance(cfg, dict) else cfg.high_pct),
        "high_high_pct": float(cfg["high_high_pct"] if isinstance(cfg, dict) else cfg.high_high_pct),
    }

def _get_active(asset_type: str, asset_id: int, code: str):
    return alarms_repo.get_active(asset_type=asset_type, asset_id=asset_id, code=code)

def _raise(asset_type: str, asset_id: int, code: str, severity: str, message: str):
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
    return a

def _clear(a):
    alarms_repo.clear(a.id, ts_cleared=_now())
    audit_repo.log(
        ts=_now(),
        asset_type=a.asset_type,
        asset_id=a.asset_id,
        code=a.code,
        severity=a.severity,
        state="CLEARED",
    )

def _escalate(old_alarm, new_code: str, new_severity: str, message: str):
    _clear(old_alarm)
    return _raise(old_alarm.asset_type, old_alarm.asset_id, new_code, new_severity, message)

def eval_tank_alarm(tank_id: int, level_pct: Optional[float]):
    """
    Evaluación de umbrales por lectura. Debe ser llamada en cada sample guardado.
    Reglas:
    - Regímenes mutuamente excluyentes (LOW/LOW_LOW vs HIGH/HIGH_HIGH).
    - Escalado LOW→LOW_LOW y HIGH→HIGH_HIGH.
    - Limpieza al volver a normal.
    """
    if level_pct is None:
        # Podrías disparar SENSOR_FAIL si lo tenés contemplado; aquí lo omitimos.
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
        if high: _clear(high)
        if highhigh: _clear(highhigh)
        if low: _escalate(low, "LOW_LOW", "critical", msg)
        elif not lowlow: _raise("tank", tank_id, "LOW_LOW", "critical", msg)
        return

    # --- LOW ---
    if level_pct <= l:
        msg = f"Nivel bajo ({level_pct:.1f}% <= {l:.2f}%)"
        if high: _clear(high)
        if highhigh: _clear(highhigh)
        if not low and not lowlow:  # no pises LOW_LOW
            _raise("tank", tank_id, "LOW", "warning", msg)
        return

    # --- HIGH_HIGH ---
    if level_pct >= hh:
        msg = f"Nivel muy alto ({level_pct:.1f}% >= {hh:.2f}%)"
        if low: _clear(low)
        if lowlow: _clear(lowlow)
        if high: _escalate(high, "HIGH_HIGH", "critical", msg)
        elif not highhigh: _raise("tank", tank_id, "HIGH_HIGH", "critical", msg)
        return

    # --- HIGH ---
    if level_pct >= h:
        msg = f"Nivel alto ({level_pct:.1f}% >= {h:.2f}%)"
        if low: _clear(low)
        if lowlow: _clear(lowlow)
        if not high and not highhigh:
            _raise("tank", tank_id, "HIGH", "warning", msg)
        return

    # --- NORMAL: limpiar todas si estaban activas ---
    for a in filter(None, [lowlow, low, high, highhigh]):
        _clear(a)
