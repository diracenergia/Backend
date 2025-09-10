# app/services/alarm.py
from __future__ import annotations
from typing import Any, Dict, Optional, Iterable

from app.repos import tanks as repo
from app.services.alarm_events import publish_raised, publish_cleared

# Mapping de código → etiqueta de umbral para el payload
_THRESH_ALIAS = {
    "LOW_LOW":   "very_low",
    "LOW":       "low",
    "HIGH":      "high",
    "HIGH_HIGH": "very_high",
}

def _as_list(x) -> Iterable[Dict[str, Any]]:
    """Normaliza la salida de clear/create del repo a iterable de dicts."""
    if x is None:
        return []
    if isinstance(x, dict):
        return [x]
    if isinstance(x, (list, tuple)):
        return x
    return []

def process_tank_thresholds(insert_result: dict) -> dict | None:
    """
    Recibe el resultado de la inserción de lectura y decide si hay alarma por umbral.
    Publica a 'alarm_events' cuando hay RAISED o CLEARED.
    """
    rlevel = insert_result.get("level_percent")
    if rlevel is None:
        return None

    tank_id = insert_result["tank_id"]
    cfg = repo.get_tank_thresholds(tank_id)  # ← tu repo actual
    if not cfg:
        print(f"[alarm] no thresholds for tank={tank_id}")
        return None

    # Tu repo devuelve: low, low_low, high, high_high
    low, low_low, high, high_high = cfg
    lvl = float(rlevel)

    code = severity = msg = None
    if lvl <= low_low:
        code, severity, msg = "LOW_LOW", "critical", f"Nivel muy bajo ({lvl:.1f}% <= {low_low:.2f}%)"
    elif lvl <= low:
        code, severity, msg = "LOW", "warning", f"Nivel bajo ({lvl:.1f}% <= {low:.2f}%)"
    elif lvl >= high_high:
        code, severity, msg = "HIGH_HIGH", "critical", f"Nivel muy alto ({lvl:.1f}% >= {high_high:.2f}%)"
    elif lvl >= high:
        code, severity, msg = "HIGH", "warning", f"Nivel alto ({lvl:.1f}% >= {high:.2f}%)"

    latest = {
        "id": insert_result["id"],
        "ts": insert_result["ts"].isoformat() if insert_result.get("ts") is not None else None,
        "tank_id": tank_id,
        "raw_json": insert_result.get("raw_json"),
        "volume_l": float(insert_result["volume_l"]) if insert_result.get("volume_l") is not None else None,
        "device_id": insert_result.get("device_id"),
        "level_percent": lvl,
        "temperature_c": float(insert_result["temperature_c"]) if insert_result.get("temperature_c") is not None else None,
    }

    if code is None:
        # No corresponde alarma en este nivel → limpiar las activas y notificar CLEARED
        cleared = repo.clear_tank_alarms(tank_id, latest)
        sent = 0
        try:
            items = _as_list(cleared)
            if items:
                for a in items:
                    aid = int(a.get("id", 0))
                    sev = str(a.get("severity") or "warning")
                    th  = a.get("threshold") or _THRESH_ALIAS.get(str(a.get("code") or "").upper(), "")
                    publish_cleared(
                        asset_type="tank",
                        asset_id=tank_id,
                        code=str(a.get("code") or "LEVEL"),
                        alarm_id=aid,
                        severity=sev,
                        threshold=th,
                        value=lvl,
                        message="Alarma normalizada",
                    )
                    sent += 1
            else:
                # Fallback: no sabemos qué id se limpió → mensaje genérico
                publish_cleared(
                    asset_type="tank",
                    asset_id=tank_id,
                    code="LEVEL",
                    alarm_id=0,  # genérico (listener lo permite)
                    severity="warning",
                    threshold="",
                    value=lvl,
                    message="Alarma normalizada",
                )
                sent += 1
            print(f"[alarm] CLEARED notify sent x{sent} tank={tank_id} lvl={lvl}")
        except Exception as e:
            print(f"[alarm] WARN publish CLEARED failed: {e} tank={tank_id} lvl={lvl}")
        return None

    # Hay alarma → crear/actualizar en repo y notificar RAISED
    a = repo.create_tank_alarm(tank_id, code, severity, msg, latest)
    try:
        aid = int(a.get("id", 0)) if isinstance(a, dict) else 0
        publish_raised(
            asset_type="tank",
            asset_id=tank_id,
            code=code,
            alarm_id=aid,
            severity=severity,
            threshold=_THRESH_ALIAS.get(code, code.lower()),
            value=lvl,
            message=msg,
        )
        print(f"[alarm] RAISED notify sent tank={tank_id} id={aid} code={code} sev={severity} lvl={lvl}")
    except Exception as e:
        print(f"[alarm] WARN publish RAISED failed: {e} tank={tank_id} code={code} lvl={lvl}")

    return a
