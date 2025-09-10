# app/services/alarm_events.py
from __future__ import annotations

import os
import json
from typing import Any, Mapping, Optional

import psycopg  # psycopg v3


# Canal y DSN
_CHANNEL = os.getenv("ALARM_NOTIFY_CHANNEL", "alarm_events")
_DSN = os.getenv("DATABASE_URL") or os.getenv("DB_URL")


def _norm_upper(s: Optional[str]) -> str:
    return (s or "").upper()


def _debug(msg: str) -> None:
    print(f"[alarm-publish] {msg}")


def publish_alarm_event(payload: Mapping[str, Any]) -> None:
    """
    Publica un evento JSON en el canal de Postgres para que lo escuche el alarm_listener.
    Requiere que DATABASE_URL/DB_URL apunte a la MISMA DB donde corre el listener.

    payload esperado (keys principales):
      - op: 'RAISED' | 'UPDATED' | 'CLEARED'
      - asset_type: 'tank' | 'pump' | 'valve' | ...
      - asset_id: int
      - code: p.ej. 'HIGH_HIGH', 'LEVEL' (se normaliza a MAYÚSCULAS)
      - severity: 'CRITICAL' | 'WARNING' | 'INFO' (se normaliza a MAYÚSCULAS)
      - message: str (opcional)
      - threshold: str (opcional)
      - value: num (opcional)
      - alarm_id: int (opcional pero MUY recomendado para dedupe en listener)
    """
    if not _DSN:
        _debug("❌ falta DATABASE_URL/DB_URL; NO se publica evento")
        return

    # Normalizamos los campos clave sin mutar el input original
    norm_payload = dict(payload)
    if "op" in norm_payload:
        norm_payload["op"] = _norm_upper(norm_payload.get("op"))
    if "code" in norm_payload:
        norm_payload["code"] = _norm_upper(norm_payload.get("code"))
    if "severity" in norm_payload:
        norm_payload["severity"] = _norm_upper(norm_payload.get("severity"))

    try:
        msg = json.dumps(norm_payload, ensure_ascii=False)
    except Exception as e:
        _debug(f"❌ error serializando JSON: {e}")
        return

    try:
        # Usamos autocommit para que pg_notify se emita inmediatamente
        with psycopg.connect(_DSN, autocommit=True, application_name="alarm-publisher") as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_notify(%s, %s);", (_CHANNEL, msg))
        _debug(f"canal={_CHANNEL} payload={msg}")
    except Exception as e:
        _debug(f"❌ error publicando en {_CHANNEL}: {e}")


def publish_raised(
    *,
    alarm_id: int,
    asset_type: str,
    asset_id: int,
    code: str,
    severity: str,
    message: str = "",
    value: Optional[float] = None,
    threshold: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> None:
    """
    Publica un evento RAISED (alarma levantada).
    """
    payload = {
        "op": "RAISED",
        "asset_type": asset_type,
        "asset_id": asset_id,
        "code": code,
        "severity": severity,
        "message": message or "",
        "threshold": threshold,
        "value": value,
        "alarm_id": alarm_id,
    }
    if extra:
        payload.update(extra)
    publish_alarm_event(payload)


def publish_cleared(
    *,
    alarm_id: int,
    asset_type: str,
    asset_id: int,
    code: str,
    severity: str,
    message: str = "",
    value: Optional[float] = None,
    threshold: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> None:
    """
    Publica un evento CLEARED (alarma limpiada).
    """
    payload = {
        "op": "CLEARED",
        "asset_type": asset_type,
        "asset_id": asset_id,
        "code": code,
        "severity": severity,
        "message": message or "",
        "threshold": threshold,
        "value": value,
        "alarm_id": alarm_id,
    }
    if extra:
        payload.update(extra)
    publish_alarm_event(payload)


# (Opcional) por si querés avisar cambios de estado sin clear/raise nuevos:
def publish_updated(
    *,
    alarm_id: int,
    asset_type: str,
    asset_id: int,
    code: str,
    severity: str,
    message: str = "",
    value: Optional[float] = None,
    threshold: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> None:
    """
    Publica un evento UPDATED (p.ej. escaló la severidad o cambió el umbral).
    El listener por defecto suele filtrar UPDATED salvo que haya cambios relevantes.
    """
    payload = {
        "op": "UPDATED",
        "asset_type": asset_type,
        "asset_id": asset_id,
        "code": code,
        "severity": severity,
        "message": message or "",
        "threshold": threshold,
        "value": value,
        "alarm_id": alarm_id,
    }
    if extra:
        payload.update(extra)
    publish_alarm_event(payload)
