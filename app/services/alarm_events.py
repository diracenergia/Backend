# app/services/alarm_events.py
from __future__ import annotations

import os
import json
from typing import Any, Mapping, Optional

import psycopg  # psycopg v3

# Usamos el DSN especial para eventos (session pooler o directo)
from app.core.db import EVENTS_DSN


def _clean(v: Optional[str]) -> str:
    return (v or "").strip()


# Canal y DSN
_CHANNEL = _clean(os.getenv("ALARM_NOTIFY_CHANNEL") or "alarm_events")
_DSN = _clean(EVENTS_DSN)  # viene ya limpio desde core/db.py


def _norm_upper(s: Optional[str]) -> str:
    return (s or "").upper()


def _norm_op(op: Optional[str]) -> str:
    """
    Acepta alias y normaliza al contrato del listener:
    'RAISE' | 'CLEAR' | 'ACK' | 'UPDATE'
    """
    o = _norm_upper(op)
    if o in {"RAISE", "RAISED"}:
        return "RAISE"
    if o in {"CLEAR", "CLEARED"}:
        return "CLEAR"
    if o in {"ACK", "ACKED"}:
        return "ACK"
    if o in {"UPDATE", "UPDATED"}:
        return "UPDATE"
    return o or "UPDATE"


def _debug(msg: str) -> None:
    print(f"[alarm-publish] {msg}")


def publish_alarm_event(payload: Mapping[str, Any]) -> None:
    """
    Publica un evento JSON en el canal de Postgres para que lo escuche el alarm_listener.
    Requiere que EVENTS_DSN apunte a una conexión que soporte LISTEN/NOTIFY.
    """
    if not _DSN:
        _debug("❌ falta EVENTS_DSN/DATABASE_URL; NO se publica evento")
        return

    # Copiamos y normalizamos sin mutar el input
    norm_payload: dict[str, Any] = dict(payload)
    norm_payload["op"] = _norm_op(norm_payload.get("op"))
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
        # autocommit=True para que pg_notify salga inmediatamente
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
    """Evento RAISE (alarma levantada)."""
    payload: dict[str, Any] = {
        "op": "RAISE",
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
    """Evento CLEAR (alarma limpia)."""
    payload: dict[str, Any] = {
        "op": "CLEAR",
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


def publish_ack(
    *,
    alarm_id: int,
    asset_type: str,
    asset_id: int,
    code: str,
    severity: str,
    message: str = "",
    extra: Optional[Mapping[str, Any]] = None,
) -> None:
    """Evento ACK (confirmación)."""
    payload: dict[str, Any] = {
        "op": "ACK",
        "asset_type": asset_type,
        "asset_id": asset_id,
        "code": code,
        "severity": severity,
        "message": message or "",
        "alarm_id": alarm_id,
    }
    if extra:
        payload.update(extra)
    publish_alarm_event(payload)


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
    """Evento UPDATE (p.ej. cambio de severidad/umbral)."""
    payload: dict[str, Any] = {
        "op": "UPDATE",
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
