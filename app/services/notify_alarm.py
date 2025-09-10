# app/services/notify_alarm.py
from __future__ import annotations

from typing import Optional

from ..core.telegram import send_telegram  # sender asíncrono existente


def _esc(s: str | None) -> str:
    if s is None:
        return ""
    # Telegram en HTML
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _equip_label(a: dict) -> str:
    """
    Etiqueta corta para el equipo (podés ajustar a tu modelo real).
    """
    t = (a.get("asset_type") or "").lower()
    i = a.get("asset_id")
    if t == "tank":
        return f"TK-{i}"
    if t == "pump":
        return f"PU-{i}"
    if t:
        return f"{t.upper()}-{i}"
    return f"{i or '-'}"


def _norm_op(a: dict) -> str:
    op = (a.get("op") or a.get("operation") or "").upper()
    if op in {"RAISE"}:
        return "RAISED"
    if op in {"CLEAR"}:
        return "CLEARED"
    return op


async def notify_alarm(a: dict) -> None:
    """
    Enviar SIEMPRE que venga un evento de cruce de umbral (RAISED/CLEARED).
    Sin anti-flood, sin filtro de severidad.
    """
    op = _norm_op(a)
    if op not in {"RAISED", "CLEARED"}:
        return

    equipo = _equip_label(a)
    code = (a.get("code") or "").upper()
    msg_text = a.get("message") or ("Alarma" if op == "RAISED" else "Alarma normalizada")

    # Timestamps “mejor esfuerzo”
    ts = (
        a.get("ts_raised") if op == "RAISED"
        else a.get("ts_cleared") or a.get("ts") or ""
    )

    header = "ALERTA" if op == "RAISED" else "NORMALIZADA"

    # Opcionales
    sev = (a.get("severity") or "").upper()
    value = a.get("value")
    threshold = a.get("threshold")

    parts = [
        f'🚨 <b>{header}</b>',
        f'<b>Equipo:</b> {_esc(equipo)}',
    ]
    if code:
        parts.append(f'<b>Código:</b> {_esc(code)}')
    if sev:
        parts.append(f'<b>Severidad:</b> {_esc(sev)}')
    parts.append(f'<b>Mensaje:</b> {_esc(msg_text)}')
    if value is not None:
        parts.append(f'<b>Valor:</b> {_esc(str(value))}')
    if threshold is not None:
        parts.append(f'<b>Umbral:</b> {_esc(str(threshold))}')
    if ts:
        parts.append(f'<b>Hora:</b> {_esc(ts)}')

    text = "\n".join(parts)
    await send_telegram(text)


async def notify_ack(a: dict, user: str) -> None:
    """
    (Opcional) Notificación de ACK si más adelante la querés usar.
    """
    equipo = _equip_label(a)
    code = (a.get("code") or "").upper()
    text = (
        f'🆗 <b>ACK</b> por <b>{_esc(user)}</b>\n'
        f'<b>Equipo:</b> {_esc(equipo)}\n'
        + (f'<b>Código:</b> {_esc(code)}\n' if code else "")
    )
    await send_telegram(text)
