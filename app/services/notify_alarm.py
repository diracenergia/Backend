# app/services/notify_alarm.py
from __future__ import annotations

import os
import logging

from ..core.telegram import send_telegram  # sender asíncrono existente

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="ts=%(asctime)s level=%(levelname)s module=%(name)s msg=%(message)s",
)
log = logging.getLogger("notify-alarm")

__all__ = ["notify_alarm", "notify_ack"]  # para que quede claro qué exportamos


def _esc(s: str | None) -> str:
    if s is None:
        return ""
    # Telegram HTML
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _equip_label(a: dict) -> str:
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
    if op == "RAISE":
        return "RAISED"
    if op == "CLEAR":
        return "CLEARED"
    return op


async def notify_alarm(a: dict) -> None:
    """
    Enviar SIEMPRE que venga un evento de cruce de umbral (RAISED/CLEARED).
    Sin anti-flood, sin filtro de severidad. Con logs detallados.
    """
    op = _norm_op(a)
    if op not in {"RAISED", "CLEARED"}:
        log.info("skip_send reason=op_not_threshold_cross op=%s", op)
        return

    equipo = _equip_label(a)
    code = (a.get("code") or "").upper()
    msg_text = a.get("message") or ("Alarma" if op == "RAISED" else "Alarma normalizada")

    # Timestamps “mejor esfuerzo”
    ts = a.get("ts_raised") if op == "RAISED" else (a.get("ts_cleared") or a.get("ts") or "")

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

    # Logs antes del envío
    log.info(
        "send_attempt op=%s equipo=%s code=%s severity=%s value=%s threshold=%s",
        op, equipo, code or "-", sev or "-", value, threshold
    )

    try:
        result = await send_telegram(text)
        log.info("send_done status=ok result=%s", result if result is not None else "none")
    except Exception as e:
        log.exception("send_error err=%s op=%s equipo=%s code=%s", e, op, equipo, code)
        # No relanzamos; dejamos que el flujo siga.


async def notify_ack(a: dict, user: str) -> None:
    """
    Notificación de ACK (confirmación) simple.
    La mantenemos para compatibilidad con módulos que la importan.
    """
    try:
        equipo = _equip_label(a)
        code = (a.get("code") or "").upper()
        text = (
            f'🆗 <b>ACK</b> por <b>{_esc(user)}</b>\n'
            f'<b>Equipo:</b> {_esc(equipo)}\n'
            + (f'<b>Código:</b> {_esc(code)}\n' if code else "")
        )

        log.info("ack_attempt user=%s equipo=%s code=%s", user, equipo, code or "-")
        result = await send_telegram(text)
        log.info("ack_done status=ok result=%s", result if result is not None else "none")
    except Exception as e:
        log.exception("ack_error err=%s user=%s", e, user)
