# app/services/notify_alarm.py
from ..core.telegram import send_telegram  # usa el sender asíncrono que ya tenés en core

_last: dict[str, float] = {}

def _once_every(key: str, seconds: int) -> bool:
    import time
    now = time.time()
    ok = key not in _last or (now - _last[key]) > seconds
    if ok:
        _last[key] = now
    return ok

def _esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def _equip_label(a: dict) -> str:
    if a.get("asset_type") == "tank":
        return "TK-" + str(a["asset_id"])
    if a.get("asset_type") == "pump":
        return "PU-" + str(a["asset_id"])
    return f'{a.get("asset_type")}-{a.get("asset_id")}'

async def notify_alarm(a: dict):
    """
    Envía alerta a Telegram para eventos de umbral.
    - Filtra por severidad (critical/warning), aceptando cualquier casing.
    - Anti-flood 5 min por equipo+severidad+operación (RAISED/CLEARED).
    """
    sev = str(a.get("severity") or "").lower()          # ← normaliza
    if sev not in ("critical", "warning"):
        return

    op = (a.get("op") or "").upper()                   # "RAISED" | "CLEARED" | ...
    equipo = _equip_label(a)
    code = (a.get("code") or "").upper()

    # Elegí un timestamp razonable según la operación
    ts = a.get("ts_raised") if op == "RAISED" else a.get("ts_cleared")
    ts = ts or a.get("ts") or "—"

    # Anti-flood por equipo+severidad+op (evita que RAISED bloquee al CLEARED)
    key = f'{a.get("asset_type")}-{a.get("asset_id")}-{sev}-{op}'
    if not _once_every(key, 300):  # 5 min
        return

    header = "ALERTA" if op == "RAISED" else ("NORMALIZADA" if op == "CLEARED" else "EVENTO")
    msg_text = a.get("message") or ("Alarma" if op == "RAISED" else "Alarma normalizada" if op == "CLEARED" else "Alarma")

    text = (
        f'<b>{header} {sev.upper()}</b>\n'
        f'<b>Equipo:</b> {_esc(equipo)}\n'
        + (f'<b>Código:</b> {_esc(code)}\n' if code else "")
        + f'<b>Mensaje:</b> {_esc(msg_text)}\n'
        + f'<b>Hora:</b> {_esc(ts)}'
    )
    await send_telegram(text)

async def notify_ack(a: dict, user: str):
    """
    Notifica ACK (confirmación) de una alarma.
    """
    equipo = _equip_label(a)
    code = (a.get("code") or "").upper()
    text = (
        f'🆗 <b>ACK</b> por <b>{_esc(user)}</b>\n'
        f'<b>Equipo:</b> {_esc(equipo)}\n'
        + (f'<b>Código:</b> {_esc(code)}\n' if code else "")
    )
    await send_telegram(text)
