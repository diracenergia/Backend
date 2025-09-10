# app/services/notify_alarm.py
from ..core.telegram import send_telegram  # si tu proyecto usa esta; si no, ajusta al módulo real

_last: dict[str, float] = {}

def _once_every(key: str, seconds: int) -> bool:
  import time
  now = time.time()
  ok = key not in _last or (now - _last[key]) > seconds
  if ok: _last[key] = now
  return ok

def _esc(s: str) -> str:
  return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

async def notify_alarm(a: dict):
  """Envía alerta a Telegram (solo critical/warning), con antiflood 5 min x equipo+severidad."""
  sev = str(a.get("severity") or "").lower()   # 👈 normaliza
  if sev not in ("critical", "warning"):
    return

  equipo = "TK-" + str(a["asset_id"]) if a["asset_type"]=="tank" \
        else "PU-" + str(a["asset_id"]) if a["asset_type"]=="pump" \
        else f'{a["asset_type"]}-{a["asset_id"]}'
  code = (a.get("code") or "").upper()
  ts = a.get("ts_raised") or "—"

  key = f'{a["asset_type"]}-{a["asset_id"]}-{sev}'  # 👈 usa la normalizada
  if not _once_every(key, 300):  # 5 min
    return

  text = (
    f'<b>ALERTA {sev.upper()}</b>\n'
    f'<b>Equipo:</b> {_esc(equipo)}\n'
    + (f'<b>Código:</b> {_esc(code)}\n' if code else "")
    + f'<b>Mensaje:</b> {_esc(a.get("message") or "Alarma")}\n'
    + f'<b>Hora:</b> {_esc(ts)}'
  )
  await send_telegram(text)

async def notify_ack(a: dict, user: str):
  equipo = "TK-" + str(a["asset_id"]) if a["asset_type"]=="tank" \
        else "PU-" + str(a["asset_id"]) if a["asset_type"]=="pump" \
        else f'{a["asset_type"]}-{a["asset_id"]}'
  code = (a.get("code") or "").upper()
  text = (
    f'🆗 <b>ACK</b> por <b>{_esc(user)}</b>\n'
    f'<b>Equipo:</b> {_esc(equipo)}\n'
    + (f'<b>Código:</b> {_esc(code)}\n' if code else "")
  )
  await send_telegram(text)
