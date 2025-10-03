# app/services/alarm_poller.py
from __future__ import annotations

"""
Alarm Poller (seguro por defecto)

- Por defecto NO arranca (ALARM_POLLER_ENABLED=0).
- Aunque arranque, viene en DRY-RUN y NO envía nada (ALARM_POLLER_DRY_RUN=1).
- DISABLE_ALARMS=1 también evita que arranque.
- start_alarm_poller()/stop_alarm_poller() existen y son seguras aunque se llamen.
"""

import os
import time
import threading
import logging
from typing import Optional, Dict, Any

from app.core.db import get_conn

# -------------------------------
# Configuración de logging
# -------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="ts=%(asctime)s level=%(levelname)s module=%(name)s msg=%(message)s",
)
log = logging.getLogger("alarm-poller")

# -------------------------------
# Flags de control (seguros por defecto)
# -------------------------------
POLLER_ENABLED = os.getenv("ALARM_POLLER_ENABLED", "0").lower() in ("1", "true", "yes")
DISABLE_ALARMS = os.getenv("DISABLE_ALARMS", "1").lower() in ("1", "true", "yes")
# Si por error habilitan el poller, DRY_RUN evita que se envíe nada
DRY_RUN = os.getenv("ALARM_POLLER_DRY_RUN", "1").lower() in ("1", "true", "yes")

DEBUG_TG = os.getenv("TELEGRAM_DEBUG", "false").lower() in ("1", "true", "yes")

# -------------------------------
# Sender de Telegram (seguro)
# -------------------------------
def tg_send(text: str) -> None:
    """
    En DRY_RUN no hace nada (solo log).
    Si algún día querés habilitar envíos reales, seteá ALARM_POLLER_DRY_RUN=0
    y agregá las credenciales TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID.
    """
    if DRY_RUN:
        if DEBUG_TG:
            log.info("tg_send(DRY_RUN) len=%s (no se envía nada)", len(text))
        return

    # En caso de habilitar envíos reales (no recomendado ahora)
    try:
        from app.services.telegram import send as _tg_send
        if DEBUG_TG:
            log.info("tg_send(local) preview len=%s", len(text))
        _tg_send(text)
        if DEBUG_TG:
            log.info("tg_send(local) OK")
    except Exception:
        # Fallback simple por urllib (se usará solo si DRY_RUN=0)
        import json
        from urllib.request import Request, urlopen
        from urllib.error import HTTPError, URLError

        token = os.environ.get("TELEGRAM_BOT_TOKEN")
        chat  = os.environ.get("TELEGRAM_CHAT_ID")
        if not token or not chat:
            log.warning("tg_send: faltan TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID (no se envía)")
            return

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = json.dumps({
            "chat_id": chat,
            "text": text,
            "parse_mode": "HTML"
        }).encode("utf-8")
        req = Request(url, data=payload, headers={"Content-Type": "application/json"})

        if DEBUG_TG:
            log.info("tg_send(urllib) url=%s chat=%s len=%s", url, chat, len(text))

        try:
            with urlopen(req, timeout=12) as resp:
                status = resp.getcode()
                body = resp.read(500).decode("utf-8", "replace")
            if DEBUG_TG or status != 200:
                log.info("tg_send(urllib) status=%s body=%s", status, body)
        except HTTPError as he:
            b = he.read(500).decode("utf-8", "replace") if he.fp else ""
            log.warning("tg_send(urllib) HTTPError code=%s body=%s", he.code, b)
        except URLError as ue:
            log.warning("tg_send(urllib) URLError reason=%s", ue.reason)

# -------------------------------
# Config del poller
# -------------------------------
_thread: Optional[threading.Thread] = None
_stop = threading.Event()

BATCH = int(os.getenv("ALARM_POLL_BATCH", "50"))
SLEEP_EMPTY = float(os.getenv("ALARM_POLL_SLEEP_EMPTY", "1.0"))
SLEEP_BUSY  = float(os.getenv("ALARM_POLL_SLEEP_BUSY",  "0.2"))
ONLY_ACTIVE = os.getenv("ALARM_POLL_ONLY_ACTIVE", "true").lower() in ("1", "true", "yes")

# -------------------------------
# Helpers
# -------------------------------
def _fmt_alarm(a: Dict[str, Any]) -> str:
    sev  = (a.get("severity") or "").upper()
    code = (a.get("code") or "").upper()
    text = f"🚨 <b>{sev}</b> | {a['asset_type']}:{a['asset_id']} | {code}"
    if a.get("message"):
        text += f"\n{a['message']}"
    try:
        ts_local = a["ts_raised"].astimezone().strftime("%Y-%m-%d %H:%M:%S")
        text += f"\n⏱ {ts_local}"
    except Exception:
        pass
    return text

# -------------------------------
# Ciclo principal
# -------------------------------
def _process_once() -> int:
    """
    Lee alarmas pendientes. En DRY_RUN:
      - NO envía Telegram.
      - NO marca tg_notified_at.
    Así no cambia estado en DB si solo queremos observar.
    """
    with get_conn() as conn, conn.cursor() as cur:
        sql = f"""
            select id, asset_type, asset_id, code, severity, message, ts_raised, telegram
            from public.alarms
            where telegram = true
              and {"is_active = true and" if ONLY_ACTIVE else ""}
                  tg_notified_at is null
            order by ts_raised asc
            limit %s
            for update skip locked
        """
        cur.execute(sql, (BATCH,))
        rows = cur.fetchall()
        if not rows:
            if DEBUG_TG:
                log.debug("no_pending")
            return 0

        cols = [d[0] for d in cur.description]
        log.info("pending=%s (dry_run=%s)", len(rows), DRY_RUN)

        sent = 0
        for r in rows:
            a = dict(zip(cols, r))
            preview = f"{a['asset_type']}:{a['asset_id']}|{(a.get('code') or '').upper()}|{(a.get('severity') or '').upper()}"

            if DRY_RUN:
                # Solo log, no envío ni update a tg_notified_at
                log.info("DRY_RUN alarm_id=%s %s (no se envía ni se marca)", a["id"], preview)
                continue

            try:
                log.info("sending alarm_id=%s %s", a["id"], preview)
                tg_send(_fmt_alarm(a))
                cur.execute(
                    "update public.alarms set tg_notified_at = now() where id = %s",
                    (a["id"],),
                )
                sent += 1
                log.info("sent_ok alarm_id=%s", a["id"])
            except Exception as e:
                log.exception("telegram_error alarm_id=%s err=%s", a["id"], e)

        if not DRY_RUN:
            conn.commit()
        else:
            # Evita commits innecesarios en DRY_RUN
            conn.rollback()

        log.info("cycle_done sent=%s (dry_run=%s)", sent, DRY_RUN)
        return sent

def _loop():
    log.info(
        "poller start enabled=%s disable_alarms=%s dry_run=%s batch=%s only_active=%s",
        POLLER_ENABLED, DISABLE_ALARMS, DRY_RUN, BATCH, ONLY_ACTIVE
    )
    while not _stop.is_set():
        try:
            n = _process_once()
            time.sleep(SLEEP_BUSY if n else SLEEP_EMPTY)
        except Exception:
            # No queremos spam: WARNING y traza
            log.exception("poller loop error")
            time.sleep(2.0)
    log.info("poller stopped")

# -------------------------------
# API pública (segura)
# -------------------------------
def start_alarm_poller() -> None:
    """
    No arranca si:
      - POLLER_ENABLED es falso (default), o
      - DISABLE_ALARMS es verdadero (default)
    """
    if not POLLER_ENABLED or DISABLE_ALARMS:
        log.info(
            "alarm_poller: disabled by env (ALARM_POLLER_ENABLED=%s DISABLE_ALARMS=%s) — not starting",
            POLLER_ENABLED, DISABLE_ALARMS,
        )
        return

    global _thread
    if _thread and _thread.is_alive():
        log.info("alarm_poller: already running")
        return

    _stop.clear()
    _thread = threading.Thread(target=_loop, name="alarm-poller", daemon=True)
    _thread.start()
    log.info("alarm_poller: thread started")

def stop_alarm_poller() -> None:
    global _thread
    _stop.set()
    if _thread:
        _thread.join(timeout=5)
    log.info("alarm_poller: thread stopped")
