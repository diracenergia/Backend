# app/services/alarm_poller.py
from __future__ import annotations

import os
import time
import threading
import logging
from typing import Optional, Dict, Any, List, Tuple

from app.core.db import get_conn

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="ts=%(asctime)s level=%(levelname)s module=%(name)s msg=%(message)s",
)
log = logging.getLogger("alarm-poller")

# ------------------------------------------------------------------------------
# Telegram sender (dos modos: servicio local o API directa)
# ------------------------------------------------------------------------------
DEBUG_TG = os.getenv("TELEGRAM_DEBUG", "false").lower() in ("1", "true", "yes")
TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "true").lower() in ("1", "true", "yes")

try:
    from app.services.telegram import send as _tg_send  # si tenés un sender propio

    def tg_send(text: str):
        if not TELEGRAM_ENABLED:
            if DEBUG_TG:
                log.info("tg_send(disabled) %s chars", len(text))
            return
        if DEBUG_TG:
            log.info("tg_send(local) preview len=%s", len(text))
        _tg_send(text)
        if DEBUG_TG:
            log.info("tg_send(local) OK")

except Exception:
    import json
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError, URLError

    def tg_send(text: str):
        if not TELEGRAM_ENABLED:
            if DEBUG_TG:
                log.info("tg_send(disabled) %s chars", len(text))
            return
        token = os.environ.get("TELEGRAM_BOT_TOKEN")
        chat = os.environ.get("TELEGRAM_CHAT_ID")
        if not token or not chat:
            raise RuntimeError("Faltan TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID")

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = json.dumps(
            {"chat_id": chat, "text": text, "parse_mode": "HTML"}
        ).encode("utf-8")
        req = Request(url, data=payload, headers={"Content-Type": "application/json"})

        if DEBUG_TG:
            log.info("tg_send(urllib) url=%s chat=%s len=%s", url, chat, len(text))

        try:
            with urlopen(req, timeout=12) as resp:
                status = resp.getcode()
                body = resp.read(500).decode("utf-8", "replace")
            if DEBUG_TG or status != 200:
                log.info("tg_send(urllib) status=%s body=%s", status, body)
            if status != 200:
                raise RuntimeError(f"Telegram fail status={status} body={body}")
        except HTTPError as he:
            b = he.read(500).decode("utf-8", "replace") if he.fp else ""
            log.info("tg_send(urllib) HTTPError code=%s body=%s", he.code, b)
            raise
        except URLError as ue:
            log.info("tg_send(urllib) URLError reason=%s", ue.reason)
            raise

# ------------------------------------------------------------------------------
# Config del poller
# ------------------------------------------------------------------------------
_thread: Optional[threading.Thread] = None
_stop = threading.Event()

BATCH = int(os.getenv("ALARM_POLL_BATCH", "50"))
SLEEP_EMPTY = float(os.getenv("ALARM_POLL_SLEEP_EMPTY", "1.0"))
SLEEP_BUSY = float(os.getenv("ALARM_POLL_SLEEP_BUSY", "0.5"))
ONLY_ACTIVE = os.getenv("ALARM_POLL_ONLY_ACTIVE", "true").lower() in ("1", "true", "yes")
BACKOFF_MAX = float(os.getenv("ALARM_POLL_BACKOFF_MAX", "60"))

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _fmt_alarm(a: Dict[str, Any]) -> str:
    sev = (a.get("severity") or "").upper()
    code = (a.get("code") or "").upper()
    text = f"🚨 <b>{sev}</b> | {a['asset_type']}:{a['asset_id']} | {code}"
    msg = a.get("message")
    if msg:
        text += f"\n{msg}"
    try:
        ts_local = a["ts_raised"].astimezone().strftime("%Y-%m-%d %H:%M:%S")
        text += f"\n⏱ {ts_local}"
    except Exception:
        pass
    return text


def _claim_pending(limit: int) -> List[Dict[str, Any]]:
    """
    Reclama un lote de alarmas pendientes (tg_notified_at IS NULL) con FOR UPDATE SKIP LOCKED,
    y las marca como notificadas AHORA para que ningún otro proceso las tome.
    Devolvemos los registros completos (para armar el mensaje) y soltamos la conexión rápido.
    """
    sql_sel = f"""
        select id, asset_type, asset_id, code, severity, message, ts_raised
        from public.alarms
        where telegram = true
          and {"is_active = true and" if ONLY_ACTIVE else ""}
              tg_notified_at is null
        order by ts_raised asc
        limit %s
        for update skip locked
    """
    rows: List[Tuple] = []
    cols: List[str] = []
    ids: List[int] = []

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql_sel, (limit,))
        rows = cur.fetchall()
        if not rows:
            return []
        cols = [d[0] for d in cur.description]
        ids = [r[0] for r in rows]  # id es la primera columna

        # Marcamos como "tomadas" ya mismo para no duplicar envío
        cur.execute(
            "update public.alarms set tg_notified_at = now() where id = any(%s)",
            (ids,),
        )
        conn.commit()

    # Convertimos fuera de la conexión
    return [dict(zip(cols, r)) for r in rows]


def _revert_failed(ids: List[int]) -> None:
    """Si falla el envío, volvemos a poner tg_notified_at = NULL para reintentar en el próximo ciclo."""
    if not ids:
        return
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "update public.alarms set tg_notified_at = null where id = any(%s)",
            (ids,),
        )
        conn.commit()
    log.info("reverted_failed ids=%s", ids)

# ------------------------------------------------------------------------------
# Ciclo principal
# ------------------------------------------------------------------------------
def _loop():
    log.info("poller start batch=%s only_active=%s", BATCH, ONLY_ACTIVE)
    backoff = 1.0

    while not _stop.is_set():
        try:
            lote = _claim_pending(BATCH)
            if not lote:
                time.sleep(SLEEP_EMPTY)
                backoff = 1.0
                continue

            log.info("pending=%s", len(lote))
            failed: List[int] = []

            for a in lote:
                try:
                    tg_send(_fmt_alarm(a))
                    # ritmo mínimo para no saturar API externa
                    time.sleep(0.05)
                    log.info("sent_ok alarm_id=%s", a["id"])
                except Exception as e:
                    log.exception("telegram_error alarm_id=%s err=%s", a["id"], e)
                    failed.append(a["id"])

            if failed:
                _revert_failed(failed)

            time.sleep(SLEEP_BUSY)
            backoff = 1.0

        except Exception as e:
            log.exception("poller loop error err=%s", e)
            time.sleep(backoff)
            backoff = min(backoff * 2.0, BACKOFF_MAX)

    log.info("poller stopped")

# ------------------------------------------------------------------------------
# Control de hilo
# ------------------------------------------------------------------------------
def start_alarm_poller():
    global _thread
    if _thread and _thread.is_alive():
        log.info("already running")
        return
    _stop.clear()
    _thread = threading.Thread(target=_loop, name="alarm-poller", daemon=True)
    _thread.start()
    log.info("thread started")


def stop_alarm_poller():
    global _thread
    _stop.set()
    if _thread:
        _thread.join(timeout=5)
    log.info("thread stopped")
