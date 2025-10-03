# app/services/alarm_poller.py
from __future__ import annotations
import os, time, threading, logging
from typing import Optional, Dict, Any
from app.core.db import get_conn

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="ts=%(asctime)s level=%(levelname)s module=%(name)s msg=%(message)s",
)
log = logging.getLogger("alarm-poller")

DEBUG_TG = os.getenv("TELEGRAM_DEBUG", "false").lower() in ("1", "true", "yes")

# --- Sender de Telegram con LOGS ---
try:
    from app.services.telegram import send as _tg_send  # si tenés un sender propio
    def tg_send(text: str):
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
        token = os.environ.get("TELEGRAM_BOT_TOKEN")
        chat  = os.environ.get("TELEGRAM_CHAT_ID")
        if not token or not chat:
            raise RuntimeError("Faltan TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID")

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = json.dumps({"chat_id": chat, "text": text, "parse_mode": "HTML"}).encode("utf-8")
        req = Request(url, data=payload, headers={"Content-Type": "application/json"})

        if DEBUG_TG:
            log.info("tg_send(urllib) url=%s chat=%s len=%s", url, chat, len(text))

        try:
            with urlopen(req, timeout=12) as resp:
                status = resp.getcode()
                body = resp.read(1000).decode("utf-8", "replace")
            if DEBUG_TG or status != 200:
                log.info("tg_send(urllib) status=%s body=%s", status, body)
            if status != 200:
                raise RuntimeError(f"Telegram fail status={status} body={body}")
        except HTTPError as he:
            b = he.read(1000).decode("utf-8", "replace") if he.fp else ""
            log.info("tg_send(urllib) HTTPError code=%s body=%s", he.code, b)
            raise
        except URLError as ue:
            log.info("tg_send(urllib) URLError reason=%s", ue.reason)
            raise


# ---- Config poller ----
_thread: Optional[threading.Thread] = None
_stop = threading.Event()
BATCH = int(os.getenv("ALARM_POLL_BATCH", "50"))
SLEEP_EMPTY = float(os.getenv("ALARM_POLL_SLEEP_EMPTY", "1.0"))
SLEEP_BUSY  = float(os.getenv("ALARM_POLL_SLEEP_BUSY",  "0.2"))
ONLY_ACTIVE = os.getenv("ALARM_POLL_ONLY_ACTIVE", "true").lower() == "true"

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

def _process_once() -> int:
    with get_conn() as conn, conn.cursor() as cur:
        sql = f"""
            select id, asset_type, asset_id, code, severity, message, ts_raised
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
        log.info("pending=%s", len(rows))

        sent = 0
        for r in rows:
            a = dict(zip(cols, r))
            try:
                preview = f"{a['asset_type']}:{a['asset_id']}|{(a.get('code') or '').upper()}|{(a.get('severity') or '').upper()}"
                log.info("sending alarm_id=%s %s", a["id"], preview)

                tg_send(_fmt_alarm(a))  # envío

                cur.execute("update public.alarms set tg_notified_at = now() where id = %s", (a["id"],))
                sent += 1
                log.info("sent_ok alarm_id=%s", a["id"])
            except Exception as e:
                log.exception("telegram_error alarm_id=%s err=%s", a["id"], e)

        conn.commit()
        log.info("cycle_done sent=%s", sent)
        return sent

def _loop():
    log.info("poller start batch=%s only_active=%s", BATCH, ONLY_ACTIVE)
    while not _stop.is_set():
        try:
            n = _process_once()
            time.sleep(SLEEP_BUSY if n else SLEEP_EMPTY)
        except Exception as e:
            log.exception("poller loop error err=%s", e)
            time.sleep(2.0)
    log.info("poller stopped")

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
