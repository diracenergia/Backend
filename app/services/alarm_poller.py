# app/services/alarm_poller.py
from __future__ import annotations
import os, time, threading, logging
from typing import Optional, Dict, Any

from app.core.db import get_conn

# --- Sender de Telegram: usa el módulo local si existe; sino, POST directo ---
try:
    from app.services.telegram import send as tg_send  # tu función síncrona
except Exception:
    import requests
    def tg_send(text: str):
        token = os.environ["TELEGRAM_BOT_TOKEN"]
        chat  = os.environ["TELEGRAM_CHAT_ID"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        r = requests.post(url, json={"chat_id": chat, "text": text, "parse_mode": "HTML"}, timeout=10)
        r.raise_for_status()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="ts=%(asctime)s level=%(levelname)s module=%(name)s msg=%(message)s",
)
log = logging.getLogger("alarm-poller")

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
            return 0

        cols = [d[0] for d in cur.description]
        sent = 0
        for r in rows:
            a = dict(zip(cols, r))
            try:
                tg_send(_fmt_alarm(a))  # síncrono
                cur.execute("update public.alarms set tg_notified_at = now() where id = %s", (a["id"],))
                sent += 1
            except Exception as e:
                log.exception("telegram_error alarm_id=%s err=%s", a["id"], e)

        conn.commit()
        return sent

def _loop():
    log.info("poller start batch=%s", BATCH)
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
