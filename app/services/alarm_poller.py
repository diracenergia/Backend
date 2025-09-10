# app/services/alarm_poller.py
from __future__ import annotations
import os, time, threading, logging
from typing import Optional, Dict, Any
from app.core.db import get_conn
from app.services import notify_alarm

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="ts=%(asctime)s level=%(levelname)s module=%(name)s msg=%(message)s",
)
log = logging.getLogger("alarm-poller")

_thread: Optional[threading.Thread] = None
_stop = threading.Event()

BATCH = int(os.getenv("ALARM_POLL_BATCH", "50"))
SLEEP_EMPTY = float(os.getenv("ALARM_POLL_SLEEP_EMPTY", "1.0"))  # seg si no hay pendientes
SLEEP_BUSY  = float(os.getenv("ALARM_POLL_SLEEP_BUSY",  "0.2"))  # seg si hubo envíos
ONLY_ACTIVE = os.getenv("ALARM_POLL_ONLY_ACTIVE", "true").lower() == "true"

def _row_to_evt(a: Dict[str, Any]) -> Dict[str, Any]:
    # Evento con la forma que espera notify_alarm.send(...)
    return {
        "op": "RAISED",  # solo disparamos en creación
        "asset_type": a["asset_type"],
        "asset_id":   a["asset_id"],
        "code":       (a.get("code") or "").upper(),
        "severity":   (a.get("severity") or "").upper(),
        "message":    a.get("message") or "",
        "ts_raised":  a.get("ts_raised"),
        "alarm_id":   a["id"],
        # extras opcionales si tu template los usa:
        # "value": a.get("extra", {}).get("value"),
        # "threshold": a.get("extra", {}).get("threshold"),
    }

def _process_once() -> int:
    # Lee pendientes y marca tg_notified_at para no duplicar
    with get_conn() as conn, conn.cursor() as cur:
        sql = f"""
            select id, asset_type, asset_id, code, severity, message, ts_raised, extra
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
        count_ok = 0

        for r in rows:
            a = dict(zip(cols, r))
            try:
                notify_alarm.send(_row_to_evt(a))  # usa tu sender existente
                cur.execute("update public.alarms set tg_notified_at = now() where id = %s", (a["id"],))
                count_ok += 1
            except Exception as e:
                log.exception("telegram_error alarm_id=%s err=%s", a["id"], e)
        conn.commit()
        return count_ok

def _loop():
    log.info("poller start batch=%s", BATCH)
    try:
        while not _stop.is_set():
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
