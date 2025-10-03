# app/services/alarm_listener.py
from __future__ import annotations
import os, json, time, threading, logging
from typing import Optional, Dict, Any

from app.core.db import get_events_conn  # ⬅️ usar la conexión directa
from app.services import notify_alarm

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="ts=%(asctime)s level=%(levelname)s module=%(name)s msg=%(message)s",
)
log = logging.getLogger("alarm-listener")

CHAN = os.getenv("ALARM_NOTIFY_CHANNEL", "alarm_events")

_thread: Optional[threading.Thread] = None
_stop = threading.Event()
_last_sent: list[dict] = []

_RETRY_BASE = 1.5
_RETRY_MAX = 30.0

def _decode_payload(payload: str) -> Dict[str, Any]:
    try:
        data = json.loads(payload)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        log.exception("decode error err=%s payload=%r", e, payload[:200])
        return {}

def _should_send(evt: Dict[str, Any]) -> bool:
    op = evt.get("op")
    ok = bool(op in ("RAISED", "CLEARED") and evt.get("asset_type") and evt.get("asset_id") is not None and evt.get("code"))
    log.info("should_send op=%s decision=%s", op, ok)
    return ok

def _dispatch(evt: Dict[str, Any]) -> None:
    try:
        log.info("dispatch start op=%s asset=%s-%s code=%s", evt.get("op"), evt.get("asset_type"), evt.get("asset_id"), evt.get("code"))
        status = notify_alarm.send(evt)
        _last_sent.append({"ts": time.time(), "evt": evt, "status": status})
        if len(_last_sent) > 100:
            _last_sent.pop(0)
        log.info("dispatch done status=%s", status)
    except Exception as e:
        log.exception("dispatch error err=%s evt=%r", e, evt)

def _listen_once() -> None:
    log.info("db_conn opening(channel=%s)", CHAN)
    with get_events_conn() as conn, conn.cursor() as cur:
        cur.execute(f'LISTEN "{CHAN}"')
        # autocommit=True en la conexión; no hace falta commit extra
        log.info("listen_subscribed channel=%s dsn=%s", CHAN, getattr(conn, "dsn", "<hidden>"))

        while not _stop.is_set():
            try:
                # psycopg3: iterador de notificaciones
                for notify in conn.notifies(timeout=5.0):
                    log.info("notify_recv pid=%s payload_len=%s", getattr(notify, "pid", None), len(notify.payload))
                    evt = _decode_payload(notify.payload)
                    if evt and _should_send(evt):
                        _dispatch(evt)
            except TimeoutError:
                # quiet period; seguir
                continue
            except Exception as e:
                log.exception("notifies loop error err=%s", e)
                break
    log.info("db_conn closed")

def _listen_loop() -> None:
    attempt = 0
    while not _stop.is_set():
        try:
            _listen_once()
            attempt = 0
        except Exception as e:
            attempt += 1
            wait_s = min(_RETRY_MAX, _RETRY_BASE ** attempt)
            log.exception("loop error err=%r; retry in %.1fs", e, wait_s)
            t_end = time.time() + wait_s
            while time.time() < t_end and not _stop.is_set():
                time.sleep(0.1)
    log.info("loop stopped")

def start_alarm_listener() -> None:
    global _thread
    if _thread and _thread.is_alive():
        log.info("already running")
        return
    _stop.clear()
    _thread = threading.Thread(target=_listen_loop, name="alarm-listener", daemon=True)
    _thread.start()
    log.info("thread started")

def stop_alarm_listener() -> None:
    global _thread
    _stop.set()
    if _thread:
        _thread.join(timeout=5)
    log.info("thread stopped")
