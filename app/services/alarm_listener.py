# app/services/alarm_listener.py
from __future__ import annotations

import os, json, time, threading, logging, queue
from typing import Optional, Dict, Any

from app.core.db import get_conn
from app.services import notify_alarm  # sender a Telegram

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="ts=%(asctime)s level=%(levelname)s module=%(name)s msg=%(message)s",
)
log = logging.getLogger("alarm-listener")

CHAN = os.getenv("ALARM_NOTIFY_CHANNEL", "alarm_events")
__VERSION__ = "alist-2025-09-10T13:45Z"

# Estado interno
_thread: Optional[threading.Thread] = None
_stop = threading.Event()
_last_sent: list[dict] = []

# Backoff reconexión
_RETRY_BASE = 1.5
_RETRY_MAX = 30.0


def _decode_payload(payload: str) -> Dict[str, Any]:
    try:
        data = json.loads(payload)
        if not isinstance(data, dict):
            raise ValueError("payload no es dict")
        return data
    except Exception as e:
        log.exception("decode error err=%s payload=%r", e, payload[:200])
        return {}


def _should_send(evt: Dict[str, Any]) -> bool:
    op = evt.get("op")
    ok = bool(op in ("RAISED", "CLEARED")
              and evt.get("asset_type")
              and evt.get("asset_id") is not None
              and evt.get("code"))
    log.info("should_send op=%s decision=%s", op, ok)
    return ok


def _dispatch(evt: Dict[str, Any]) -> None:
    try:
        log.info("dispatch start op=%s asset=%s-%s code=%s",
                 evt.get("op"), evt.get("asset_type"), evt.get("asset_id"), evt.get("code"))
        status = notify_alarm.send(evt)
        _last_sent.append({"ts": time.time(), "evt": evt, "status": status})
        if len(_last_sent) > 100:
            _last_sent.pop(0)
        log.info("dispatch done status=%s", status)
    except Exception as e:
        log.exception("dispatch error err=%s evt=%r", e, evt)


def _get_notifies_queue(conn):
    """
    Soporta ambas APIs:
      - psycopg3 habitual: conn.notifies  -> queue-like
      - otras builds:     conn.notifies() -> queue-like
    """
    attr = getattr(conn, "notifies", None)
    if attr is None:
        raise AttributeError("connection has no 'notifies'")
    if callable(attr):
        # tu caso actual: es una función que devuelve la cola
        q = attr()
        log.info("notifies source=function -> queue=%s", type(q).__name__)
        return q
    # caso estándar: atributo queue-like
    log.info("notifies source=attribute -> queue=%s", type(attr).__name__)
    return attr


def _listen_once() -> None:
    log.info("db_conn opening channel=%s version=%s", CHAN, __VERSION__)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f'LISTEN "{CHAN}"')
        try:
            conn.commit()
        except Exception:
            pass
        log.info("listen_subscribed channel=%s", CHAN)

        q = _get_notifies_queue(conn)

        last_log = time.time()
        while not _stop.is_set():
            try:
                notify = q.get(timeout=5.0)
            except queue.Empty:
                now = time.time()
                if now - last_log > 60:
                    log.info("idle waiting channel=%s", CHAN)
                    last_log = now
                continue
            except Exception as e:
                log.exception("notifies.get error err=%s", e)
                continue

            try:
                pid = getattr(notify, "pid", None)
                payload = getattr(notify, "payload", None)
                if payload is None:
                    log.warning("notify without payload pid=%s", pid)
                    continue
                log.info("notify_recv pid=%s payload_len=%s", pid, len(payload))
                evt = _decode_payload(payload)
                if evt and _should_send(evt):
                    _dispatch(evt)
            except Exception as e:
                log.exception("notify handle error err=%s", e)

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
            log.exception("loop error err=%r; retrying in %.1fs", e, wait_s)
            for _ in range(int(wait_s * 10)):
                if _stop.is_set():
                    break
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
    log.info("thread started version=%s", __VERSION__)


def stop_alarm_listener() -> None:
    global _thread
    _stop.set()
    if _thread:
        _thread.join(timeout=5)
    log.info("thread stopped")
