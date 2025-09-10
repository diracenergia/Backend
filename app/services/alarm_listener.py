# app/services/alarm_listener.py
from __future__ import annotations

import os
import json
import time
import threading
import logging
from typing import Optional, Dict, Any

from app.core.db import get_conn  # debe devolver una psycopg.Connection (v3)
from app.services import notify_alarm  # donde está la lógica de Telegram

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="ts=%(asctime)s level=%(levelname)s module=%(name)s msg=%(message)s",
)
log = logging.getLogger("alarm-listener")

CHAN = os.getenv("ALARM_NOTIFY_CHANNEL", "alarm_events")

# Estado interno
_thread: Optional[threading.Thread] = None
_stop = threading.Event()
_last_sent: list[dict] = []   # cache de últimos mensajes enviados (debug)

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
    """
    Regla simple: enviar solo RAISED/CLEARED que tengan asset_type/asset_id/code.
    Podés agregar filtros más finos acá si querés.
    """
    op = evt.get("op")
    ok = bool(op in ("RAISED", "CLEARED")
              and evt.get("asset_type")
              and evt.get("asset_id") is not None
              and evt.get("code"))
    log.info("should_send op=%s decision=%s", op, ok)
    return ok


def _dispatch(evt: Dict[str, Any]) -> None:
    """
    Llama al módulo notify_alarm (que vos ya tenés mandando Telegram).
    """
    try:
        log.info("dispatch start op=%s asset=%s-%s code=%s",
                 evt.get("op"), evt.get("asset_type"), evt.get("asset_id"), evt.get("code"))
        # -> usá el notify que ya tenés. Puedes adaptar campos si tu función espera otros nombres
        status = notify_alarm.send(evt)
        _last_sent.append({"ts": time.time(), "evt": evt, "status": status})
        if len(_last_sent) > 100:
            _last_sent.pop(0)
        log.info("dispatch done status=%s", status)
    except Exception as e:
        log.exception("dispatch error err=%s evt=%r", e, evt)


def _listen_once() -> None:
    """
    Abre conexión, LISTEN, y consume notificaciones con psycopg3:
    - conn.notifies.get(timeout=…)
    """
    log.info("db_conn opening channel=%s", CHAN)
    with get_conn() as conn, conn.cursor() as cur:
        # psycopg3: LISTEN
        cur.execute(f'LISTEN "{CHAN}"')
        try:
            conn.commit()  # por si la conexión no está en autocommit
        except Exception:
            pass
        log.info("listen_subscribed channel=%s", CHAN)

        # Bucle de consumo
        while not _stop.is_set():
            try:
                # psycopg3: queue-like API para notificaciones
                # timeout en segundos (float). Si no llega nada, tira queue.Empty
                notify = conn.notifies.get(timeout=5.0)  # type: ignore[attr-defined]
            except Exception:
                # timeouts u otras excepciones benignas: seguir
                continue

            try:
                log.info("notify_recv pid=%s payload_len=%s", getattr(notify, "pid", None), len(notify.payload))
                evt = _decode_payload(notify.payload)
                if not evt:
                    continue
                if _should_send(evt):
                    _dispatch(evt)
            except Exception as e:
                log.exception("notify handle error err=%s", e)

    log.info("db_conn closed")


def _listen_loop() -> None:
    """
    Loop con reconexión exponencial.
    """
    attempt = 0
    while not _stop.is_set():
        try:
            _listen_once()
            # Si salimos normalmente, reiniciamos intento
            attempt = 0
        except Exception as e:
            attempt += 1
            wait_s = min(_RETRY_MAX, _RETRY_BASE ** attempt)
            log.exception("loop error err=%r; retrying in %.1fs", e, wait_s)
            # Pequeño sleep antes de reintentar
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
    log.info("thread started")


def stop_alarm_listener() -> None:
    global _thread
    _stop.set()
    if _thread:
        _thread.join(timeout=5)
    log.info("thread stopped")
