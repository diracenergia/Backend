# app/services/alarm_listener.py
from __future__ import annotations

import os
import json
import select
import asyncio
import logging
from threading import Thread, Event
from typing import Optional

import psycopg  # v3

from app.core.db import EVENTS_DSN  # DSN especial para LISTEN/NOTIFY
from app.services.notify_alarm import notify_alarm

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="ts=%(asctime)s level=%(levelname)s module=%(name)s msg=%(message)s",
)
log = logging.getLogger("alarm-listener")

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
CHANNEL = (os.getenv("ALARM_NOTIFY_CHANNEL") or "alarm_events").strip()

# Thread y stop flag
_thread: Optional[Thread] = None
_stop_evt: Optional[Event] = None


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _parse_payload(raw: str) -> Optional[dict]:
    try:
        payload = json.loads(raw)
        log.debug("parse ok payload_keys=%s", list(payload.keys()))
        return payload
    except Exception as e:
        log.error("json_invalid err=%s raw_preview=%s", e, raw[:200])
        return None


def _op_from_payload(payload: dict) -> str:
    return (payload.get("op") or payload.get("operation") or "").upper()


def _should_send(payload: dict) -> bool:
    """
    Enviamos SIEMPRE cruces de umbral:
      - RAISED: se cruzó el umbral
      - CLEARED: volvió a la normalidad
    No deduplicamos ni anti-spam acá.
    """
    op = _op_from_payload(payload)
    decision = op in {"RAISED", "RAISE", "CLEARED", "CLEAR"}
    log.debug("should_send op=%s decision=%s", op, decision)
    return decision


async def _dispatch_async(payload: dict) -> None:
    try:
        log.info(
            "dispatch start op=%s asset_type=%s asset_id=%s code=%s",
            _op_from_payload(payload),
            payload.get("asset_type"),
            payload.get("asset_id"),
            payload.get("code"),
        )
        await notify_alarm(payload)
        log.info("dispatch done status=sent")
    except Exception as e:
        log.exception("dispatch error err=%s", e)


def _listen_loop(stop_evt: Event) -> None:
    """
    Hilo bloqueante que hace LISTEN al canal y despacha a notify_alarm.
    Usa select() para esperar notificaciones sin ocupar CPU.
    """
    log.info("loop starting channel=%s dsn_present=%s", CHANNEL, bool(EVENTS_DSN))

    # Event loop propio para corrutinas
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Conexión en autocommit para recibir notificaciones inmediatamente
    try:
        conn = psycopg.connect(EVENTS_DSN, autocommit=True, application_name="alarm-listener")
        log.info("db_connect ok")
    except Exception as e:
        log.exception("db_connect error err=%s", e)
        return

    try:
        with conn.cursor() as cur:
            cur.execute(f'LISTEN "{CHANNEL}";')
        log.info("listen_subscribed channel=%s", CHANNEL)
    except Exception as e:
        log.exception("listen_subscribe error err=%s channel=%s", e, CHANNEL)
        try:
            conn.close()
        except Exception:
            pass
        return

    try:
        fd = conn.fileno()
        while not stop_evt.is_set():
            ready, _, _ = select.select([fd], [], [], 2.0)  # 2s para chequear stop flag
            if not ready:
                continue

            conn.poll()
            while conn.notifies:
                n = conn.notifies.pop(0)
                raw = n.payload or ""
                log.debug("notify_recv len=%s", len(raw))
                payload = _parse_payload(raw)
                if not payload:
                    continue

                if _should_send(payload):
                    loop.create_task(_dispatch_async(payload))
                else:
                    log.info(
                        "notify_skip op=%s reason=not_threshold_cross",
                        _op_from_payload(payload),
                    )
    except Exception as e:
        log.exception("loop error err=%s", e)
    finally:
        try:
            conn.close()
            log.info("db_conn closed")
        except Exception:
            pass
        try:
            loop.stop()
            loop.close()
        except Exception:
            pass
        log.info("loop stopped")


# -----------------------------------------------------------------------------
# API pública
# -----------------------------------------------------------------------------
def start_alarm_listener() -> None:
    global _thread, _stop_evt
    if _thread and _thread.is_alive():
        log.warning("already_started")
        return
    _stop_evt = Event()
    _thread = Thread(target=_listen_loop, args=(_stop_evt,), daemon=True)
    _thread.start()
    log.info("thread_started")


def stop_alarm_listener() -> None:
    global _thread, _stop_evt
    if _stop_evt:
        _stop_evt.set()
    if _thread:
        _thread.join(timeout=2.0)
    log.info("thread_stopped")


# -----------------------------------------------------------------------------
# Self-test opcional: permite verificar sin eventos reales
# -----------------------------------------------------------------------------
async def selftest_fire_samples() -> None:
    """
    Llama a notify_alarm() con dos payloads de ejemplo (RAISED y CLEARED).
    Útil para probar formato y envío a Telegram.
    """
    sample_raised = {
        "op": "RAISED",
        "asset_type": "tank",
        "asset_id": 12,
        "code": "LEVEL_HIGH",
        "message": "Nivel alto sobre >90%",
        "severity": "WARNING",
        "value": 92.3,
        "threshold": 90.0,
        "ts_raised": "2025-09-10T10:00:00Z",
    }
    sample_cleared = {
        "op": "CLEARED",
        "asset_type": "tank",
        "asset_id": 12,
        "code": "LEVEL_HIGH",
        "message": "Nivel normalizado",
        "severity": "INFO",
        "value": 88.0,
        "threshold": 90.0,
        "ts_cleared": "2025-09-10T10:05:00Z",
    }

    log.info("selftest start")
    await notify_alarm(sample_raised)
    await notify_alarm(sample_cleared)
    log.info("selftest done")
