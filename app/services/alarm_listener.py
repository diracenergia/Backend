# app/services/alarm_listener.py
from __future__ import annotations

import os
import json
import select
from threading import Thread, Event
from typing import Optional

import psycopg  # v3

from app.core.db import EVENTS_DSN  # DSN especial para LISTEN/NOTIFY
from app.services.notify_alarm import notify_alarm  # nuestro notifier async
import asyncio


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
def _debug(msg: str) -> None:
    print(f"[alarm-listener] {msg}")


def _parse_payload(raw: str) -> Optional[dict]:
    try:
        return json.loads(raw)
    except Exception as e:
        _debug(f"❌ JSON inválido: {e} raw={raw[:200]}")
        return None


def _should_send(payload: dict) -> bool:
    """
    Enviamos SIEMPRE cruces de umbral:
      - RAISED: se cruzó el umbral
      - CLEARED: volvió a la normalidad
    No deduplicamos ni aplicamos anti-spam acá.
    """
    op = (payload.get("op") or payload.get("operation") or "").upper()
    # Permitimos también alias comunes por si el publisher manda variantes.
    if op in {"RAISED", "RAISE"}:
        return True
    if op in {"CLEARED", "CLEAR"}:
        return True
    return False


async def _dispatch_async(payload: dict) -> None:
    try:
        await notify_alarm(payload)
    except Exception as e:
        _debug(f"❌ error en notify_alarm: {e}")


def _listen_loop(stop_evt: Event) -> None:
    """
    Hilo bloqueante que hace LISTEN al canal y despacha a notify_alarm.
    Usa select() para esperar notificaciones sin ocupar CPU.
    """
    _debug(f"iniciando loop; canal={CHANNEL}")

    # Un event loop propio para nuestras corrutinas
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Conexión en autocommit para que reciba notificaciones inmediatamente
    try:
        conn = psycopg.connect(EVENTS_DSN, autocommit=True, application_name="alarm-listener")
    except Exception as e:
        _debug(f"❌ no se pudo conectar a EVENTS_DSN: {e}")
        return

    try:
        with conn.cursor() as cur:
            cur.execute(f'LISTEN "{CHANNEL}";')
        _debug("LISTEN suscripto correctamente")
    except Exception as e:
        _debug(f"❌ error al suscribirse a LISTEN {CHANNEL}: {e}")
        try:
            conn.close()
        except Exception:
            pass
        return

    try:
        fd = conn.fileno()
        while not stop_evt.is_set():
            # Espera hasta 2s por actividad de socket para poder chequear el stop flag
            ready, _, _ = select.select([fd], [], [], 2.0)
            if not ready:
                continue

            # Trae notificaciones pendientes
            conn.poll()
            while conn.notifies:
                n = conn.notifies.pop(0)
                raw = n.payload or ""
                payload = _parse_payload(raw)
                if not payload:
                    continue

                if _should_send(payload):
                    loop.create_task(_dispatch_async(payload))
                else:
                    # Mensajes que no son cruces de umbral (UPDATE, ACK, etc.)
                    pass
    except Exception as e:
        _debug(f"❌ loop error: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            loop.stop()
            loop.close()
        except Exception:
            pass
        _debug("loop finalizado")


# -----------------------------------------------------------------------------
# API pública
# -----------------------------------------------------------------------------
def start_alarm_listener() -> None:
    global _thread, _stop_evt
    if _thread and _thread.is_alive():
        _debug("ya estaba iniciado")
        return
    _stop_evt = Event()
    _thread = Thread(target=_listen_loop, args=(_stop_evt,), daemon=True)
    _thread.start()
    _debug("started")


def stop_alarm_listener() -> None:
    global _thread, _stop_evt
    if _stop_evt:
        _stop_evt.set()
    if _thread:
        _thread.join(timeout=2.0)
    _debug("stopped")
