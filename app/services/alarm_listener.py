# app/services/alarm_listener.py
from __future__ import annotations

import os, json, time, threading, logging, queue, inspect
from typing import Optional, Dict, Any, Callable, Tuple

from app.core.db import get_conn
from app.services import notify_alarm  # tu sender a Telegram

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="ts=%(asctime)s level=%(levelname)s module=%(name)s msg=%(message)s",
)
log = logging.getLogger("alarm-listener")

CHAN = os.getenv("ALARM_NOTIFY_CHANNEL", "alarm_events")
__VERSION__ = "alist-2025-09-10T14:05Z"

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


# ---------- Soporte multi-API de psycopg3 para notifies ----------

def _supports_timeout(fn: Callable) -> bool:
    """Detecta si la función acepta 'timeout' en la firma."""
    try:
        sig = inspect.signature(fn)
        return any(p.kind in (p.KEYWORD_ONLY, p.POSITIONAL_OR_KEYWORD) and p.name == "timeout"
                   for p in sig.parameters.values())
    except Exception:
        return False


def _get_notifies_source(conn) -> Tuple[str, object, Optional[Callable[[], object]]]:
    """
    Devuelve (mode, obj, factory)
      - mode 'queue'     -> obj tiene .get(timeout)
      - mode 'gen_to'    -> obj es generator creado con timeout (factory recrea)
      - mode 'gen_block' -> obj es generator sin timeout (factory recrea)
    """
    attr = getattr(conn, "notifies", None)
    if attr is None:
        raise AttributeError("connection has no 'notifies'")

    # Caso 1: atributo queue-like
    if not callable(attr):
        if hasattr(attr, "get"):
            log.info("notifies mode=queue type=%s", type(attr).__name__)
            return "queue", attr, None
        # atributo pero no queue: raro
        log.warning("notifies attribute inesperado type=%s", type(attr).__name__)
        return "unknown_attr", attr, None

    # Caso 2: callable -> intentamos con timeout
    mode = "gen_block"
    factory: Optional[Callable[[], object]] = None

    if _supports_timeout(attr):
        # usa timeout kw si lo soporta
        def factory_timeout():
            return attr(timeout=5.0)
        obj = factory_timeout()
        mode = "gen_to"
        factory = factory_timeout
        log.info("notifies mode=gen_to (callable con timeout) obj=%s", type(obj).__name__)
    else:
        # sin timeout (bloqueante)
        def factory_block():
            return attr()
        obj = factory_block()
        mode = "gen_block"
        factory = factory_block
        log.info("notifies mode=gen_block (callable sin timeout) obj=%s", type(obj).__name__)

    return mode, obj, factory


def _next_notify_gen(gen_obj, factory: Optional[Callable[[], object]]):
    """
    Avanza un generator de notifies. Si termina, lo recrea vía factory.
    Devuelve (notify, gen_obj_actualizado)
    """
    try:
        notify = next(gen_obj)
        return notify, gen_obj
    except StopIteration:
        # volvemos a crear el generator
        if factory:
            gen_obj = factory()
            return None, gen_obj
        return None, gen_obj
    except Exception as e:
        log.exception("generator next error err=%s", e)
        return None, gen_obj


# ---------- Loop principal ----------

def _listen_once() -> None:
    log.info("db_conn opening channel=%s version=%s", CHAN, __VERSION__)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f'LISTEN "{CHAN}"')
        try:
            conn.commit()
        except Exception:
            pass
        log.info("listen_subscribed channel=%s", CHAN)

        mode, source, factory = _get_notifies_source(conn)
        last_idle = time.time()

        while not _stop.is_set():
            try:
                if mode == "queue":
                    # Queue-like API
                    notify = source.get(timeout=5.0)  # type: ignore[attr-defined]
                elif mode in ("gen_to", "gen_block"):
                    # Generator API
                    notify, source = _next_notify_gen(source, factory)  # type: ignore[assignment]
                    if notify is None:
                        # sin evento; damos oportunidad a salir
                        if mode == "gen_block":
                            # en gen_block no hay timeout: evitamos busy-loop
                            time.sleep(0.05)
                        # log de idle cada 60s
                        now = time.time()
                        if now - last_idle > 60:
                            log.info("idle waiting channel=%s mode=%s", CHAN, mode)
                            last_idle = now
                        continue
                else:
                    # fallback muy raro: dormimos y reintentamos
                    time.sleep(0.5)
                    continue
            except queue.Empty:
                # queue con timeout sin eventos
                now = time.time()
                if now - last_idle > 60:
                    log.info("idle waiting channel=%s mode=%s", CHAN, mode)
                    last_idle = now
                continue
            except Exception as e:
                log.exception("notifies error err=%s mode=%s", e, mode)
                # Pequeño sleep para evitar loop caliente ante error repetido
                time.sleep(0.1)
                continue

            # Tenemos un notify
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
