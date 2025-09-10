# app/services/alarm_listener.py
from __future__ import annotations

import os
import json
import time
import asyncio
import select
from threading import Thread, Event
from html import escape
from time import monotonic
from typing import Optional, Dict, Any

import psycopg  # v3
from app.core.telegram import send_telegram as tg_send_async  # tu sender async
from app.core.db import get_conn

# =========================
# Config
# =========================
CHAN = os.getenv("ALARM_NOTIFY_CHANNEL", "alarm_events")
DSN  = os.getenv("DATABASE_URL") or os.getenv("DB_URL")

# =========================
# Estado global
# =========================
_stop_evt: Optional[Event] = None
_thread: Optional[Thread] = None

# Anti-spam / dedupe por alarm_id
_SEV_RANK = {"INFO": 1, "WARNING": 2, "CRITICAL": 3}
_last_sent: Dict[int, Dict[str, Any]] = {}   # alarm_id -> {op, sev, th, t}

# =========================
# Helpers
# =========================
def _fmt_asset(asset_type: Optional[str], asset_id: Optional[int]) -> str:
    at = (asset_type or "").lower()
    prefix = {"tank": "TK", "pump": "PU", "valve": "VL"}.get(at, (at[:2] or "AS").upper())
    return f"{prefix}-{asset_id or 0}"

def _fetch_alarm(alarm_id: Optional[int]) -> Optional[dict]:
    if not alarm_id:
        return None
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, asset_type, asset_id, code, severity, message, is_active, extra
                  FROM public.alarms
                 WHERE id = %s
                """,
                (alarm_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
    except Exception as e:
        print(f"[alarm-listener] fetch alarm {alarm_id} error: {e}")
        return None

def _html_from(payload: dict, alarm: Optional[dict]) -> str:
    op         = (payload.get("op") or "").upper()            # RAISED | UPDATED | CLEARED
    alarm_id   = payload.get("alarm_id")
    asset_type = payload.get("asset_type") or (alarm and alarm.get("asset_type"))
    asset_id   = payload.get("asset_id")   or (alarm and alarm.get("asset_id"))
    code       = payload.get("code")       or (alarm and alarm.get("code"))
    severity   = (payload.get("severity") or (alarm and alarm.get("severity","")) or "").upper()
    threshold  = payload.get("threshold")  or (
        alarm and isinstance(alarm.get("extra"), dict) and alarm["extra"].get("threshold")
    ) or ""
    message    = (payload.get("message") or (alarm and alarm.get("message")) or "")

    emoji      = {"RAISED": "🚨", "UPDATED": "🔺", "CLEARED": "✅"}.get(op, "ℹ️")
    asset_lbl  = _fmt_asset(asset_type, asset_id)

    parts = [
        f"{emoji} <b>{escape(op)}</b> · <code>{escape(asset_lbl)}</code> · <code>{escape(str(code or ''))}</code>",
        f"<b>{escape(severity)}</b>" if severity else "",
        escape(message) if message else "",
        f"<i>threshold:</i> {escape(str(threshold))}" if threshold else "",
        f"<i>id:</i> <code>{escape(str(alarm_id))}</code>" if alarm_id else "",
    ]
    return "\n".join(p for p in parts if p)

def _should_send(payload: dict, alarm: Optional[dict]) -> bool:
    """
    Reglas anti-spam:
      - Enviamos 1 sola vez por RAISED (por alarm_id).
      - Enviamos siempre el CLEARED (pero no repetido).
      - UPDATED solo si escala severidad o cambia threshold.
    """
    op = (payload.get("op") or "").upper()
    alarm_id = payload.get("alarm_id")
    if not alarm_id:
        return True  # sin ID no podemos dedup → preferimos enviar

    sev = (payload.get("severity") or (alarm and alarm.get("severity","")) or "").upper()
    th  = payload.get("threshold") or (
        alarm and isinstance(alarm.get("extra"), dict) and alarm["extra"].get("threshold")
    ) or ""

    prev = _last_sent.get(alarm_id)

    if op == "RAISED":
        if prev and prev.get("op") == "RAISED":
            return False
        _last_sent[alarm_id] = {"op": "RAISED", "sev": sev, "th": th, "t": monotonic()}
        return True

    if op == "UPDATED":
        if not prev:
            _last_sent[alarm_id] = {"op": "UPDATED", "sev": sev, "th": th, "t": monotonic()}
            return True
        if _SEV_RANK.get(sev, 0) > _SEV_RANK.get(prev.get("sev",""), 0) or th != prev.get("th"):
            _last_sent[alarm_id] = {"op": "UPDATED", "sev": sev, "th": th, "t": monotonic()}
            return True
        return False

    if op == "CLEARED":
        if prev and prev.get("op") == "CLEARED":
            return False
        _last_sent[alarm_id] = {"op": "CLEARED", "sev": sev, "th": th, "t": monotonic()}
        return True

    # Desconocidos: enviar una vez
    if not prev:
        _last_sent[alarm_id] = {"op": op, "sev": sev, "th": th, "t": monotonic()}
        return True
    return False

def _send_to_telegram(text: str) -> None:
    try:
        res = asyncio.run(tg_send_async(text))
        print("[alarm-listener] tg result:", res)
    except Exception as e:
        print(f"[alarm-listener] telegram send error: {e}")

def _handle_payload(payload: dict):
    alarm = _fetch_alarm(payload.get("alarm_id"))
    if not _should_send(payload, alarm):
        # suprimido por anti-spam
        return
    text = _html_from(payload, alarm)
    _send_to_telegram(text)

# =========================
# Loop principal
# =========================
def _listen_loop(stop_evt: Event):
    if not DSN:
        print("[alarm-listener] ❌ faltan credenciales DB: set DATABASE_URL o DB_URL")
        return

    backoff = 1.0
    while not stop_evt.is_set():
        try:
            with psycopg.connect(DSN, autocommit=True, application_name="alarm-listener") as conn:
                with conn.cursor() as cur:
                    cur.execute(f"LISTEN {CHAN};")
                print(f"[alarm-listener] ✅ listening on '{CHAN}'")

                fd = conn.pgconn.socket  # file descriptor del socket libpq
                backoff = 1.0  # reset backoff al conectar

                while not stop_evt.is_set():
                    # Espera hasta 1s por datos
                    r, _, _ = select.select([fd], [], [], 1.0)
                    if r:
                        # Traer datos del socket a psycopg y preparar notificaciones
                        conn.pgconn.consume_input()
                        conn.poll()  # IMPORTANTE: hace visibles las notifies()

                    # Drenar notificaciones
                    for notify in conn.notifies():
                        try:
                            payload = json.loads(notify.payload)
                            print("[alarm-listener] recv:", payload)
                        except Exception as e:
                            print("[alarm-listener] bad payload:", notify.payload, e)
                            continue
                        _handle_payload(payload)

        except Exception as e:
            print(f"[alarm-listener] error de conexión/loop: {e}")
            # backoff exponencial suave hasta 10s
            time.sleep(backoff)
            backoff = min(backoff * 2, 10.0)

# =========================
# Control público
# =========================
def start_alarm_listener():
    global _stop_evt, _thread
    if _thread and _thread.is_alive():
        return
    _stop_evt = Event()
    _thread = Thread(target=_listen_loop, args=(_stop_evt,), daemon=True)
    _thread.start()
    print("[alarm-listener] started")

def stop_alarm_listener():
    global _stop_evt, _thread
    if _stop_evt:
        _stop_evt.set()
    if _thread:
        _thread.join(timeout=2)
    print("[alarm-listener] stopped")
