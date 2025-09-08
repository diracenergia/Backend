# app/services/alarm_listener.py
import os
import json
import time
import asyncio
import select
from threading import Thread, Event
from html import escape
from time import monotonic

import psycopg  # v3
from app.core.db import get_conn
from app.core.telegram import send_telegram as tg_send_async  # MISMO sender que ACK

_stop_evt: Event | None = None
_thread: Thread | None = None

# ====== Anti-spam / dedupe ======
# Enviamos 1 sola vez por alarma en RAISED, siempre en CLEARED,
# y UPDATED sólo si escala severidad o cambia threshold.
_SEV_RANK = {"INFO": 1, "WARNING": 2, "CRITICAL": 3}
_last_sent: dict[int, dict] = {}   # alarm_id -> {op, sev, th, t}

def _fmt_asset(asset_type: str | None, asset_id: int | None) -> str:
    at = (asset_type or "").lower()
    prefix = {"tank": "TK", "pump": "PU", "valve": "VL"}.get(at, (at[:2] or "AS").upper())
    return f"{prefix}-{asset_id or 0}"

def _fetch_alarm(alarm_id: int | None) -> dict | None:
    if not alarm_id:
        return None
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, asset_type, asset_id, code, severity, message, is_active, extra
              FROM public.alarms
             WHERE id = %s
        """, (alarm_id,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))

def _html_from(payload: dict, alarm: dict | None) -> str:
    op         = (payload.get("op") or "").upper()            # RAISED | UPDATED | CLEARED
    alarm_id   = payload.get("alarm_id")
    asset_type = payload.get("asset_type") or (alarm and alarm.get("asset_type"))
    asset_id   = payload.get("asset_id")   or (alarm and alarm.get("asset_id"))
    code       = payload.get("code")       or (alarm and alarm.get("code"))
    severity   = (payload.get("severity") or (alarm and alarm.get("severity","")) or "").upper()
    threshold  = payload.get("threshold")  or (
        alarm and isinstance(alarm.get("extra"), dict) and alarm["extra"].get("threshold")
    ) or ""
    message    = (alarm and alarm.get("message")) or ""

    emoji      = {"RAISED": "🚨", "UPDATED": "🔺", "CLEARED": "✅"}.get(op, "ℹ️")
    asset_lbl  = _fmt_asset(asset_type, asset_id)

    parts = [
        f"{emoji} <b>{escape(op)}</b> · <code>{escape(asset_lbl)}</code> · <code>{escape(str(code or ''))}</code>",
        f"<b>{escape(severity)}</b>" if severity else "",
        escape(message) if message else "",
        f"<i>threshold:</i> {escape(str(threshold))}" if threshold else "",
        f"<i>id:</i> {escape(str(alarm_id))}" if alarm_id else "",
    ]
    return "\n".join(p for p in parts if p)

def _should_send(payload: dict, alarm: dict | None) -> bool:
    """Reglas anti-spam: una vez por RAISED, siempre CLEARED,
    UPDATED sólo si sube severidad o cambia threshold."""
    op = (payload.get("op") or "").upper()
    alarm_id = payload.get("alarm_id")
    if not alarm_id:
        return True  # si no viene ID no podemos dedupe: mejor enviar

    sev = (payload.get("severity") or (alarm and alarm.get("severity","")) or "").upper()
    th  = payload.get("threshold") or (
        alarm and isinstance(alarm.get("extra"), dict) and alarm["extra"].get("threshold")
    ) or ""

    prev = _last_sent.get(alarm_id)

    if op == "RAISED":
        # si ya mandamos RAISED para este id, no repetir
        if prev and prev.get("op") == "RAISED":
            return False
        _last_sent[alarm_id] = {"op": "RAISED", "sev": sev, "th": th, "t": monotonic()}
        return True

    if op == "UPDATED":
        # Si nunca mandamos nada de esta alarma, permitimos 1er envío
        if not prev:
            _last_sent[alarm_id] = {"op": "UPDATED", "sev": sev, "th": th, "t": monotonic()}
            return True
        # Enviar sólo si escala severidad o cambia threshold
        if _SEV_RANK.get(sev, 0) > _SEV_RANK.get(prev.get("sev",""), 0) or th != prev.get("th"):
            _last_sent[alarm_id] = {"op": "UPDATED", "sev": sev, "th": th, "t": monotonic()}
            return True
        return False

    if op == "CLEARED":
        # siempre avisamos el clear, pero no repitamos múltiples CLEARED
        if prev and prev.get("op") == "CLEARED":
            return False
        _last_sent[alarm_id] = {"op": "CLEARED", "sev": sev, "th": th, "t": monotonic()}
        return True

    # Para otros OP desconocidos, por defecto enviamos una vez
    if not prev:
        _last_sent[alarm_id] = {"op": op, "sev": sev, "th": th, "t": monotonic()}
        return True
    return False

def _handle_payload(payload: dict):
    alarm = _fetch_alarm(payload.get("alarm_id"))
    if not _should_send(payload, alarm):
        # mensaje suprimido por reglas anti-spam
        return
    text  = _html_from(payload, alarm)
    try:
        res = asyncio.run(tg_send_async(text))   # usa TU sender async (igual que ACK)
        print("[alarm-listener] tg result:", res)
    except Exception as e:
        print(f"[alarm-listener] telegram send error: {e}")

def _listen_loop(stop_evt: Event):
    dsn = os.getenv("DATABASE_URL") or os.getenv("DB_URL")
    if not dsn:
        print("[alarm-listener] ❌ no DSN: set DATABASE_URL o DB_URL")
        return

    # autocommit + application_name (visible en pg_stat_activity)
    with psycopg.connect(dsn, autocommit=True, application_name="alarm-listener") as conn:
        with conn.cursor() as cur:
            cur.execute("LISTEN alarm_events;")
        print("[alarm-listener] ✅ listening on 'alarm_events'")

        fd = conn.pgconn.socket  # file descriptor del socket de libpq

        while not stop_evt.is_set():
            try:
                # Espera hasta 1s a que haya datos en el socket
                r, _, _ = select.select([fd], [], [], 1.0)
                if r:
                    # Hay datos → consumir para que entren a psycopg
                    conn.pgconn.consume_input()

                # Drenar notificaciones pendientes (psycopg3: notifies() es iterable)
                for notify in conn.notifies():
                    try:
                        payload = json.loads(notify.payload)
                        print("[alarm-listener] recv:", payload)
                    except Exception as e:
                        print("[alarm-listener] bad payload:", notify.payload, e)
                        continue

                    _handle_payload(payload)

            except Exception as e:
                print(f"[alarm-listener] error: {e}")
                time.sleep(2)  # backoff suave y continuar

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
