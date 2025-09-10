# app/routes/debug_alarm.py
from __future__ import annotations

import os
import json
import time
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Body
from psycopg import conninfo

# Usamos la conexión de EVENTOS (autocommit=True) para publicar en el canal
from app.core.db import get_events_conn as get_conn

router = APIRouter()


# -----------------------------
# Helpers
# -----------------------------
def _mask_user(u: Optional[str]) -> Optional[str]:
    if not u:
        return u
    if len(u) <= 2:
        return "*" * len(u)
    return u[0] + "***" + u[-1]


def _now_iso() -> str:
    # ISO con TZ UTC; para compatibilidad, dejemos sufijo 'Z'
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# -----------------------------
# Diagnóstico de DSNs y estado
# -----------------------------
@router.get("/__alarm_diag")
def alarm_diag() -> dict[str, Any]:
    out: dict[str, Any] = {}

    # --- estado del listener (thread en memoria)
    try:
        import app.services.alarm_listener as al  # type: ignore

        thr = getattr(al, "_thread", None)
        out["listener"] = {
            "import_ok": True,
            "thread_alive": bool(thr and thr.is_alive()),
        }
    except Exception as e:
        out["listener"] = {"import_ok": False, "error": repr(e)}

    # --- DSN elegido para el listener (ENV con prioridad)
    dsn_env = (
        os.getenv("EVENTS_DB_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("DB_URL")
        or ""
    )
    d: dict[str, Any] = {}
    src = (
        "EVENTS_DB_URL"
        if os.getenv("EVENTS_DB_URL")
        else "DATABASE_URL"
        if os.getenv("DATABASE_URL")
        else "DB_URL"
        if os.getenv("DB_URL")
        else None
    )
    try:
        if dsn_env:
            d = conninfo.conninfo_to_dict(dsn_env)
    except Exception:
        pass
    out["listener_env_dsn"] = {
        "present": bool(dsn_env),
        "source": src,
        "host": d.get("host"),
        "port": d.get("port"),
        "dbname": d.get("dbname"),
        "user": _mask_user(d.get("user")),
    }

    # --- DSN real al que conecta este endpoint (misma conexión que el listener)
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT current_database(), current_user,
                       inet_server_addr()::text, inet_server_port()
                """
            )
            db, usr, host, port = cur.fetchone()
        out["app_db"] = {
            "host": host,
            "port": str(port),
            "dbname": db,
            "user": _mask_user(usr),
        }
    except Exception as e:
        out["app_db"] = {"error": repr(e)}

    # --- comparación rápida dominio/IP (solo informativa)
    try:
        same = (
            out.get("listener_env_dsn", {}).get("host")
            == out.get("app_db", {}).get("host")
            and out.get("listener_env_dsn", {}).get("dbname")
            == out.get("app_db", {}).get("dbname")
        )
        out["same_db_host"] = same
    except Exception:
        out["same_db_host"] = None

    return out


# -----------------------------
# Control del listener
# -----------------------------
@router.post("/__alarm_start")
def alarm_start() -> dict[str, Any]:
    try:
        from app.services.alarm_listener import start_alarm_listener  # type: ignore

        start_alarm_listener()
        return {"ok": True, "msg": "listener started"}
    except Exception as e:
        return {"ok": False, "error": repr(e)}


@router.post("/__alarm_stop")
def alarm_stop() -> dict[str, Any]:
    try:
        from app.services.alarm_listener import stop_alarm_listener  # type: ignore

        stop_alarm_listener()
        return {"ok": True, "msg": "listener stopped"}
    except Exception as e:
        return {"ok": False, "error": repr(e)}


@router.get("/__alarm_listener_status")
def alarm_listener_status() -> dict[str, Any]:
    try:
        import app.services.alarm_listener as al  # type: ignore

        thr = getattr(al, "_thread", None)
        alive = bool(thr and thr.is_alive())
        channel = getattr(al, "CHANNEL", "alarm_events")
        sent_cache = getattr(al, "_sent_cache", None)
        size = len(sent_cache) if isinstance(sent_cache, list) else 0
        return {"alive": alive, "channel": channel, "sent_cache": size}
    except Exception as e:
        return {"alive": False, "error": repr(e)}


# -----------------------------
# Pings / Publicación por canal
# -----------------------------
@router.get("/__alarm_notify_ping")
def alarm_notify_ping() -> dict[str, Any]:
    """
    PING de diagnóstico: publica un NOTIFY con op=PING (el listener normalmente lo ignora).
    Sirve para confirmar conectividad/DSN.
    """
    ping = {
        "op": "PING",
        "asset_type": "test",
        "asset_id": 0,
        "code": "PING",
        "alarm_id": int(time.time() * 1000),
        "severity": "INFO",
        "threshold": "n/a",
        "ts": _now_iso(),
        "message": "diagnostic ping",
    }
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_notify('alarm_events', %s)", (json.dumps(ping),))
    return {"ok": True, "sent": ping}


@router.post("/diag/listener/ping")
def diag_listener_ping() -> dict[str, Any]:
    """
    Publica un evento RAISED 'real' en el canal de alarmas (el listener debe reenviarlo a Telegram).
    """
    payload = {
        "op": "RAISED",
        "asset_type": "tank",
        "asset_id": 1,
        "code": "LOW",
        "message": "diag ping",
        "severity": "warning",
        "value": 9.9,
        "threshold": "low",
        "ts_raised": _now_iso(),
    }
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_notify('alarm_events', %s)", (json.dumps(payload),))
    return {"ok": True, "published": payload, "at": time.time()}


@router.post("/diag/listener/publish")
def diag_listener_publish(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """
    Publica el JSON que envíes como body tal cual en el canal 'alarm_events'.
    Útil para probar CLEARED, distintos codes/severity, etc.
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_notify('alarm_events', %s)", (json.dumps(payload),))
    return {"ok": True, "published": payload, "at": time.time()}


@router.get("/diag/listener/last")
def diag_listener_last() -> dict[str, Any]:
    """
    Muestra lo último que el listener dijo haber enviado (cache en memoria).
    """
    try:
        import app.services.alarm_listener as al  # type: ignore

        thr = getattr(al, "_thread", None)
        alive = bool(thr and thr.is_alive())
        channel = getattr(al, "CHANNEL", "alarm_events")

        items = getattr(al, "_last_items", None)
        if not isinstance(items, list):
            # fallback a _sent_cache si es lo que mantiene el listener
            items = getattr(al, "_sent_cache", []) or []
        return {"alive": alive, "channel": channel, "count": len(items), "items": items}
    except Exception as e:
        return {"alive": False, "error": repr(e)}
