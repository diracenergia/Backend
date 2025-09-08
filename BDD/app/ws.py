# app/ws.py
from __future__ import annotations

import os
import json
import asyncio, time  # ⬅️ NUEVO
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, parse_qs

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()

PRESENCE_TTL_SEC = int(os.getenv("TELEMETRY_TTL_SECONDS", "30"))
REQUIRE_API_KEY = os.getenv("TELEMETRY_REQUIRE_API_KEY", "0") in ("1", "true", "True")

_ALLOWED = set(
    k.strip() for k in os.getenv("TELEMETRY_ALLOWED_KEYS", "").split(",") if k.strip()
)
_SINGLE = os.getenv("TELEMETRY_API_KEY", "").strip()
if _SINGLE:
    _ALLOWED.add(_SINGLE)

presence: Dict[str, Dict[str, Any]] = {}

def _now() -> datetime:
    return datetime.now(timezone.utc)

def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if isinstance(dt, datetime) else None

def _extract_api_key_and_device(ws: WebSocket) -> tuple[str, str]:
    q = parse_qs(urlparse(str(ws.url)).query)
    qs_key = (q.get("api_key", [""])[0] or "").strip()
    qs_dev = (q.get("device_id", [""])[0] or "").strip()

    h_auth = ws.headers.get("authorization", "")
    h_xkey = ws.headers.get("x-api-key", "")
    h_dev  = ws.headers.get("x-device-id", "")

    header_key = ""
    if h_auth and h_auth.lower().startswith("bearer "):
        header_key = h_auth.split(" ", 1)[1].strip()
    elif h_xkey:
        header_key = h_xkey.strip()

    api_key = qs_key or header_key or ""
    device  = qs_dev or h_dev or ""
    return api_key, device

def _is_valid_api_key(key: str) -> bool:
    if not REQUIRE_API_KEY:
        return True
    if not key:
        return False
    if _ALLOWED:
        return key in _ALLOWED
    return bool(key)

def _effective_online(info: Dict[str, Any]) -> bool:
    if not info:
        return False
    last_seen: datetime = info.get("last_seen")
    online_flag: bool = bool(info.get("online"))
    if not isinstance(last_seen, datetime):
        return False
    return online_flag and (_now() - last_seen <= timedelta(seconds=PRESENCE_TTL_SEC))

def presence_snapshot(device_id: str) -> Optional[Dict[str, Any]]:
    info = presence.get(device_id)
    if not info:
        return None
    return {
        "online": _effective_online(info),
        "last_seen": _iso(info.get("last_seen")),
    }

# ⬇️ NUEVO: keepalive opcional del servidor
async def _server_keepalive(ws: WebSocket, device_id: str, period_sec: int = 15):
    while True:
        await asyncio.sleep(period_sec)
        try:
            await ws.send_json({
                "type": "status",
                "payload": {"online": True, "device_id": device_id},
                "ts": int(time.time() * 1000),
            })
        except Exception:
            break

@router.websocket("/ws/telemetry")
async def ws_telemetry(ws: WebSocket):
    await ws.accept()
    api_key, device_id = _extract_api_key_and_device(ws)

    if not _is_valid_api_key(api_key):
        await ws.close(code=4401)
        return

    if not device_id:
        await ws.close(code=4400, reason="device_id required")
        return

    presence[device_id] = {"online": True, "last_seen": _now()}

    # status inicial (lo tuyo ya estaba bien)
    try:
        await ws.send_json({"type": "status", "payload": {"online": True, "device_id": device_id}})
    except RuntimeError:
        presence[device_id] = {"online": False, "last_seen": _now()}
        return

    # ⬇️ NUEVO: keepalive del server (opcional)
    ka_task = asyncio.create_task(_server_keepalive(ws, device_id, period_sec=15))

    try:
        while True:
            msg = await ws.receive_text()

            # actualizo last_seen siempre que llega algo
            presence[device_id]["last_seen"] = _now()

            # intento parsear; si no es JSON, igual sirve como actividad
            try:
                obj = json.loads(msg)
            except Exception:
                obj = {"type": "message", "raw": msg}

            t = (obj.get("type") or "").lower()

            # ⬇️ NUEVO: eco de beats/hello/status para que el front vea “actividad”
            if t in ("beat", "hello", "heartbeat", "status"):
                await ws.send_json({
                    "type": "heartbeat",
                    "device_id": device_id,
                    "ts": int(time.time() * 1000),
                })
            else:
                # opcional: ACK genérico (útil para debug)
                await ws.send_json({
                    "type": "ack",
                    "device_id": device_id,
                    "ts": int(time.time() * 1000),
                })

    except WebSocketDisconnect:
        presence[device_id] = {"online": False, "last_seen": _now()}
    finally:
        try:
            ka_task.cancel()
        except Exception:
            pass
