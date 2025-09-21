# app/routes/live_view.py
from __future__ import annotations
import asyncio, json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

viz_router = APIRouter(prefix="/viz", tags=["viz"])

# Cache en memoria (última lectura conocida)
PUMPS: Dict[int, Dict[str, Any]] = {}
TANKS: Dict[int, Dict[str, Any]] = {}

# Clientes conectados al WS
CLIENTS: Set[WebSocket] = set()
RUNNING: bool = False
TASK: Optional[asyncio.Task] = None
PERIOD = 1.0  # segundos

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# ---- APIs para que /ingest actualice el cache ----
def apply_pump_ingest(payload: Dict[str, Any]) -> None:
    """
    Llamala desde /ingest/pump con el JSON recibido.
    Campos tolerados: pump_id, is_on, flow_lpm, pressure_bar, speed_pct, ts, etc.
    """
    pid = int(payload.get("pump_id"))
    curr = PUMPS.get(pid, {"pump_id": pid})
    curr.update({
        "pump_id": pid,
        "is_on": bool(payload.get("is_on", curr.get("is_on", False))),
        "flow_lpm": float(payload.get("flow_lpm", curr.get("flow_lpm", 0.0) or 0.0)),
        "pressure_bar": float(payload.get("pressure_bar", curr.get("pressure_bar", 0.0) or 0.0)),
        "speed_pct": int(payload.get("speed_pct", curr.get("speed_pct", 0) or 0)),
        "ts": payload.get("ts") or curr.get("ts") or _now_iso(),
    })
    PUMPS[pid] = curr

def apply_tank_ingest(payload: Dict[str, Any]) -> None:
    """
    Llamala desde /ingest/tank con el JSON recibido.
    Campos tolerados: tank_id, level_pct, inflow_lpm, outflow_lpm, ts, etc.
    """
    tid = int(payload.get("tank_id"))
    curr = TANKS.get(tid, {"tank_id": tid})
    def _f(x, d=0.0):
        try: return float(x)
        except: return d
    curr.update({
        "tank_id": tid,
        "level_pct": _f(payload.get("level_pct", curr.get("level_pct", 0.0))),
        "inflow_lpm": _f(payload.get("inflow_lpm", curr.get("inflow_lpm", 0.0))),
        "outflow_lpm": _f(payload.get("outflow_lpm", curr.get("outflow_lpm", 0.0))),
        "ts": payload.get("ts") or curr.get("ts") or _now_iso(),
    })
    TANKS[tid] = curr

# ---- WebSocket y estado ----
async def _broadcast(msg: Dict[str, Any]):
    dead: List[WebSocket] = []
    data = json.dumps(msg)
    for ws in list(CLIENTS):
        try:
            await ws.send_text(data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        try: CLIENTS.discard(ws)
        except: pass

async def _loop():
    global RUNNING
    while RUNNING:
        # Emitir snapshot de lo último recibido por /ingest/*
        await _broadcast({
            "type": "snapshot",
            "pumps": list(PUMPS.values()),
            "tanks": list(TANKS.values()),
        })
        await asyncio.sleep(PERIOD)

def _ensure_loop():
    global RUNNING, TASK
    if not RUNNING:
        RUNNING = True
        TASK = asyncio.create_task(_loop())

@viz_router.websocket("/ws")
async def ws_viz(websocket: WebSocket):
    # Si tu backend exige org, manéjalo aquí (query ?org_id=...)
    await websocket.accept()
    CLIENTS.add(websocket)
    _ensure_loop()
    try:
        while True:
            await websocket.receive_text()  # mantener viva la conexión
    except WebSocketDisconnect:
        CLIENTS.discard(websocket)
    except Exception:
        CLIENTS.discard(websocket)

@viz_router.get("/state")
async def viz_state():
    return {
        "pumps": list(PUMPS.values()),
        "tanks": list(TANKS.values()),
        "count": {"pumps": len(PUMPS), "tanks": len(TANKS)},
    }
