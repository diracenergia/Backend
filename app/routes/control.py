# app/routes/control.py
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional, Literal

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field, conint, field_validator

# Usamos el cache del visor en vivo
from app.routes.live_view import PUMPS, TANKS, apply_pump_ingest, apply_tank_ingest

control_router = APIRouter(prefix="/control", tags=["control"])

# ----------------- Helpers -----------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _lph_to_lpm(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v) / 60.0
    except Exception:
        return None

# ----------------- Modelos -----------------
PumpCmdLit = Literal["START", "STOP", "SPEED"]
TankCmdLit = Literal["FILL", "DRAIN", "STOP", "SET_LEVEL", "SET_DRAIN_RATE", "SET_INFLOW_RATE"]

class PumpCommandIn(BaseModel):
    cmd: PumpCmdLit
    pump_id: int = Field(..., ge=1)
    user: str = "dashboard"
    speed_pct: Optional[conint(ge=0, le=100)] = None

    # Programación simple (opcional)
    start_at: Optional[datetime] = None   # programa ejecutar el cmd en horario futuro
    duration_sec: Optional[int] = Field(None, ge=1)  # si cmd=START y duration_sec>0 => auto STOP

class TankCommandIn(BaseModel):
    cmd: TankCmdLit
    tank_id: int = Field(..., ge=1)
    user: str = "dashboard"
    level_percent: Optional[float] = Field(None, ge=0.0, le=100.0)

    # Caudales
    drain_lpm: Optional[float] = Field(None, ge=0.0)  # vaciado en L/min
    drain_lph: Optional[float] = Field(None, ge=0.0)  # vaciado en L/h (se convierte)
    inflow_lpm: Optional[float] = Field(None, ge=0.0) # llenado en L/min

    # Programación simple (opcional)
    start_at: Optional[datetime] = None
    duration_sec: Optional[int] = Field(None, ge=1)

    @field_validator("level_percent")
    @classmethod
    def _clamp_level(cls, v):
        if v is None:
            return v
        return max(0.0, min(100.0, float(v)))

# ----------------- Tareas programadas (muy simples) -----------------
# Para no meter dependencias, usamos create_task con sleep
async def _run_later(delay_sec: int, coro):
    await asyncio.sleep(delay_sec)
    await coro

# ----------------- PUMP control -----------------
@control_router.post("/pump", status_code=status.HTTP_200_OK)
async def control_pump(cmd: PumpCommandIn):
    async def _apply_now():
        # Asegurar estructura en cache
        p = PUMPS.get(cmd.pump_id, {"pump_id": cmd.pump_id, "is_on": False, "speed_pct": 0, "flow_lpm": 0.0, "pressure_bar": 0.0, "ts": _now_iso()})
        c = cmd.cmd.upper()
        if c == "START":
            p["is_on"] = True
            if not p.get("speed_pct"):
                p["speed_pct"] = 50
        elif c == "STOP":
            p["is_on"] = False
        elif c == "SPEED":
            if cmd.speed_pct is None:
                raise HTTPException(status_code=400, detail="speed_pct es obligatorio con cmd=SPEED")
            p["speed_pct"] = int(cmd.speed_pct)
            p["is_on"] = p["is_on"] or p["speed_pct"] > 0
        else:
            raise HTTPException(status_code=400, detail="cmd inválido")
        p["ts"] = _now_iso()
        PUMPS[cmd.pump_id] = p
        # Publicar en visor
        apply_pump_ingest({
            "pump_id": p["pump_id"],
            "is_on": p["is_on"],
            "flow_lpm": p.get("flow_lpm", 0.0),
            "pressure_bar": p.get("pressure_bar", 0.0),
            "speed_pct": p.get("speed_pct", 0),
            "ts": p["ts"],
        })

    # Programación (start_at)
    if cmd.start_at and cmd.start_at > datetime.now(timezone.utc):
        delay = int((cmd.start_at - datetime.now(timezone.utc)).total_seconds())
        asyncio.create_task(_run_later(delay, _apply_now()))
    else:
        await _apply_now()

    # Auto-stop si corresponde
    if cmd.cmd.upper() == "START" and cmd.duration_sec and cmd.duration_sec > 0:
        async def _auto_stop():
            await asyncio.sleep(cmd.duration_sec)
            stop_cmd = PumpCommandIn(cmd="STOP", pump_id=cmd.pump_id)
            await control_pump(stop_cmd)
        asyncio.create_task(_auto_stop())

    return {"ok": True, "pump_id": cmd.pump_id, "applied": cmd.cmd}

# ----------------- TANK control -----------------
@control_router.post("/tank", status_code=status.HTTP_200_OK)
async def control_tank(cmd: TankCommandIn):
    async def _apply_now():
        t = TANKS.get(cmd.tank_id, {"tank_id": cmd.tank_id, "level_percent": 50.0, "inflow_lpm": 0.0, "outflow_lpm": 0.0, "ts": _now_iso()})
        c = cmd.cmd.upper()

        if c == "FILL":
            # activar entrada (llenado)
            t["inflow_lpm"] = max(float(cmd.inflow_lpm or 10.0), 0.0)
            t["outflow_lpm"] = t.get("outflow_lpm", 0.0)
        elif c == "DRAIN":
            # activar salida (vaciado); si pasás L/h lo convierto a L/min
            drain_lpm = cmd.drain_lpm
            if drain_lpm is None and cmd.drain_lph is not None:
                drain_lpm = _lph_to_lpm(cmd.drain_lph)
            t["outflow_lpm"] = max(float(drain_lpm or 10.0), 0.0)
            t["inflow_lpm"] = t.get("inflow_lpm", 0.0)
        elif c == "STOP":
            # sin entrada ni salida forzada
            t["inflow_lpm"] = 0.0
            t["outflow_lpm"] = 0.0
        elif c == "SET_LEVEL":
            if cmd.level_percent is None:
                raise HTTPException(status_code=400, detail="level_percent es obligatorio con cmd=SET_LEVEL")
            t["level_percent"] = float(cmd.level_percent)
        elif c == "SET_DRAIN_RATE":
            drain_lpm = cmd.drain_lpm
            if drain_lpm is None and cmd.drain_lph is not None:
                drain_lpm = _lph_to_lpm(cmd.drain_lph)
            if drain_lpm is None:
                raise HTTPException(status_code=400, detail="drain_lpm o drain_lph obligatorio con SET_DRAIN_RATE")
            t["outflow_lpm"] = max(float(drain_lpm), 0.0)
        elif c == "SET_INFLOW_RATE":
            if cmd.inflow_lpm is None:
                raise HTTPException(status_code=400, detail="inflow_lpm obligatorio con SET_INFLOW_RATE")
            t["inflow_lpm"] = max(float(cmd.inflow_lpm), 0.0)
        else:
            raise HTTPException(status_code=400, detail="cmd inválido")

        t["ts"] = _now_iso()
        TANKS[cmd.tank_id] = t

        # Publicar en visor
        apply_tank_ingest({
            "tank_id": t["tank_id"],
            "level_pct": t.get("level_percent"),  # visor normaliza level_percent/level_pct
            "inflow_lpm": t.get("inflow_lpm", 0.0),
            "outflow_lpm": t.get("outflow_lpm", 0.0),
            "ts": t["ts"],
        })

    # Programación (start_at)
    if cmd.start_at and cmd.start_at > datetime.now(timezone.utc):
        delay = int((cmd.start_at - datetime.now(timezone.utc)).total_seconds())
        asyncio.create_task(_run_later(delay, _apply_now()))
    else:
        await _apply_now()

    # Apagado automático tras duración (si se configuró)
    if cmd.duration_sec and cmd.duration_sec > 0:
        async def _auto_stop_tank():
            await asyncio.sleep(cmd.duration_sec)
            stop_cmd = TankCommandIn(cmd="STOP", tank_id=cmd.tank_id)
            await control_tank(stop_cmd)
        asyncio.create_task(_auto_stop_tank())

    return {"ok": True, "tank_id": cmd.tank_id, "applied": cmd.cmd}
