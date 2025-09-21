# app/routes/ingest_pump.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Literal, Any, Dict

from fastapi import APIRouter, Body, status, HTTPException
from pydantic import BaseModel, Field, conint

# Importá las funciones del visor en vivo
# (asegurate de tener app/routes/live_view.py con apply_pump_ingest)
try:
    from app.routes.live_view import apply_pump_ingest
except Exception:
    # Fallback por si todavía no lo creaste; evita romper import
    def apply_pump_ingest(_: Dict[str, Any]) -> None:
        pass

router = APIRouter(prefix="/ingest", tags=["ingest"])

# --------------------- Modelos ---------------------

CmdLit = Literal["START", "STOP", "AUTO", "MAN", "SPEED"]

class PumpPayload(BaseModel):
    pump_id: int
    is_on: Optional[bool] = None
    flow_lpm: Optional[float] = None
    pressure_bar: Optional[float] = None
    voltage_v: Optional[float] = None
    current_a: Optional[float] = None
    control_mode: Literal["auto", "manual"] | None = None
    manual_lockout: Optional[bool] = None
    # agregado: muchas simulaciones envían velocidad
    speed_pct: conint(ge=0, le=100) | None = None
    extra: Optional[dict] = None
    ts: Optional[datetime] = None

class PumpConfigIn(BaseModel):
    remote_enabled: bool | None = None
    drive_type: Literal["direct", "soft", "vfd"] | None = None
    vfd_min_speed_pct: conint(ge=0, le=100) | None = None
    vfd_max_speed_pct: conint(ge=0, le=100) | None = None
    vfd_default_speed_pct: conint(ge=0, le=100) | None = None

class PumpCommandIn(BaseModel):
    cmd: CmdLit
    user: str = Field(..., description="Quién disparó el comando")
    speed_pct: conint(ge=0, le=100) | None = None  # solo SPEED

    class Config:
        extra = "ignore"

    @classmethod
    def __get_validators__(cls):
        yield cls._normalize_cmd

    @staticmethod
    def _normalize_cmd(values):
        v = values.get("cmd")
        if isinstance(v, str):
            v = v.strip()
            if v.upper().startswith("CMD_"):
                v = v[4:]
            values["cmd"] = v.upper()
        return values

# --------------------- Utils ---------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)

# --------------------- Endpoints ---------------------

@router.post("/pump", status_code=status.HTTP_201_CREATED)
async def ingest_pump(payload: PumpPayload = Body(...)):
    """
    Recibe lecturas de bomba.
    - Asegura timestamp
    - Actualiza cache del visor (para WS /viz/ws)
    - (Opcional) Persistencia en DB: ver bloque TODO
    """
    data = payload.model_dump(exclude_none=True)
    if "ts" not in data or data["ts"] is None:
        data["ts"] = _now()

    # Validaciones mínimas
    pid = data.get("pump_id")
    if pid is None:
        raise HTTPException(422, "pump_id es obligatorio")

    # Actualiza el cache del live view (para que el front dibuje tarjetas)
    try:
        apply_pump_ingest(data)
    except Exception as e:
        # No fallamos la ingesta por un problema visual
        # pero lo dejamos visible en el response para debug si querés
        return {"ok": True, "warn": f"live_view skipped: {e}", "pump": data}

    # TODO: guardar en tu base de datos/cola si corresponde
    # await db.save_pump_reading(data)

    return {"ok": True, "pump": data}
