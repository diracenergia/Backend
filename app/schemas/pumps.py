# app/schemas/pumps.py
from __future__ import annotations
from typing import Any, Dict, Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator

def _norm_mode(v: Optional[str]) -> str:
    if not v:
        return "manual"
    s = str(v).strip().lower()
    return s if s in ("manual", "auto") else "manual"

class PumpPayload(BaseModel):
    """Payload de ingest para lecturas de bomba (Pydantic v2)."""
    pump_id: int = Field(..., description="FK a public.pumps.id")

    # Estado/Control
    is_on: Optional[bool] = None
    control_mode: Optional[str] = "manual"
    manual_lockout: Optional[bool] = None

    # Métricas
    flow_lpm: Optional[float] = None
    pressure_bar: Optional[float] = None
    voltage_v: Optional[float] = None
    current_a: Optional[float] = None
    speed_pct: Optional[float] = None

    # Tiempo
    ts: Optional[datetime] = None

    # Extra (se serializea a raw_json en el repo)
    extra: Optional[Dict[str, Any]] = None

    @field_validator("control_mode", mode="before")
    @classmethod
    def _v_mode(cls, v):
        return _norm_mode(v)

    @field_validator("speed_pct")
    @classmethod
    def _v_speed_pct(cls, v):
        if v is None:
            return v
        v = float(v)
        if not (0.0 <= v <= 100.0):
            raise ValueError("speed_pct debe estar entre 0 y 100")
        return v

    # Solo v2: NADA de class Config aquí
    model_config = {
        "extra": "ignore",
        "str_strip_whitespace": True,
    }

__all__ = ["PumpPayload"]
