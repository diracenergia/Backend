# app/schemas/pumps.py
from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import datetime

# Compat Pydantic v1/v2
try:
    from pydantic import BaseModel, Field, field_validator
    _V2 = True
except Exception:
    from pydantic import BaseModel, Field, validator as field_validator  # type: ignore
    _V2 = False


def _norm_mode(v: Optional[str]) -> str:
    if not v:
        return "manual"
    s = str(v).strip().lower()
    return s if s in ("manual", "auto") else "manual"


class PumpPayload(BaseModel):
    """
    Payload de ingest para lecturas de bomba.
    Campos opcionales se mandan solo si tenés dato; sino van como NULL.
    """
    pump_id: int = Field(..., description="ID de la bomba (FK a public.pumps.id)")

    # Estado/Control
    is_on: Optional[bool] = Field(None, description="Estado ON/OFF reportado")
    control_mode: Optional[str] = Field("manual", description="manual|auto")
    manual_lockout: Optional[bool] = Field(None, description="Bloqueo manual activo")

    # Métricas
    flow_lpm: Optional[float] = Field(None, description="Caudal [L/min]")
    pressure_bar: Optional[float] = Field(None, description="Presión [bar]")
    voltage_v: Optional[float] = Field(None, description="Tensión [V]")
    current_a: Optional[float] = Field(None, description="Corriente [A]")
    speed_pct: Optional[float] = Field(None, description="Velocidad VFD [% 0..100]")

    # Tiempo
    ts: Optional[datetime] = Field(None, description="Timestamp ISO8601 (UTC recomendado)")

    # Extra (se guarda en raw_json en DB si el repo lo serializa)
    extra: Optional[Dict[str, Any]] = Field(None, description="Datos extras, JSON")

    # --- Validadores ---
    if _V2:
        @field_validator("control_mode", mode="before")
        @classmethod
        def _v_control_mode(cls, v):
            return _norm_mode(v)

        @field_validator("speed_pct")
        @classmethod
        def _v_speed_pct(cls, v):
            if v is None:
                return v
            if not (0.0 <= float(v) <= 100.0):
                raise ValueError("speed_pct debe estar entre 0 y 100")
            return float(v)
    else:
        @field_validator("control_mode", pre=True)  # type: ignore
        def _v1_control_mode(cls, v):
            return _norm_mode(v)

        @field_validator("speed_pct")  # type: ignore
        def _v1_speed_pct(cls, v):
            if v is None:
                return v
            v = float(v)
            if not (0.0 <= v <= 100.0):
                raise ValueError("speed_pct debe estar entre 0 y 100")
            return v

    class Config:
        # Pydantic v1
        extra = "ignore"
        anystr_strip_whitespace = True

    model_config = {
        # Pydantic v2
        "extra": "ignore",
        "str_strip_whitespace": True,
    }

__all__ = ["PumpPayload"]
