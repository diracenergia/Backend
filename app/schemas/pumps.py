# app/schemas/pumps.py
from __future__ import annotations
from typing import Any, Dict, Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator

# -------------------------
# Lecturas (ingesta)
# -------------------------
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

    model_config = {"extra": "ignore", "str_strip_whitespace": True}


# -------------------------
# Configuración de bomba
# -------------------------
_ALLOWED_DRIVE_TYPES = {None, "direct", "vfd"}

class PumpConfigIn(BaseModel):
    """
    Esquema de entrada para upsert de configuración de bomba.
    Todos los campos son opcionales (solo se actualiza lo provisto).
    """
    drive_type: Optional[str] = Field(None, description="direct|vfd")
    remote_enabled: Optional[bool] = None
    vfd_min_speed_pct: Optional[float] = None
    vfd_max_speed_pct: Optional[float] = None
    vfd_default_speed_pct: Optional[float] = None

    @field_validator("drive_type", mode="before")
    @classmethod
    def _v_drive_type(cls, v):
        if v is None:
            return None
        s = str(v).strip().lower()
        if s not in _ALLOWED_DRIVE_TYPES:
            raise ValueError("drive_type debe ser 'direct' o 'vfd'")
        return s

    @field_validator("vfd_min_speed_pct", "vfd_max_speed_pct", "vfd_default_speed_pct")
    @classmethod
    def _v_pct_bounds(cls, v):
        if v is None:
            return v
        v = float(v)
        if not (0.0 <= v <= 100.0):
            raise ValueError("valores VFD deben estar entre 0 y 100")
        return v

    # Validación cruzada: min <= max, y default dentro de [min, max] si están presentes
    @field_validator("*")
    @classmethod
    def _v_cross(cls, v, info):
        # No podemos leer otros campos aquí; lo hacemos en model_post_init
        return v

    def model_post_init(self, __context: Any) -> None:
        mn = self.vfd_min_speed_pct
        mx = self.vfd_max_speed_pct
        df = self.vfd_default_speed_pct
        if mn is not None and mx is not None and mn > mx:
            raise ValueError("vfd_min_speed_pct debe ser <= vfd_max_speed_pct")
        if df is not None:
            lo = mn if mn is not None else 0.0
            hi = mx if mx is not None else 100.0
            if not (lo <= df <= hi):
                raise ValueError("vfd_default_speed_pct debe estar entre min y max")

    model_config = {"extra": "ignore", "str_strip_whitespace": True}


class PumpConfigOut(BaseModel):
    """Respuesta típica del upsert que retorna el repo/ruta."""
    pump_id: int
    drive_type: Optional[str] = None
    remote_enabled: Optional[bool] = None
    vfd_min_speed_pct: Optional[float] = None
    vfd_max_speed_pct: Optional[float] = None
    vfd_default_speed_pct: Optional[float] = None
    updated_at: Optional[datetime] = None

    model_config = {"extra": "ignore", "str_strip_whitespace": True}


__all__ = ["PumpPayload", "PumpConfigIn", "PumpConfigOut"]
