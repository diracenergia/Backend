# app/schemas/ingest.py
from __future__ import annotations
from typing import Optional, Any, Dict
from datetime import datetime
from pydantic import BaseModel, Field, field_validator

# ---------- IN ----------
class TankIngestIn(BaseModel):
    tank_id: int = Field(..., ge=1)
    level_percent: float = Field(..., ge=0.0, le=100.0)
    ts: Optional[datetime] = None           # ISO 8601 opcional
    device_id: Optional[str] = None         # si viene, lo usamos como fallback
    volume_l: Optional[float] = None
    temperature_c: Optional[float] = None
    raw_json: Optional[Dict[str, Any]] = None

    @field_validator("device_id")
    @classmethod
    def _dev_as_str(cls, v):
        if v is None:
            return v
        return str(v).strip()

# ---------- OUT ----------
class TankIngestOut(BaseModel):
    id: Optional[int] = None
    tank_id: int
    device_id: Optional[str] = None
    ts: Optional[datetime] = None
    level_percent: float
    volume_l: Optional[float] = None
    temperature_c: Optional[float] = None
    raw_json: Optional[Dict[str, Any]] = None
    ok: bool = True
