# app/schemas/ingest.py
from __future__ import annotations
from typing import Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field

class TankIngestIn(BaseModel):
    tank_id: int
    level_percent: float = Field(ge=0, le=100)
    ts: Optional[datetime] = None
    device_id: Optional[int] = None
    volume_l: Optional[float] = None
    temperature_c: Optional[float] = None
    inflow_lpm: Optional[float] = None
    outflow_lpm: Optional[float] = None
    raw_json: Optional[Any] = None  # dict/list/str/None

class TankIngestOut(BaseModel):
    id: Optional[int] = None
    tank_id: int
    device_id: Optional[str] = None  # en tus rutas lo devolvés como string
    ts: Optional[datetime] = None
    level_percent: float
    volume_l: Optional[float] = None
    temperature_c: Optional[float] = None
    inflow_lpm: Optional[float] = None
    outflow_lpm: Optional[float] = None
    raw_json: Optional[Any] = None
