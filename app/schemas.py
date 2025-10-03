from typing import Optional
from pydantic import BaseModel

class TankConfigIn(BaseModel):
    low_pct: Optional[float] = None
    low_low_pct: Optional[float] = None
    high_pct: Optional[float] = None
    high_high_pct: Optional[float] = None
    updated_by: Optional[str] = None

class TankConfigOut(BaseModel):
    tank_id: int
    name: Optional[str] = None
    location_id: Optional[int] = None
    location_name: Optional[str] = None
    low_pct: Optional[float] = None
    low_low_pct: Optional[float] = None
    high_pct: Optional[float] = None
    high_high_pct: Optional[float] = None
    updated_by: Optional[str] = None
    updated_at: Optional[str] = None  # o datetime

class PumpConfigIn(BaseModel):
    low_pct: Optional[float] = None
    low_low_pct: Optional[float] = None
    high_pct: Optional[float] = None
    high_high_pct: Optional[float] = None
    updated_by: Optional[str] = None

class PumpConfigOut(BaseModel):
    pump_id: int
    name: Optional[str] = None
    location_id: Optional[int] = None
    location_name: Optional[str] = None
    low_pct: Optional[float] = None
    low_low_pct: Optional[float] = None
    high_pct: Optional[float] = None
    high_high_pct: Optional[float] = None
    updated_by: Optional[str] = None
    updated_at: Optional[str] = None
