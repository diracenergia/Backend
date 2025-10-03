from typing import Optional
from pydantic import BaseModel
from datetime import datetime

# /infra/locations
class Location(BaseModel):
    id: int
    name: str
    address: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    active: bool
    created_at: datetime

# /tanks/config
class TankConfigOut(BaseModel):
    tank_id: int
    name: str | None = None
    low_pct: Optional[float] = None
    low_low_pct: Optional[float] = None
    high_pct: Optional[float] = None
    high_high_pct: Optional[float] = None
    updated_by: Optional[str] = None
    updated_at: Optional[datetime] = None

# /pumps/config
class PumpConfigOut(BaseModel):
    pump_id: int
    name: str | None = None
    low_pct: Optional[float] = None
    low_low_pct: Optional[float] = None
    high_pct: Optional[float] = None
    high_high_pct: Optional[float] = None
    updated_by: Optional[str] = None
    updated_at: Optional[datetime] = None

# /alarms
class Alarm(BaseModel):
    id: int
    asset_type: str
    asset_id: int
    code: str | None = None
    severity: str | None = None
    message: str | None = None
    ts_raised: datetime
    is_active: bool
    telegram: bool
    tg_notified_at: Optional[datetime] = None
