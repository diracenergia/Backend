from typing import Literal, Optional, List
from datetime import datetime
from pydantic import BaseModel

AssetType = Literal["tank", "pump", "valve", "manifold"]

class Location(BaseModel):
    id: int
    code: str
    name: str

class LocationWithStats(Location):
    assets_total: int
    tanks_count: int
    pumps_count: int
    valves_count: int
    manifolds_count: int
    alarms_active: int
    alarms_critical_active: int

class AssetItem(BaseModel):
    id: int
    name: Optional[str] = None
    code: Optional[str] = None

class AssetGroup(BaseModel):
    type: AssetType
    items: List[AssetItem]

class LocationSummary(BaseModel):
    location_id: int
    location_code: str
    location_name: str
    assets_total: int
    tanks_count: int
    pumps_count: int
    valves_count: int
    manifolds_count: int
    alarms_active: int
    alarms_critical_active: int
    pump_readings_30d: Optional[int] = None
    avg_flow_lpm_30d: Optional[float] = None
    avg_pressure_bar_30d: Optional[float] = None
    pumps_last_seen: Optional[datetime] = None
    tank_readings_30d: Optional[int] = None
    avg_level_pct_30d: Optional[float] = None
    tanks_last_seen: Optional[datetime] = None

class LocationTree(BaseModel):
    location: Location
    summary: LocationSummary
    assets: List[AssetGroup]
