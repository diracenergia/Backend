# app/schemas.py
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, conint, confloat

# =========================
# Tanks - Config
# =========================

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

    # Umbrales de configuración
    low_pct: Optional[float] = None
    low_low_pct: Optional[float] = None
    high_pct: Optional[float] = None
    high_high_pct: Optional[float] = None
    updated_by: Optional[str] = None
    updated_at: Optional[str] = None  # o datetime

    # Campos runtime (desde v_tanks_with_config)
    level_pct: Optional[float] = None
    age_sec: Optional[int] = None
    online: Optional[bool] = None


# =========================
# Pumps - Config
# =========================

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


# =========================
# Tanks - Ingesta de lecturas
# =========================

class TankIngestIn(BaseModel):
    tank_id: conint(gt=0)
    level_pct: confloat(ge=0, le=100)
    created_at: Optional[datetime] = None  # si no se envía, el backend usa NOW()

class TankIngestOut(BaseModel):
    id: int
    tank_id: int
    level_pct: float
    created_at: datetime
