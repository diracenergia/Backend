# app/schemas/tanks.py
from typing import Optional, Dict, Any, Literal, List
from pydantic import BaseModel, Field
from datetime import datetime

# -----------------------
# Tanques (metadatos)
# -----------------------
class TankBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=64)
    location_text: Optional[str] = Field(None, max_length=255)
    material: Optional[str] = Field(None, description="tipo ENUM tank_material en DB")
    fluid: Optional[str] = None
    install_year: Optional[int] = Field(None, ge=1900, le=2100)
    capacity_m3: Optional[float] = Field(None, ge=0)
    height_m: Optional[float] = Field(None, ge=0)
    diameter_m: Optional[float] = Field(None, ge=0)

class TankCreate(TankBase):
    user_id: Optional[int] = None

class TankUpdate(TankBase):
    pass

class TankOut(TankBase):
    id: int
    user_id: Optional[int] = None
    created_at: datetime

# -----------------------
# Comandos a tanques (tabla: public.tank_commands)
# -----------------------
# Valores válidos según CHECK:
#   cmd IN ('SET_VALVE','SET_LEAK','SET_NOISE','SET_TANK_LEVEL','SCENARIO','SET_PERIODS')
#   status IN ('queued','sent','acked','failed','expired')

CmdLiteral = Literal["SET_VALVE","SET_LEAK","SET_NOISE","SET_TANK_LEVEL","SCENARIO","SET_PERIODS"]
StatusLiteral = Literal["queued","sent","acked","failed","expired"]

class TankCommandIn(BaseModel):
    cmd: CmdLiteral = Field(..., description="Tipo de comando")
    payload: Optional[Dict[str, Any]] = Field(None, description="JSON opcional con parámetros")
    requested_by: str = Field(..., min_length=1, max_length=64, description="Usuario o sistema que solicita")

class TankCommandOut(BaseModel):
    id: int
    tank_id: int
    cmd: CmdLiteral
    payload: Optional[Dict[str, Any]] = None
    requested_by: str
    ts_created: datetime
    ts_sent: Optional[datetime] = None
    ts_acked: Optional[datetime] = None
    status: StatusLiteral
    error: Optional[str] = None

# -----------------------
# Status de tanques (desde alarmas)
# -----------------------
TankStatusLiteral = Literal["disconnected", "warning", "critical", "ok"]

class TankStatusOut(BaseModel):
    tank_id: int
    tank_name: str
    ts: Optional[datetime] = None
    level_percent: Optional[float] = Field(None, ge=0, le=100)
    has_data: bool
    status: TankStatusLiteral
    color_hex: str = Field(..., description="Color sugerido para pintar el tanque (e.g. #9CA3AF)")

# Si querés tipar la lista para el endpoint de listado:
# TankStatusList = List[TankStatusOut]
