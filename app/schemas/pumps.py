from pydantic import BaseModel, Field, conint
from typing import Optional, Literal
from datetime import datetime

CmdLit = Literal["START", "STOP", "AUTO", "MAN", "SPEED"]

class PumpPayload(BaseModel):
    pump_id: int
    is_on: Optional[bool] = None
    flow_lpm: Optional[float] = None
    pressure_bar: Optional[float] = None
    voltage_v: Optional[float] = None
    current_a: Optional[float] = None
    control_mode: Literal["auto", "manual"] | None = None
    manual_lockout: Optional[bool] = None
    extra: Optional[dict] = None
    ts: Optional[datetime] = None

class PumpConfigIn(BaseModel):
    remote_enabled: bool | None = None
    drive_type: Literal["direct", "soft", "vfd"] | None = None
    vfd_min_speed_pct: conint(ge=0, le=100) | None = None
    vfd_max_speed_pct: conint(ge=0, le=100) | None = None
    vfd_default_speed_pct: conint(ge=0, le=100) | None = None

class PumpCommandIn(BaseModel):
    cmd: CmdLit
    user: str = Field(..., description="Quién disparó el comando")
    speed_pct: conint(ge=0, le=100) | None = None  # solo SPEED

    class Config:
        extra = "ignore"

    @classmethod
    def __get_validators__(cls):  # normaliza cmd como en tu código
        yield cls._normalize_cmd

    @staticmethod
    def _normalize_cmd(values):
        v = values.get("cmd")
        if isinstance(v, str):
            v = v.strip()
            if v.upper().startswith("CMD_"):
                v = v[4:]
            values["cmd"] = v.upper()
        return values
