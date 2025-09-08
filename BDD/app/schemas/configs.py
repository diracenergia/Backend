# app/schemas/configs.py
from typing import Optional
from pydantic import BaseModel, Field, model_validator
from datetime import datetime

class TankConfigIn(BaseModel):
    # Validamos rango 0..100 con Pydantic (v2) usando ge/le
    low_pct: Optional[float] = Field(None, ge=0, le=100, description="Umbral bajo (%)")
    low_low_pct: Optional[float] = Field(None, ge=0, le=100, description="Umbral bajo-bajo (%)")
    high_pct: Optional[float] = Field(None, ge=0, le=100, description="Umbral alto (%)")
    high_high_pct: Optional[float] = Field(None, ge=0, le=100, description="Umbral alto-alto (%)")
    updated_by: Optional[int] = Field(None, description="Usuario que actualiza")

    @model_validator(mode="after")
    def check_order(self):
        lo2, lo, hi, hi2 = self.low_low_pct, self.low_pct, self.high_pct, self.high_high_pct
        # Si vienen todos, validamos el orden lógico: low_low <= low < high <= high_high
        if all(v is not None for v in (lo2, lo, hi, hi2)):
            if not (lo2 <= lo < hi <= hi2):
                raise ValueError("Orden inválido: low_low <= low < high <= high_high")
        return self

class TankConfigOut(BaseModel):
    tank_id: int
    low_pct: Optional[float] = None
    low_low_pct: Optional[float] = None
    high_pct: Optional[float] = None
    high_high_pct: Optional[float] = None
    updated_by: Optional[int] = None
    updated_at: datetime
