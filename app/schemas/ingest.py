# app/schemas/ingest.py
from typing import Optional, Dict, Any, Annotated, Union
from pydantic import BaseModel, Field
from datetime import datetime

Pct = Annotated[float, Field(ge=0, le=100)]
NonNegFloat = Annotated[float, Field(ge=0)]
# Aceptamos letras, números y separadores típicos para device IDs
DeviceId = Annotated[str, Field(min_length=1, pattern=r"^[A-Za-z0-9._:\-]+$")]

class TankIngestIn(BaseModel):
    tank_id: Annotated[int, Field(ge=1, description="ID del tanque")]
    level_percent: Pct = Field(..., description="Nivel en %")
    ts: Optional[datetime] = Field(
        None, description="Timestamp; si no viene, la DB usa now()"
    )
    # Permitimos int o str; Pydantic lo normaliza a str en el Out si hiciera falta
    device_id: Optional[Union[DeviceId, int]] = Field(
        None, description="ID lógico del device (opcional; si no, se toma de X-Device-Id)"
    )
    volume_l: Optional[NonNegFloat] = Field(None, description="Volumen en litros (opcional)")
    temperature_c: Optional[float] = Field(None, ge=-50, le=150, description="Temperatura en °C (opcional)")
    raw_json: Optional[Dict[str, Any]] = Field(None, description="Payload bruto opcional")

    model_config = {
        "extra": "ignore",  # ignora campos desconocidos en el POST
    }

class TankIngestOut(BaseModel):
    id: int
    tank_id: int
    ts: datetime
    level_percent: float
    volume_l: Optional[NonNegFloat] = None
    temperature_c: Optional[float] = None
    device_id: Optional[str] = None  # string en la salida
    raw_json: Optional[Dict[str, Any]] = None
