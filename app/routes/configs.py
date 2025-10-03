# app/routes/configs.py
from fastapi import APIRouter, Depends, Path, Body
from datetime import datetime
from app.core.security import device_id_dep
from app.repos import tanks as repo
from app.schemas.configs import TankConfigIn, TankConfigOut

router = APIRouter(prefix="/tanks", tags=["config"])

# 1) LISTA TODAS LAS CONFIGS (usa la vista v_tanks_with_config)
@router.get("/config")
def list_configs(_=Depends(device_id_dep)):
    return repo.list_tanks_with_config()

# 2) LEE UNA CONFIG
@router.get("/{tank_id}/config", response_model=TankConfigOut)
def get_config(tank_id: int = Path(..., ge=1), _=Depends(device_id_dep)):
    cfg = repo.get_tank_config(tank_id)
    if not cfg:
        return {
            "tank_id": tank_id,
            "low_pct": None,
            "low_low_pct": None,
            "high_pct": None,
            "high_high_pct": None,
            "updated_by": None,
            "updated_at": datetime.utcnow(),  # campo requerido en el schema
        }
    return cfg

# 3) UPSERT
@router.put("/{tank_id}/config", response_model=TankConfigOut)
def upsert_config(tank_id: int, payload: TankConfigIn = Body(...), _=Depends(device_id_dep)):
    return repo.upsert_tank_config(
        tank_id,
        low_pct=payload.low_pct,
        low_low_pct=payload.low_low_pct,
        high_pct=payload.high_pct,
        high_high_pct=payload.high_high_pct,
        updated_by=payload.updated_by,
    )
