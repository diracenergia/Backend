# app/routes/tanks.py
from fastapi import APIRouter, Depends, HTTPException
from app.schemas.tanks import TankOut, TankCreate, TankUpdate
from app.repos import tanks as repo
from app.core.security import device_id_dep  # o la dep que uses para auth
from typing import List, Optional

router = APIRouter(prefix="/tanks", tags=["tanks"])

@router.get("", response_model=List[TankOut])
async def list_tanks(user_id: Optional[int]=None, _=Depends(device_id_dep)):
    return await repo.list_tanks(user_id)

@router.get("/{tank_id}", response_model=TankOut)
async def get_tank(tank_id: int, _=Depends(device_id_dep)):
    t = await repo.get_tank(tank_id)
    if not t:
        raise HTTPException(404, "Tank not found")
    return t

@router.post("", response_model=TankOut, status_code=201)
async def create_tank(payload: TankCreate, _=Depends(device_id_dep)):
    return await repo.create_tank(payload.dict(exclude_unset=True))

@router.put("/{tank_id}", response_model=TankOut)
async def update_tank(tank_id: int, payload: TankUpdate, _=Depends(device_id_dep)):
    return await repo.update_tank(tank_id, payload.dict(exclude_unset=True))
