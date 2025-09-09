# app/routes/tanks.py
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from psycopg import errors as psy_errors

from app.schemas.tanks import TankOut, TankCreate, TankUpdate
from app.repos import tanks as repo
from app.core.security import device_id_dep  # auth por API key

router = APIRouter(prefix="/tanks", tags=["tanks"])


def _to_dict(model):
    if hasattr(model, "model_dump"):
        return model.model_dump(exclude_unset=True)
    return model.dict(exclude_unset=True)


@router.get("", response_model=List[TankOut])
def list_tanks(user_id: Optional[int] = None, _=Depends(device_id_dep)):
    return repo.list_tanks(user_id)


@router.get("/{tank_id}", response_model=TankOut)
def get_tank(tank_id: int, _=Depends(device_id_dep)):
    t = repo.get_tank(tank_id)
    if not t:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tank not found")
    return t


@router.post("", response_model=TankOut, status_code=status.HTTP_201_CREATED)
def create_tank(payload: TankCreate, _=Depends(device_id_dep)):
    data = _to_dict(payload)
    try:
        return repo.create_tank(data)
    except psy_errors.UniqueViolation:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Tank already exists")
    except psy_errors.ForeignKeyViolation:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid foreign key")
    except Exception as e:
        print(f"[tanks] create error: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="create failed")


@router.put("/{tank_id}", response_model=TankOut)
def update_tank(tank_id: int, payload: TankUpdate, _=Depends(device_id_dep)):
    data = _to_dict(payload)
    try:
        updated = repo.update_tank(tank_id, data)
        if not updated:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tank not found")
        return updated
    except psy_errors.ForeignKeyViolation:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid foreign key")
    except Exception as e:
        print(f"[tanks] update error: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="update failed")
