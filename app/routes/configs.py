# app/routes/configs.py
from fastapi import APIRouter, Depends
from psycopg import OperationalError, errors as psy_errors
from app.core.security import device_id_dep
from app.repos import tanks as repo

router = APIRouter(prefix="/tanks", tags=["config"])

@router.get("/config")
def list_configs(_=Depends(device_id_dep)):
    try:
        return repo.list_tanks_with_config()
    except (OperationalError, psy_errors.AdminShutdown, psy_errors.CannotConnectNow, TimeoutError):
        return []
    except Exception:
        return []
