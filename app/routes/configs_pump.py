# app/routes/configs_pump.py
from fastapi import APIRouter
from psycopg import OperationalError, errors as psy_errors
from app.repos import pumps as repo

router = APIRouter(tags=["config"])

@router.get("/pumps/config")
def list_pumps_with_config():
    try:
        return repo.list_pumps_with_config()
    except (OperationalError, psy_errors.AdminShutdown, psy_errors.CannotConnectNow, TimeoutError):
        return []
    except Exception:
        return []
