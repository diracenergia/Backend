# app/routes/alarms.py
from fastapi import APIRouter, Depends
from psycopg import OperationalError
from psycopg import errors as psy_errors

router = APIRouter(prefix="/alarms", tags=["alarms"])

@router.get("")
def list_alarms(active: bool = True, _=Depends(require_org)):
    try:
        return repo.list_alarms(active=active)
    except (OperationalError, psy_errors.AdminShutdown):
        # No 500: devolvé vacío para no frenar el front
        return []
