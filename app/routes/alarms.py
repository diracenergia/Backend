# app/routes/alarms.py
from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from psycopg import OperationalError
from psycopg import errors as psy_errors

from app.core.security import device_id_dep
from app.core.tenancy import require_org
from app.repos import alarms as repo  # asegúrate de tener app/repos/alarms.py

router = APIRouter(prefix="/alarms", tags=["alarms"])

@router.get("")
def list_alarms(
    active: bool = Query(default=True),
    _=Depends(device_id_dep),  # como en tanks: auth por device/api-key si aplica
):
    """
    Lista alarmas activas/inactivas para la organización actual.
    Debe ser tolerante a fallas de DB: ante OperationalError => [].
    """
    org_id = require_org()
    try:
        # Si tu repo requiere org_id, pasalo explícito
        return repo.list_alarms(org_id=org_id, active=active)
    except (OperationalError, psy_errors.AdminShutdown):
        # No 500: devolvé vacío para no frenar el front
        return []
    except Exception:
        # Último paraguas para que nunca rompa el boot
        return []
