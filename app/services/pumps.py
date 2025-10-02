from fastapi import APIRouter, Depends, Query
from psycopg import OperationalError, errors as psy_errors
from app.core.tenancy import require_org
from app.core.security import device_id_dep
from app.repos import pumps as repo

router = APIRouter(prefix="/pumps", tags=["pumps"])

@router.get("/config")
def pumps_config(user_id: int | None = Query(default=None), _=Depends(device_id_dep)):
    org_id = require_org()
    try:
        return repo.list_pumps_config(org_id=org_id, user_id=user_id)
    except (OperationalError, psy_errors.AdminShutdown, psy_errors.CannotConnectNow, TimeoutError):
        return []
    except Exception:
        return []
