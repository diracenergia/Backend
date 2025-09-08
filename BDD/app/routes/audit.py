# app/routes/audit.py
from fastapi import APIRouter, Query
from datetime import datetime
from app.repos import audit as repo

router = APIRouter(prefix="/audit", tags=["audit"])

@router.get("")
def audit_list(
    asset_type: str | None = Query(None),
    asset_id: int | None = Query(None),
    code: str | None = Query(None),
    state: str | None = Query(None),
    since: datetime | None = Query(None),
    until: datetime | None = Query(None),
    limit: int = Query(200, ge=1, le=5000),
):
    return repo.list_audit(asset_type, asset_id, code, state, since, until, limit)
