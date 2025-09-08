from fastapi import APIRouter, Query
from app.repos import pumps as repo

router = APIRouter(tags=["history"])

@router.get("/pumps/{pump_id}/history")
def pump_history(pump_id: int, limit: int = Query(200, ge=1, le=5000)):
    return repo.pump_history_rows(pump_id, limit)
