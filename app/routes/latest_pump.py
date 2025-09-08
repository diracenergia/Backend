from fastapi import APIRouter, HTTPException
from app.repos import pumps as repo

router = APIRouter(tags=["latest"])

@router.get("/pumps/{pump_id}/latest")
def latest_pump(pump_id: int):
    row = repo.latest_pump_row(pump_id)
    if not row:
        raise HTTPException(404, "Sin lecturas")
    return row
