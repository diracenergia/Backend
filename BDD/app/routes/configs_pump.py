from fastapi import APIRouter
from app.schemas.pumps import PumpConfigIn
from app.repos import pumps as repo

router = APIRouter(tags=["config"])

@router.get("/pumps")
def list_pumps():
    return repo.list_pumps()

@router.get("/pumps/config")
def list_pumps_with_config():
    return repo.list_pumps_with_config()

@router.post("/pumps/{pump_id}/config")
def upsert_pump_config(pump_id: int, body: PumpConfigIn):
    return {"ok": True, "config": repo.upsert_pump_config(pump_id, body)}
