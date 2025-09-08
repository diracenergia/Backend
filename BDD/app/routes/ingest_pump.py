from fastapi import APIRouter, Depends
from app.core.security import device_id_dep
from app.schemas.pumps import PumpPayload
from app.repos import pumps as repo

router = APIRouter(prefix="/ingest", tags=["ingest"])

@router.post("/pump", status_code=201)
def ingest_pump(payload: PumpPayload, device_id: int = Depends(device_id_dep)):
    new_id = repo.insert_pump_reading(device_id, payload)
    return {"ok": True, "reading_id": new_id}
