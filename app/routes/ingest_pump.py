# app/routes/ingest_pump.py
from fastapi import APIRouter, Header, Request, status
from app.schemas.pumps import PumpPayload
from app.repos.pumps import insert_pump_reading

router = APIRouter(prefix="/ingest", tags=["ingest"])

@router.post("/pump", status_code=status.HTTP_201_CREATED)
def ingest_pump(
    payload: PumpPayload,
    request: Request,
    x_org_id: int | None = Header(default=None, convert_underscores=False),
    x_device_id: int | None = Header(default=None, convert_underscores=False),
):
    reading_id = insert_pump_reading(
        device_id=x_device_id,
        payload=payload,
        org_id=x_org_id,  # setea app.org_id y evita ''::bigint
    )
    src_ip = request.client.host if request and request.client else "unknown"
    return {"ok": True, "reading_id": reading_id, "source_ip": src_ip}
