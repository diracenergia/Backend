# app/routes/ingest_pump_safe.py
from fastapi import APIRouter, Header, Request, status
from app.schemas.pumps import PumpPayload
from app.repos.pumps import insert_pump_reading
from datetime import datetime, timezone

router = APIRouter(prefix="/ingest", tags=["ingest"])

@router.post("/pump2", status_code=status.HTTP_201_CREATED)
def ingest_pump_safe(
    payload: PumpPayload,
    request: Request,
    x_org_id: int | None = Header(default=None, convert_underscores=False),
    x_device_id: int | None = Header(default=None, convert_underscores=False),
):
    # Lógica mínima y “a prueba de balas”: delega al repo
    reading_id = insert_pump_reading(
        device_id=x_device_id,     # puede ser None (FK permite NULL)
        payload=payload,
        org_id=x_org_id,           # setea app.org_id en la conexión (evita ''::bigint)
    )
    source_ip = request.client.host if request and request.client else "unknown"
    return {"ok": True, "reading_id": reading_id, "source_ip": source_ip}
