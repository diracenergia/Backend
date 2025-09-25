# app/routes/ingest_pump.py
from __future__ import annotations
from typing import Any, Optional
from datetime import datetime, timezone
import logging

from fastapi import APIRouter, Header, Request, status, HTTPException
from psycopg.rows import dict_row

from app.schemas.pumps import PumpPayload
from app.repos.pumps import insert_pump_reading

# Visor en vivo (best-effort)
try:
    from app.routes.live_view import apply_pump_ingest  # actualiza cache para /viz/ws y /viz/state
except Exception:
    def apply_pump_ingest(_: dict[str, Any]) -> None:
        pass

log = logging.getLogger("rdls.ingest.pump")
router = APIRouter(prefix="/ingest", tags=["ingest"])

@router.post("/pump", status_code=status.HTTP_201_CREATED)
def ingest_pump(
    payload: PumpPayload,
    request: Request,
    x_org_id: Optional[int] = Header(default=None, convert_underscores=False),
    x_device_id: Optional[int] = Header(default=None, convert_underscores=False),
):
    """
    Inserta una lectura de bomba y publica al visor (si está disponible).
    Headers esperados: X-Org-Id, X-Device-Id (opcionales).
    Body: PumpPayload.
    Respuesta: {"ok": true, "reading_id": <int>, "source_ip": "..."}.
    """
    try:
        reading_id = insert_pump_reading(
            device_id=x_device_id,
            payload=payload,
            org_id=x_org_id,   # 👈 acá se setea app.org_id en la conexión; evita ''::bigint
        )
    except HTTPException:
        raise
    except Exception as exc:
        log.exception("DB insert failed", exc_info=exc)
        raise HTTPException(status_code=500, detail="db_error: no se pudo insertar la lectura de bomba") from exc

    # Publicación best-effort al live view
    try:
        data = payload.model_dump(exclude_none=True)
        data.setdefault("ts", datetime.now(timezone.utc).isoformat())
        apply_pump_ingest({
            "pump_id": data.get("pump_id"),
            "is_on": data.get("is_on"),
            "flow_lpm": data.get("flow_lpm"),
            "pressure_bar": data.get("pressure_bar"),
            "speed_pct": data.get("speed_pct"),
            "ts": data.get("ts"),
        })
    except Exception as e:
        log.warning("[viz] apply_pump_ingest failed: %s", e)

    src_ip = request.client.host if request and request.client else "unknown"
    return {"ok": True, "reading_id": reading_id, "source_ip": src_ip}

@router.get("/pump/ping", status_code=status.HTTP_200_OK)
def pump_ping():
    return {"ok": True, "service": "ingest_pump"}
