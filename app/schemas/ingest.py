import logging, traceback
from fastapi import APIRouter, Header, HTTPException, Request
from app.schemas.ingest import TankIngestIn, TankIngestOut
# importa lo que uses de repo/eval
log = logging.getLogger("rdls.ingest")

router = APIRouter()

@router.post("/ingest/tank", response_model=TankIngestOut)
async def ingest_tank(
    reading: TankIngestIn,
    request: Request,
    x_org_id: str | None = Header(None),
    x_device_id: str | None = Header(None),
    x_api_key: str | None = Header(None),
):
    log.debug(f"[ingest_tank] hdr org={x_org_id} dev={x_device_id} api={bool(x_api_key)} body={reading.model_dump()}")

    try:
        # VALIDACIONES FK (opcionales pero aclaran errores → 400 en vez de 500)
        if not repo.tank_exists(reading.tank_id, x_org_id):
            log.warning(f"[ingest_tank] tank FK fail tank={reading.tank_id} org={x_org_id}")
            raise HTTPException(status_code=400, detail="invalid tank_id (fk)")

        dev_id = reading.device_id or (x_device_id and str(x_device_id))
        if dev_id and not repo.device_exists(dev_id):
            log.warning(f"[ingest_tank] device FK fail device={dev_id}")
            raise HTTPException(status_code=400, detail="invalid device_id (fk)")

        saved = repo.insert_tank_reading(
            org_id=x_org_id,
            tank_id=reading.tank_id,
            device_id=dev_id,
            level_percent=reading.level_percent,
            ts=reading.ts,
            volume_l=reading.volume_l,
            temperature_c=reading.temperature_c,
            raw_json=reading.raw_json,
        )
        log.info(f"[ingest_tank] OK id={saved.id} tank={reading.tank_id} org={x_org_id}")

        # EVALUACIÓN DE ALARMAS (no debe romper la ingesta)
        try:
            from app.services import alarms_eval
            alarms_eval.evaluate_for_tank(reading.tank_id, org_id=x_org_id)
        except Exception:
            log.exception(f"[ingest_tank] alarm-eval failed tank={reading.tank_id} org={x_org_id}")

        return saved

    except HTTPException:
        # ya seteamos status y detail
        raise
    except Exception as e:
        log.error(f"[ingest_tank] failed: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="ingest failed")
