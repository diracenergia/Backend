import logging, traceback
from fastapi import APIRouter, Header, HTTPException, Request
from typing import Optional

from app.schemas.ingest import TankIngestIn, TankIngestOut
# importa tu repositorio real (ajusta este import a tu proyecto)
from app.core import repo

log = logging.getLogger("rdls.ingest")
router = APIRouter()

@router.post("/ingest/tank", response_model=TankIngestOut)
async def ingest_tank(
    reading: TankIngestIn,
    request: Request,
    x_org_id: Optional[str] = Header(None),
    x_device_id: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None),
):
    # Normalizaciones suaves
    org_id_norm: Optional[int] = None
    try:
        org_id_norm = int(x_org_id) if x_org_id not in (None, "", "null") else None
    except Exception:
        # si llega basura en el header, lo dejamos como None y que tenancy/DB lo trate
        pass

    # `device_id` puede venir en body (int/str) o header; priorizamos body
    dev_id = reading.device_id if reading.device_id not in (None, "") else x_device_id
    if isinstance(dev_id, int):
        dev_id = str(dev_id)  # Out espera str; y evitamos FK con tipos distintos

    log.debug(
        "[ingest_tank] hdr org=%s dev=%s api=%s body=%s",
        org_id_norm, dev_id, bool(x_api_key), reading.model_dump()
    )

    try:
        # --- VALIDACIONES EXPLÍCITAS PARA DEVOLVER 400 (mejor que 500 genérico) ---
        if not repo.tank_exists(reading.tank_id, org_id=org_id_norm):
            log.warning("[ingest_tank] tank FK fail tank=%s org=%s", reading.tank_id, org_id_norm)
            raise HTTPException(status_code=400, detail="invalid tank_id (fk)")

        if dev_id:
            if not repo.device_exists(dev_id):
                log.warning("[ingest_tank] device FK fail device=%s", dev_id)
                raise HTTPException(status_code=400, detail="invalid device_id (fk)")

        # --- INSERT ---
        saved = repo.insert_tank_reading(
            org_id=org_id_norm,
            tank_id=reading.tank_id,
            device_id=dev_id,
            level_percent=reading.level_percent,
            ts=reading.ts,
            volume_l=reading.volume_l,
            temperature_c=reading.temperature_c,
            raw_json=reading.raw_json,
        )
        log.info("[ingest_tank] OK id=%s tank=%s org=%s", getattr(saved, "id", None), reading.tank_id, org_id_norm)

        # --- ALARMAS (no debe romper la ingesta) ---
        try:
            from app.services import alarms_eval
            alarms_eval.evaluate_for_tank(reading.tank_id, org_id=org_id_norm)
        except Exception:
            log.exception("[ingest_tank] alarm-eval failed tank=%s org=%s", reading.tank_id, org_id_norm)

        return saved

    except HTTPException:
        # status/detail ya seteados
        raise
    except Exception as e:
        # traceback completo para cazar el 500 exacto
        log.error("[ingest_tank] failed: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail="ingest failed")
