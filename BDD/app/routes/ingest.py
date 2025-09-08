# app/routes/ingest.py
from fastapi import APIRouter, Depends, HTTPException
from app.schemas.ingest import TankIngestIn, TankIngestOut
from app.repos import tanks as repo
from app.core.security import device_id_dep
from app.services.alarms_eval import eval_tank_alarm  # <-- debe existir

router = APIRouter(prefix="/ingest", tags=["ingest"])

def _get_level_percent(saved):
    if saved is None:
        return None
    if isinstance(saved, dict):
        return saved.get("level_percent")
    if hasattr(saved, "model_dump"):
        return saved.model_dump().get("level_percent")
    return getattr(saved, "level_percent", None)

@router.post("/tank", response_model=TankIngestOut)
def ingest_tank(payload: TankIngestIn, auth=Depends(device_id_dep)):
    device_id = (payload.device_id if isinstance(payload.device_id, str) else None) or auth.get("device_id")

    try:
        saved = repo.insert_tank_reading(
            tank_id=payload.tank_id,
            level_percent=payload.level_percent,
            ts=None,  # NOW() en DB
            device_id=device_id,
            volume_l=None,
            temperature_c=None,
            raw_json=None,
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB insert failed: {e}")

    # 🔴 Ejecutar evaluación en línea para confirmar que corre
    try:
        lvl = _get_level_percent(saved)
        print(f"[ingest] eval_tank_alarm tank={payload.tank_id} lvl={lvl}")
        eval_tank_alarm(payload.tank_id, lvl)
    except Exception as e:
        # logueá, pero devolvé la lectura igual
        print(f"[WARN] alarm eval failed: {e}")

    return saved
