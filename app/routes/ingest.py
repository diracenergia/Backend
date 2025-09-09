# app/routes/ingest.py
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from psycopg import errors as psy_errors

from app.schemas.ingest import TankIngestIn, TankIngestOut
from app.repos import tanks as repo
from app.core.security import device_id_dep
from app.services.alarms_eval import eval_tank_alarm  # debe existir

router = APIRouter(prefix="/ingest", tags=["ingest"])


def _get_level_percent(saved: Any) -> Optional[float]:
    if saved is None:
        return None
    if isinstance(saved, dict):
        return saved.get("level_percent")
    if hasattr(saved, "model_dump"):
        return saved.model_dump().get("level_percent")
    return getattr(saved, "level_percent", None)


@router.post("/tank", response_model=TankIngestOut, status_code=status.HTTP_201_CREATED)
def ingest_tank(payload: TankIngestIn, auth=Depends(device_id_dep)):
    """
    - Prioriza el device_id resuelto por API Key; si no hay, usa el del payload.
    - Inserta (tank_id, level_percent, volume_l, temperature_c, raw_json, device_id).
    - Mapea errores de DB a 400 (FK/Checks) o 500 (otros).
    - Evalúa alarmas en best-effort (no bloquea la respuesta).
    """
    # 1) Elegir device_id (preferimos el autenticado para evitar spoof)
    dev_from_auth = (auth or {}).get("device_id")
    dev_from_payload = getattr(payload, "device_id", None)
    device_id_db = dev_from_auth or dev_from_payload
    if device_id_db is not None:
        device_id_db = str(device_id_db)  # columna es TEXT

    # 2) Extras opcionales
    volume_l = getattr(payload, "volume_l", None)
    temperature_c = getattr(payload, "temperature_c", None)
    raw_json = getattr(payload, "extra", None)

    # 3) Insert con manejo de errores fino
    try:
        saved = repo.insert_tank_reading(
            tank_id=payload.tank_id,
            level_percent=payload.level_percent,
            ts=None,  # NOW() en DB
            device_id=device_id_db,
            volume_l=volume_l,
            temperature_c=temperature_c,
            raw_json=raw_json,
        )
    except psy_errors.ForeignKeyViolation:
        # p.ej. tank_id no existe
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="invalid tank_id or device_id (foreign key)",
        )
    except psy_errors.CheckViolation as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"payload violates constraint: {e}",
        )
    except Exception as e:
        # Log + 500 (no 503 genérico)
        print(f"[ingest/tank] DB insert failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ingest failed",
        )

    # 4) Evaluación de alarmas (best-effort)
    try:
        lvl = _get_level_percent(saved)
        print(f"[ingest] eval_tank_alarm tank={payload.tank_id} lvl={lvl}")
        eval_tank_alarm(payload.tank_id, lvl)
    except Exception as e:
        print(f"[WARN] alarm eval failed: {e}")

    return saved
