# app/routes/ingest.py
from typing import Any, Optional, Dict
import logging
import importlib
import os

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.encoders import jsonable_encoder
from psycopg import errors as psy_errors

from app.schemas.ingest import TankIngestIn, TankIngestOut
from app.repos import tanks as repo
from app.core.security import device_id_dep
from app.repos.presence import bump_presence  # presencia online/offline

log = logging.getLogger("rdls.ingest")
router = APIRouter(prefix="/ingest", tags=["ingest"])

SHOW_ERRORS = os.getenv("SHOW_ERRORS", "0") in ("1", "true", "True")

def _get_level_percent(saved: Any) -> Optional[float]:
    if saved is None:
        return None
    if isinstance(saved, dict):
        return saved.get("level_percent")
    if hasattr(saved, "model_dump"):
        return saved.model_dump().get("level_percent")
    return getattr(saved, "level_percent", None)

def _saved_as_dict(saved: Any) -> Dict[str, Any]:
    if saved is None:
        return {}
    if isinstance(saved, dict):
        return saved
    if hasattr(saved, "model_dump"):
        return saved.model_dump()
    out = {}
    for k in (
        "id", "tank_id", "device_id", "ts",
        "level_percent", "volume_l", "temperature_c", "raw_json",
    ):
        if hasattr(saved, k):
            out[k] = getattr(saved, k)
    return out

def _to_int_or_none(v) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, int):
        return v
    try:
        return int(str(v).strip())
    except Exception:
        return None

def _get_eval_fn():
    try:
        mod = importlib.import_module("app.services.alarms_eval")
        fn = getattr(mod, "eval_tank_alarm", None)
        if not callable(fn):
            raise AttributeError("eval_tank_alarm no encontrado/callable")
        ver = getattr(mod, "__VERSION__", None)
        if ver:
            log.info("alarms-eval module loaded version=%s", ver)
        return fn
    except Exception as e:
        log.exception("import eval_tank_alarm failed err=%s", e)
        return None

@router.post("/tank", response_model=TankIngestOut, status_code=status.HTTP_201_CREATED)
def ingest_tank(payload: TankIngestIn, auth=Depends(device_id_dep)):
    # 0) Log de entrada (sin exponer secrets)
    try:
        hdr_info = {
            "strict": (auth.get("strict") if isinstance(auth, dict) else getattr(auth, "strict", None)),
            "api_key_present": True if (isinstance(auth, dict) and auth.get("api_key")) else hasattr(auth, "api_key"),
            "header_device": (auth.get("device_id") if isinstance(auth, dict) else getattr(auth, "device_id", None)),
        }
    except Exception:
        hdr_info = {}
    log.debug("[ingest] headers=%s body=%s", hdr_info, payload.model_dump())

    # 1) Elegir device_id (preferimos el autenticado para evitar spoof)
    dev_from_auth = None
    try:
        if isinstance(auth, dict):
            dev_from_auth = auth.get("device_id")
        elif hasattr(auth, "device_id"):
            dev_from_auth = getattr(auth, "device_id")
        else:
            dev_from_auth = auth
    except Exception:
        dev_from_auth = None

    dev_from_payload = getattr(payload, "device_id", None)
    device_id_db = _to_int_or_none(dev_from_auth) or _to_int_or_none(dev_from_payload)

    # 2) Extras opcionales
    volume_l = getattr(payload, "volume_l", None)
    temperature_c = getattr(payload, "temperature_c", None)
    raw_json = getattr(payload, "raw_json", None)

    # 3) Insert con manejo de errores fino
    try:
        log.info(
            "[ingest/tank] INSERT params tank_id=%s lvl=%.2f ts=%s device_id=%s vol=%s temp=%s raw=%s",
            payload.tank_id, payload.level_percent, getattr(payload, "ts", None),
            device_id_db, volume_l, temperature_c, (list(raw_json.keys()) if isinstance(raw_json, dict) else raw_json)
        )
        saved = repo.insert_tank_reading(
            tank_id=payload.tank_id,
            level_percent=payload.level_percent,
            ts=getattr(payload, "ts", None),
            device_id=device_id_db,
            volume_l=volume_l,
            temperature_c=temperature_c,
            raw_json=raw_json,
        )
    except psy_errors.ForeignKeyViolation as e:
        log.warning("[ingest/tank] FK violation tank_id=%s device_id=%s err=%s", payload.tank_id, device_id_db, e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="invalid tank_id or device_id (foreign key)",
        )
    except psy_errors.CheckViolation as e:
        log.warning("[ingest/tank] check violation: %s", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"payload violates constraint: {e}",
        )
    except Exception as e:
        log.exception("[ingest/tank] DB insert failed type=%s err=%s", e.__class__.__name__, e)
        if SHOW_ERRORS:
            # 🔥 SOLO PARA DEBUG: revelar la causa exacta
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"ingest failed: {e.__class__.__name__}: {e}",
            )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ingest failed",
        )

    # 4) Bump de presencia (no crítico)
    try:
        if device_id_db:
            bump_presence(device_id_db)
    except Exception as e:
        log.warning("[presence] bump failed err=%s", e)

    # 5) Evaluación de alarmas (best-effort)
    try:
        lvl = _get_level_percent(saved)
        log.info("[ingest] eval_tank_alarm tank=%s lvl=%s", payload.tank_id, lvl)
        eval_fn = _get_eval_fn()
        if not eval_fn:
            log.warning("[ingest] eval_tank_alarm no disponible; ver logs de 'rdls.ingest'")
        else:
            eval_fn(payload.tank_id, lvl)
    except Exception as e:
        log.warning("[WARN] alarm eval failed: %s", e)

    # 6) Respuesta JSON-safe
    saved_dict = _saved_as_dict(saved)
    out = TankIngestOut(
        id=saved_dict.get("id"),
        tank_id=saved_dict.get("tank_id", payload.tank_id),
        device_id=(str(saved_dict.get("device_id")) if saved_dict.get("device_id") is not None
                   else (str(device_id_db) if device_id_db is not None else None)),
        ts=saved_dict.get("ts"),
        level_percent=saved_dict.get("level_percent", payload.level_percent),
        volume_l=saved_dict.get("volume_l"),
        temperature_c=saved_dict.get("temperature_c"),
        raw_json=saved_dict.get("raw_json"),
    )
    log.info("[ingest/tank] OK id=%s tank=%s lvl=%.2f", out.id, out.tank_id, out.level_percent)
    return jsonable_encoder(out)
