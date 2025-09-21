# app/routes/ingest.py
from typing import Any, Optional, Dict
import logging
import importlib

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.encoders import jsonable_encoder
from psycopg import errors as psy_errors

from app.schemas.ingest import TankIngestIn, TankIngestOut
from app.repos import tanks as repo
from app.core.security import device_id_dep
from app.repos.presence import bump_presence  # presencia online/offline

log = logging.getLogger("rdls.ingest")

router = APIRouter(prefix="/ingest", tags=["ingest"])


def _get_level_percent(saved: Any) -> Optional[float]:
    if saved is None:
        return None
    if isinstance(saved, dict):
        return saved.get("level_percent")
    if hasattr(saved, "model_dump"):
        return saved.model_dump().get("level_percent")
    return getattr(saved, "level_percent", None)


def _saved_as_dict(saved: Any) -> Dict[str, Any]:
    """
    Normaliza lo que devuelva el repo (row/Record/pydantic/dict)
    a un dict con tipos JSON-seguros.
    """
    if saved is None:
        return {}
    if isinstance(saved, dict):
        return saved
    if hasattr(saved, "model_dump"):
        return saved.model_dump()
    # objeto con attrs
    out = {}
    for k in (
        "id",
        "tank_id",
        "device_id",
        "ts",
        "level_percent",
        "volume_l",
        "temperature_c",
        "raw_json",
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
    """
    Import perezoso para no tumbar la app si app.services.alarms_eval tiene un error.
    """
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
    """
    - Prioriza el device_id autenticado (header X-Device-Id / API key); si no hay, usa el del payload.
    - Inserta (tank_id, level_percent, ts, device_id, volume_l, temperature_c, raw_json).
    - FK/Checks → 400; otros errores → 500.
    - Evalúa alarmas y bump de presencia en best-effort.
    """

    # 0) Log de entrada (sin exponer secrets)
    try:
        hdr_info = {
            "strict": getattr(auth, "strict", None) if not isinstance(auth, dict) else auth.get("strict"),
            "api_key_present": bool((isinstance(auth, dict) and auth.get("api_key")) or getattr(auth, "api_key", None)),
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

    # 👉 normalizamos: DB y repo esperan texto; si vino int lo pasamos a str
    device_id_int = _to_int_or_none(dev_from_auth) or _to_int_or_none(dev_from_payload)
    device_id_db: Optional[str] = str(device_id_int) if device_id_int is not None else (
        str(dev_from_payload) if dev_from_payload not in (None, "") else None
    )

    # 2) Extras opcionales
    volume_l = getattr(payload, "volume_l", None)
    temperature_c = getattr(payload, "temperature_c", None)
    raw_json = getattr(payload, "raw_json", None)   # nombre correcto

    # 3) Insert con manejo de errores fino
    try:
        saved = repo.insert_tank_reading(
            tank_id=payload.tank_id,
            level_percent=payload.level_percent,
            ts=getattr(payload, "ts", None),  # usa ts del payload si viene; si no, NOW() en DB
            device_id=device_id_db,           # <-- ya string
            volume_l=volume_l,
            temperature_c=temperature_c,
            raw_json=raw_json,
        )
    except psy_errors.ForeignKeyViolation:
        log.warning("[ingest/tank] FK violation tank_id=%s device_id=%s", payload.tank_id, device_id_db)
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
        log.exception("[ingest/tank] DB insert failed err=%s", e)
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
        device_id=saved_dict.get("device_id", device_id_db),
        ts=saved_dict.get("ts"),
        level_percent=saved_dict.get("level_percent", payload.level_percent),
        volume_l=saved_dict.get("volume_l"),
        temperature_c=saved_dict.get("temperature_c"),
        raw_json=saved_dict.get("raw_json"),
    )

    log.info("[ingest/tank] OK id=%s tank=%s lvl=%.2f dev=%s", out.id, out.tank_id, out.level_percent, out.device_id)
    return jsonable_encoder(out)
