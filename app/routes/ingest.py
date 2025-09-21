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

log = logging.getLogger("ingest")

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
        "org_id",
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

    # 1) Elegir device_id (preferimos el autenticado para evitar spoof)
    dev_from_auth = None
    try:
        # device_id_dep puede retornar dict/pydantic/objeto simple
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
    raw_json = getattr(payload, "extra", None)

    # 3) Insert con manejo de errores fino
    try:
        saved = repo.insert_tank_reading(
            tank_id=payload.tank_id,
            level_percent=payload.level_percent,
            ts=getattr(payload, "ts", None),  # usa ts del payload si viene; si no, NOW() en DB
            device_id=device_id_db,           # FK a devices.id (INT)
            volume_l=volume_l,
            temperature_c=temperature_c,
            raw_json=raw_json,
        )
    except psy_errors.ForeignKeyViolation:
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
            log.warning("[ingest] eval_tank_alarm no disponible; ver logs de 'ingest'")
        else:
            eval_fn(payload.tank_id, lvl)
    except Exception as e:
        log.warning("[WARN] alarm eval failed: %s", e)

    # 6) Respuesta JSON-safe (evita serializar objetos del driver/ORM)
    saved_dict = _saved_as_dict(saved)

    # Construimos el modelo de salida con defaults seguros
    out = TankIngestOut(
        id=saved_dict.get("id"),
        org_id=saved_dict.get("org_id"),
        tank_id=saved_dict.get("tank_id", payload.tank_id),
        device_id=saved_dict.get("device_id", device_id_db),
        ts=saved_dict.get("ts"),  # si DB hizo NOW(), debería venir seteado
        level_percent=saved_dict.get("level_percent", payload.level_percent),
        volume_l=saved_dict.get("volume_l"),
        temperature_c=saved_dict.get("temperature_c"),
        raw_json=saved_dict.get("raw_json"),
        ok=True,
    )

    # FastAPI ya serializa pydantic, pero usamos jsonable_encoder por seguridad si hiciera falta
    return jsonable_encoder(out)
