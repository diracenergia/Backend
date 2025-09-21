# app/routes/ingest.py  (SECCIÓN TANK)
from typing import Any, Optional, Dict
import logging
import importlib
import os

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.encoders import jsonable_encoder
from psycopg import errors as psy_errors
import psycopg

from app.schemas.ingest import TankIngestIn, TankIngestOut
from app.repos import tanks as repo
from app.core.security import device_id_dep
from app.repos.presence import bump_presence  # presencia online/offline

log = logging.getLogger("ingest")
router = APIRouter(prefix="/ingest", tags=["ingest"])

VERBOSE = os.getenv("INGEST_VERBOSE_ERRORS", "0") in ("1", "true", "True")


def _diag_from_psycopg(e: BaseException) -> Dict[str, Any]:
    """Extrae lo más útil de psycopg para devolver en detalle (solo DEV)."""
    d: Dict[str, Any] = {"type": type(e).__name__, "msg": str(e)}
    if isinstance(e, psycopg.Error):
        d["sqlstate"] = getattr(e, "sqlstate", None)
        diag = getattr(e, "diag", None)
        if diag:
            d["diag"] = {
                "message_primary": getattr(diag, "message_primary", None),
                "constraint_name": getattr(diag, "constraint_name", None),
                "schema_name": getattr(diag, "schema_name", None),
                "table_name": getattr(diag, "table_name", None),
                "column_name": getattr(diag, "column_name", None),
                "datatype_name": getattr(diag, "datatype_name", None),
                "internal_query": getattr(diag, "internal_query", None),
                "context": getattr(diag, "context", None),
            }
    return d


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
    out: Dict[str, Any] = {}
    for k in ("id", "org_id", "tank_id", "device_id", "ts",
              "level_percent", "volume_l", "temperature_c", "raw_json"):
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
    """
    Inserta lectura de tanque. Ahora devuelve DIAGNÓSTICO útil ante 400/500.
    """

    # Elegimos device_id (si tu col es TEXT, podés no castear a int)
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

    # priorizamos el del payload si viene
    dev_from_payload = getattr(payload, "device_id", None)
    device_id_db = _to_int_or_none(dev_from_auth) or _to_int_or_none(dev_from_payload)

    # Campos opcionales
    volume_l = getattr(payload, "volume_l", None)
    temperature_c = getattr(payload, "temperature_c", None)
    raw_json = getattr(payload, "raw_json", None)  # <- OJO: tomamos raw_json, no "extra"

    log.debug(
        "[ingest/tank] in tank=%s lvl=%s ts=%s dev=%s",
        payload.tank_id, payload.level_percent, getattr(payload, "ts", None), device_id_db
    )

    try:
        saved = repo.insert_tank_reading(
            tank_id=payload.tank_id,
            level_percent=payload.level_percent,
            ts=getattr(payload, "ts", None),
            device_id=device_id_db,            # si tu DB tiene TEXT, cambia firma y no castees
            volume_l=volume_l,
            temperature_c=temperature_c,
            raw_json=raw_json,
        )
    except psy_errors.ForeignKeyViolation as e:
        diag = _diag_from_psycopg(e)
        log.warning("[ingest/tank] FK violation: %s", diag)
        raise HTTPException(status_code=400, detail={"error": "fk_violation", **diag})
    except psy_errors.CheckViolation as e:
        diag = _diag_from_psycopg(e)
        log.warning("[ingest/tank] CHECK violation: %s", diag)
        raise HTTPException(status_code=400, detail={"error": "check_violation", **diag})
    except psy_errors.NotNullViolation as e:
        diag = _diag_from_psycopg(e)
        log.warning("[ingest/tank] NOT NULL violation: %s", diag)
        raise HTTPException(status_code=400, detail={"error": "not_null", **diag})
    except psycopg.Error as e:
        # Cualquier otro error de Postgres (UndefinedColumn, RLS, etc.)
        diag = _diag_from_psycopg(e)
        log.error("[ingest/tank] PG error: %s", diag)
        detail = diag if VERBOSE else {"error": "db_error"}
        raise HTTPException(status_code=500, detail=detail)
    except Exception as e:
        diag = _diag_from_psycopg(e)
        log.exception("[ingest/tank] unknown error: %s", diag)
        detail = diag if VERBOSE else {"error": "ingest_failed"}
        raise HTTPException(status_code=500, detail=detail)

    # presencia (best-effort)
    try:
        if device_id_db:
            bump_presence(device_id_db)
    except Exception as e:
        log.warning("[presence] bump failed err=%s", e)

    # alarmas (best-effort)
    try:
        lvl = _get_level_percent(saved)
        eval_fn = _get_eval_fn()
        if eval_fn:
            eval_fn(payload.tank_id, lvl)
    except Exception as e:
        log.warning("[WARN] alarm eval failed: %s", e)

    saved_dict = _saved_as_dict(saved)
    out = TankIngestOut(
        id=saved_dict.get("id"),
        org_id=saved_dict.get("org_id"),
        tank_id=saved_dict.get("tank_id", payload.tank_id),
        device_id=saved_dict.get("device_id", device_id_db),
        ts=saved_dict.get("ts"),
        level_percent=saved_dict.get("level_percent", payload.level_percent),
        volume_l=saved_dict.get("volume_l"),
        temperature_c=saved_dict.get("temperature_c"),
        raw_json=saved_dict.get("raw_json"),
        ok=True,
    )
    return jsonable_encoder(out)
