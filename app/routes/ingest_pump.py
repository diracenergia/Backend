# app/routes/ingest_pump.py
from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import datetime, timezone
import asyncio
import inspect
import logging

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.core.security import device_id_dep
from app.schemas.pumps import PumpPayload
from app.repos import pumps as repo

# >>> NEW: intentar publicar al “visor en vivo”; si no existe, no romper
try:
    from app.routes.live_view import apply_pump_ingest  # actualiza cache para /viz/ws y /viz/state
except Exception:
    def apply_pump_ingest(_: Dict[str, Any]) -> None:
        pass

log = logging.getLogger("rdls.ingest.pump")
router = APIRouter(prefix="/ingest", tags=["ingest"])


def _extract_device_id(auth_obj: Any) -> Optional[int]:
    """
    Extrae device_id desde el objeto de autenticación/dev dependencia.

    En modo demo o sin auth real, device_id_dep puede devolver:
      - dict con "device_id"
      - objeto con atributo .device_id
      - string convertible a int

    Retorna None si no existe o no es convertible.
    """
    raw = None
    if isinstance(auth_obj, dict):
        raw = auth_obj.get("device_id")
    else:
        raw = getattr(auth_obj, "device_id", None)

    if raw is None:
        return None

    try:
        s = str(raw).strip()
        return int(s) if s and (s.lstrip("-").isdigit()) else None
    except Exception:
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _await_maybe(fn, *args, **kwargs):
    """Permite usar repos asíncronos o síncronos indistintamente."""
    try:
        res = fn(*args, **kwargs)
        if inspect.isawaitable(res):
            return await res
        return res
    except Exception:
        raise


@router.post("/pump", status_code=status.HTTP_201_CREATED)
async def ingest_pump(
    payload: PumpPayload,
    auth: Any = Depends(device_id_dep),
    request: Request = None,
):
    """
    Inserta una lectura de bomba y publica en el visor en vivo (si está disponible).

    - Body: PumpPayload (ver app.schemas.pumps)
    - Header opcional: X-Device-Id: <int> (según tu device_id_dep)
    - Devuelve: {"ok": true, "reading_id": <int>, "source_ip": "..."}
    """
    device_id = _extract_device_id(auth)

    # Log mínimo (no sensible) para diagnóstico
    try:
        client = request.client.host if request and request.client else "unknown"
    except Exception:
        client = "unknown"

    # --- Persistencia (repo puede ser sync o async) ---
    try:
        reading_id = await _await_maybe(repo.insert_pump_reading, device_id, payload)
        # Si tu repo es estrictamente async/sync, podés usar directamente:
        # reading_id = await repo.insert_pump_reading(device_id, payload)
        # o
        # reading_id = repo.insert_pump_reading(device_id, payload)
    except HTTPException:
        raise
    except Exception as exc:
        log.exception("DB insert failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="db_error: no se pudo insertar la lectura de bomba",
        ) from exc

    # --- Publicación en “visor en vivo” (best-effort) ---
    try:
        data = payload.model_dump(exclude_none=True)
        # asegurar timestamp ISO si no vino
        if "ts" not in data or data.get("ts") is None:
            data["ts"] = _now_iso()
        # estructura estándar esperada por el visor
        publish = {
            "pump_id": data.get("pump_id"),
            "is_on": data.get("is_on"),
            "flow_lpm": data.get("flow_lpm"),
            "pressure_bar": data.get("pressure_bar"),
            # si tu schema tiene speed_pct, lo publicamos; si no, queda None
            "speed_pct": data.get("speed_pct"),
            "ts": data.get("ts"),
        }
        apply_pump_ingest(publish)
    except Exception as e:
        # No fallamos la ingesta por problemas de visualización
        log.warning("[viz] apply_pump_ingest failed: %s", e)

    return {"ok": True, "reading_id": reading_id, "source_ip": client}


# (Opcional) un ping rápido para verificar que el módulo está montado
@router.get("/pump/ping", status_code=status.HTTP_200_OK)
def pump_ping():
    return {"ok": True, "service": "ingest_pump"}
