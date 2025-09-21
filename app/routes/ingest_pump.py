# app/routes/ingest_pump.py

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.core.security import device_id_dep
from app.schemas.pumps import PumpPayload
from app.repos import pumps as repo

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
        # getattr devuelve None si no existe el atributo
        raw = getattr(auth_obj, "device_id", None)

    if raw is None:
        return None

    # Acepta int directo o string con dígitos
    try:
        # str(...) para manejar tipos como Decimal/UUID que puedan venir serializados
        s = str(raw).strip()
        return int(s) if s and (s.lstrip("-").isdigit()) else None
    except Exception:
        return None


@router.post("/pump", status_code=status.HTTP_201_CREATED)
async def ingest_pump(
    payload: PumpPayload,
    auth: Any = Depends(device_id_dep),
    request: Request = None,
):
    """
    Inserta una lectura de bomba.

    - Body: PumpPayload (ver app.schemas.pumps)
    - Header opcional (según tu dependencia): X-Device-Id: <int>
    - Devuelve: {"ok": true, "reading_id": <int>}
    """
    device_id = _extract_device_id(auth)

    # Log mínimo (no sensible) para diagnóstico
    try:
        client = request.client.host if request and request.client else "unknown"
    except Exception:
        client = "unknown"

    # Validación ligera: si el repo requiere device_id no nulo, avisamos.
    # Si tu repositorio acepta None (por ejemplo, para simulaciones),
    # podés quitar este bloque.
    if device_id is None:
        # Mantener simetría con la ruta de tanques: si querés permitir None,
        # comentá esta excepción y el repo decidirá.
        # Nota: Si preferís permitir None, dejá que repo.insert_pump_reading lo maneje.
        # raise HTTPException(
        #     status_code=status.HTTP_400_BAD_REQUEST,
        #     detail="device_id ausente o inválido (cabecera X-Device-Id o contexto de auth).",
        # )
        pass

    try:
        reading_id = await repo.insert_pump_reading(device_id, payload)
        # Algunos repositorios pueden ser sync; si el tuyo es sync, usa:
        # reading_id = repo.insert_pump_reading(device_id, payload)
    except HTTPException:
        # Si el repo ya lanza HTTPException, la propagamos
        raise
    except Exception as exc:
        # Aquí atrapamos errores de DB, validación interna, etc.
        # No exponemos información sensible; dejamos un mensaje claro para el cliente.
        # En tus logs reales podés registrar `exc` completo.
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="db_error: no se pudo insertar la lectura de bomba",
        ) from exc

    return {"ok": True, "reading_id": reading_id, "source_ip": client}


# (Opcional) un ping rápido para verificar que el módulo está montado
@router.get("/pump/ping", status_code=status.HTTP_200_OK)
def pump_ping():
    return {"ok": True, "service": "ingest_pump"}
