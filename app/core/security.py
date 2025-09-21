# app/core/security.py  — MODO DEMO 100% PERMISIVO
from __future__ import annotations
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from fastapi import Request

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def device_id_dep(
    request: Request,
    x_api_key: Optional[str] = None,
    authorization: Optional[str] = None,
    x_device_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    ⚠️ Permisivo total (solo DEV):
      - No exige API key ni valida lista blanca.
      - Acepta device_id si viene por header o query (?device_id=), pero no lo requiere.
      - Nunca levanta 401/403 por autenticación.
    """
    # no usamos ninguna credencial; sólo devolvemos metadatos
    device_id = (
        (x_device_id or "").strip()
        or (request.query_params.get("device_id") or "").strip()
        or None
    )
    return {
        "api_key": None,          # siempre None en modo demo
        "device_id": device_id,   # puede ser None
        "ts": _now_iso(),
        "strict": False,          # señal para logs
    }
