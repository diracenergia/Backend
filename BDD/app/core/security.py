# app/core/security.py
from __future__ import annotations

import os
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from fastapi import Request, Header, HTTPException

# === Modo de autenticación ===
# DEV (por defecto): no exige api_key (permite sin clave).
# PROD: exportá INGEST_REQUIRE_API_KEY=1 para exigir api_key.
REQUIRE_API_KEY = os.getenv("INGEST_REQUIRE_API_KEY", "0") in ("1", "true", "True")

# Listas/valores permitidos (opcional). Si las seteás y estás en modo estricto,
# se validará que la api_key pertenezca a este conjunto.
_ALLOWED = set(
    k.strip() for k in os.getenv("INGEST_ALLOWED_KEYS", "").split(",") if k.strip()
)
_SINGLE = os.getenv("DEFAULT_API_KEY", "").strip()
if _SINGLE:
    _ALLOWED.add(_SINGLE)

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _extract_api_key_and_device(
    request: Request,
    x_api_key: Optional[str],
    authorization: Optional[str],
    x_device_id: Optional[str],
) -> tuple[Optional[str], Optional[str]]:
    # 1) api_key por headers
    api_key: Optional[str] = None
    if x_api_key:
        api_key = x_api_key.strip()
    elif authorization and authorization.lower().startswith("bearer "):
        api_key = authorization.split(" ", 1)[1].strip()

    # 2) fallback por querystring
    if not api_key:
        qs_key = (request.query_params.get("api_key") or "").strip()
        api_key = qs_key or None

    # 3) device_id por header o query
    device_id = (x_device_id or "").strip() or (request.query_params.get("device_id") or "").strip() or None

    return api_key, device_id

def device_id_dep(
    request: Request,
    x_api_key: Optional[str] = Header(None),
    authorization: Optional[str] = Header(None),
    x_device_id: Optional[str] = Header(None),
) -> Dict[str, Any]:
    """
    Dependencia permisiva:
      - Acepta Authorization: Bearer <token>, X-API-Key, o ?api_key=
      - device_id desde X-Device-Id o ?device_id=
      - En modo estricto (INGEST_REQUIRE_API_KEY=1), 401 si falta api_key o no está permitida.
    Devuelve un dict con {"api_key", "device_id", "ts"}.
    """
    api_key, device_id = _extract_api_key_and_device(request, x_api_key, authorization, x_device_id)

    if REQUIRE_API_KEY:
        if not api_key:
            raise HTTPException(401, "Missing API key")
        if _ALLOWED and api_key not in _ALLOWED:
            raise HTTPException(401, "Invalid API key")

    return {
        "api_key": api_key,         # puede ser None en modo permisivo
        "device_id": device_id,     # puede ser None si no vino en header/query
        "ts": _now_iso(),
        "strict": REQUIRE_API_KEY,
    }
