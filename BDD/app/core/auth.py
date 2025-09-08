# app/core/auth.py
from __future__ import annotations
import os
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from fastapi import Request, Header, HTTPException

# Modo: en producción podés exigir clave; en dev permitir sin clave.
REQUIRE_API_KEY = os.getenv("INGEST_REQUIRE_API_KEY", "0") in ("1", "true", "True")
# Si querés un valor por defecto para dev (opcional):
DEFAULT_DEV_KEY = os.getenv("DEFAULT_API_KEY", "simulador123")  # mismo que usás en el front

def _now_utc():
    return datetime.now(timezone.utc)

def get_auth_ctx(
    request: Request,
    x_api_key: Optional[str] = Header(None),
    authorization: Optional[str] = Header(None),
    x_device_id: Optional[str] = Header(None),
) -> Dict[str, Any]:
    """
    Extrae api_key y device_id de:
      - Authorization: Bearer <token>
      - X-API-Key: <token>
      - ?api_key=... (querystring)
      - X-Device-Id o ?device_id=...
    En modo estricto: 401 si no hay api_key.
    En modo permisivo: api_key puede ser None (o DEFAULT_DEV_KEY si querés).
    """
    # 1) Headers
    api_key = None
    if x_api_key:
        api_key = x_api_key.strip()
    elif authorization and authorization.lower().startswith("bearer "):
        api_key = authorization.split(" ", 1)[1].strip()

    # 2) Querystring (fallback)
    if not api_key:
        api_key = (request.query_params.get("api_key") or "").strip() or None

    # 3) device_id
    device_id = (x_device_id or "").strip() or (request.query_params.get("device_id") or "").strip() or None

    if REQUIRE_API_KEY and not api_key:
        # producción estricta
        raise HTTPException(401, "Missing API key")

    if not api_key and not REQUIRE_API_KEY:
        # desarrollo permisivo: podés devolver None o una default
        api_key = DEFAULT_DEV_KEY or None

    return {
        "api_key": api_key,
        "device_id": device_id,
        "ts": _now_utc().isoformat(),
        "strict": REQUIRE_API_KEY,
    }
