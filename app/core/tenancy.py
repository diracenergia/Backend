# app/core/tenancy.py
from __future__ import annotations

from typing import Optional, Tuple, Dict, Any
from contextvars import ContextVar
from fastapi import HTTPException, Request
import os

# ==== Contexto actual (por-request) ====
_current_user_id: ContextVar[Optional[int]] = ContextVar("tenant_user_id", default=None)
_current_org_id:  ContextVar[Optional[int]] = ContextVar("tenant_org_id",  default=None)
_current_role:    ContextVar[Optional[str]] = ContextVar("tenant_role",    default=None)

def set_context(user_id: Optional[int], org_id: Optional[int], role: Optional[str] = None) -> None:
    _current_user_id.set(user_id)
    _current_org_id.set(org_id)
    _current_role.set(role)

def clear_context() -> None:
    _current_user_id.set(None)
    _current_org_id.set(None)
    _current_role.set(None)

def get_context() -> Tuple[Optional[int], Optional[int], Optional[str]]:
    return _current_user_id.get(), _current_org_id.get(), _current_role.get()

def get_user_id() -> Optional[int]:
    return _current_user_id.get()

def get_org_id() -> Optional[int]:
    return _current_org_id.get()

def get_role() -> Optional[str]:
    return _current_role.get()

# ==== Config ====
# En producción podés poner TENANCY_ENFORCE_ORG=1 para volver a exigirlo
ENFORCE_ORG = os.getenv("TENANCY_ENFORCE_ORG", "0").lower() in ("1", "true")
DEFAULT_ORG_ID_RAW = os.getenv("TENANCY_DEFAULT_ORG_ID", "1")
try:
    DEFAULT_ORG_ID: Optional[int] = int(str(DEFAULT_ORG_ID_RAW))
except Exception:
    # si querés manejar strings, podrías cambiar el tipo a Optional[Any]
    DEFAULT_ORG_ID = 1

VERIFY_JWT = os.getenv("TENANCY_VERIFY_JWT", "0").lower() in ("1", "true")
JWT_AUD     = os.getenv("TENANCY_JWT_AUD")
JWT_ISS     = os.getenv("TENANCY_JWT_ISS")
JWT_ALGO    = os.getenv("TENANCY_JWT_ALGO", "RS256")

# ==== JWT util ====
def _parse_bearer_token(authorization: Optional[str]) -> Dict[str, Any]:
    if not authorization:
        return {}
    auth = authorization.strip()
    if not auth.lower().startswith("bearer "):
        return {}
    token = auth[7:].strip()
    try:
        import jwt  # PyJWT
        if VERIFY_JWT:
            public_key = os.getenv("TENANCY_JWT_PUBLIC_KEY")
            if not public_key:
                payload = jwt.decode(token, options={"verify_signature": False})
            else:
                payload = jwt.decode(
                    token,
                    public_key,
                    algorithms=[JWT_ALGO],
                    audience=JWT_AUD if JWT_AUD else None,
                    issuer=JWT_ISS if JWT_ISS else None,
                )
        else:
            payload = jwt.decode(token, options={"verify_signature": False})
        return payload or {}
    except Exception:
        return {}

def _coerce_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(str(value))
    except Exception:
        return None

# --------- Dependencia usada desde middleware ----------
async def tenant_ctx_dep(
    request: Request,
    authorization: Optional[str] = None,
    x_org_id: Optional[str] = None,
    x_user_id: Optional[str] = None,
    x_role: Optional[str] = None,
):
    """
    Prioridad:
      1) Headers: X-Org-Id, X-User-Id, X-Role
      2) JWT Bearer: sub/user_id, active_org_id/org_id, role
      3) Query string (útil para WebSocket): org_id, user_id, role
    Si ENFORCE_ORG=0, usa DEFAULT_ORG_ID cuando no venga.
    """
    # headers
    if authorization is None:
        authorization = request.headers.get("authorization")
    if x_org_id is None:
        x_org_id = request.headers.get("x-org-id")
    if x_user_id is None:
        x_user_id = request.headers.get("x-user-id")
    if x_role is None:
        x_role = request.headers.get("x-role")

    hdr_org = _coerce_int(x_org_id)
    hdr_usr = _coerce_int(x_user_id)
    role    = (x_role or None)

    # jwt
    claims: Dict[str, Any] = {}
    if hdr_org is None or hdr_usr is None or role is None:
        claims = _parse_bearer_token(authorization)
    jwt_user = _coerce_int(claims.get("sub") or claims.get("user_id"))
    jwt_org  = _coerce_int(claims.get("active_org_id") or claims.get("org_id"))
    jwt_role = claims.get("role")

    # query
    q = request.query_params
    q_org  = _coerce_int(q.get("org_id"))
    q_user = _coerce_int(q.get("user_id"))
    q_role = q.get("role")

    # resolver finales con prioridad: header > query > jwt
    user_id = hdr_usr if hdr_usr is not None else (q_user if q_user is not None else jwt_user)
    org_id  = hdr_org if hdr_org is not None else (q_org  if q_org  is not None else jwt_org)
    role    = role    if role    is not None else (q_role if q_role is not None else jwt_role)

    # En este modo NO exigimos org: usamos default si falta
    if org_id is None:
        if ENFORCE_ORG:
            raise HTTPException(400, "Falta X-Org-Id (o active_org_id en token)")
        org_id = DEFAULT_ORG_ID

    # Guardar contexto
    set_context(user_id=user_id, org_id=org_id, role=role)
    return {"user_id": user_id, "org_id": org_id, "role": role}

# ==== Helpers de requerimiento (suaves) ====
def require_org() -> int:
    """
    En modo no estricto, devuelve el org actual o el DEFAULT,
    sin lanzar excepción.
    """
    org_id = get_org_id()
    if org_id is None:
        # si llegaste acá sin contexto, devolvemos default
        return int(DEFAULT_ORG_ID) if DEFAULT_ORG_ID is not None else 0
    return int(org_id)

def require_user() -> int:
    """
    Si usás login de usuario/contraseña para otras rutas, podés dejar esta validación.
    Para la demo/simulación no suele usarse.
    """
    user_id = get_user_id()
    if user_id is None:
        raise HTTPException(401, "Falta usuario (X-User-Id o token)")
    return int(user_id)
