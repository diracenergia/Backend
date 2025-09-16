# app/core/tenancy.py
from __future__ import annotations

from typing import Optional, Tuple, Dict, Any
from contextvars import ContextVar
from fastapi import Header, HTTPException, Request
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

def require_org() -> int:
    org_id = get_org_id()
    if org_id is None:
        raise HTTPException(400, "Falta X-Org-Id")
    return int(org_id)

def require_user() -> int:
    user_id = get_user_id()
    if user_id is None:
        raise HTTPException(401, "Falta usuario (X-User-Id o token)")
    return int(user_id)

# ==== Utilidades de Auth/JWT (opcional) ====
VERIFY_JWT = os.getenv("TENANCY_VERIFY_JWT", "0") in ("1", "true", "True")
JWT_AUD     = os.getenv("TENANCY_JWT_AUD")  # opcional
JWT_ISS     = os.getenv("TENANCY_JWT_ISS")  # opcional
JWT_ALGO    = os.getenv("TENANCY_JWT_ALGO", "RS256")  # si verificás firma

def _parse_bearer_token(authorization: Optional[str]) -> Dict[str, Any]:
    """
    Devuelve el payload del JWT si hay Authorization: Bearer ... .
    Por defecto NO verifica firma (útil en dev). Activá TENANCY_VERIFY_JWT=1 para verificar.
    """
    if not authorization or not authorization.startswith("Bearer "):
        return {}
    token = authorization.replace("Bearer ", "", 1).strip()
    try:
        import jwt  # PyJWT
        if VERIFY_JWT:
            # Necesitás TENANCY_JWT_PUBLIC_KEY (o jwks) para validar
            public_key = os.getenv("TENANCY_JWT_PUBLIC_KEY")
            if not public_key:
                # Si no hay clave, caemos a decodificar sin verify como fallback controlado
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
        # Acepta str/float/int
        return int(str(value))
    except Exception:
        return None

# --------- Dependencia FastAPI ----------
async def tenant_ctx_dep(
    request: Request,
    x_org_id: Optional[int] = Header(default=None, convert_underscores=False),
    x_user_id: Optional[int] = Header(default=None, convert_underscores=False),
    x_role:   Optional[str] = Header(default=None, convert_underscores=False),
    authorization: Optional[str] = Header(default=None),
):
    """
    Resuelve la organización/usuario activos y los inyecta en ContextVars.
    Prioridad (de mayor a menor):
      1) Headers: X-Org-Id, X-User-Id, X-Role
      2) JWT Bearer (claims sugeridas: sub=user_id, active_org_id, role)
    Requiere SIEMPRE org_id. user_id puede ser opcional según tus rutas.
    """
    # 1) Intentar headers
    hdr_org = _coerce_int(x_org_id)
    hdr_usr = _coerce_int(x_user_id)
    role    = (x_role or None)

    # 2) Si falta algo, intentar token
    if hdr_org is None or hdr_usr is None or role is None:
        claims = _parse_bearer_token(authorization)
        # Convenciones de claims (ajustá si tus tokens difieren)
        jwt_user = _coerce_int(claims.get("sub") or claims.get("user_id"))
        jwt_org  = _coerce_int(claims.get("active_org_id") or claims.get("org_id"))
        jwt_role = claims.get("role") or claims.get("roles", {}).get(str(jwt_org)) if isinstance(claims.get("roles"), dict) else claims.get("role")

        # Mezclar: header tiene prioridad si vino
        user_id = hdr_usr if hdr_usr is not None else jwt_user
        org_id  = hdr_org if hdr_org is not None else jwt_org
        role    = role if role is not None else (jwt_role if isinstance(jwt_role, str) else None)
    else:
        user_id = hdr_usr
        org_id  = hdr_org

    # 3) Validaciones mínimas
    if org_id is None:
        raise HTTPException(400, "Falta X-Org-Id (o active_org_id en token)")

    # (opcional) validar que org_id sea > 0
    if int(org_id) <= 0:
        raise HTTPException(400, "X-Org-Id inválido")

    # 4) Guardar en ContextVars
    set_context(user_id=user_id, org_id=org_id, role=role)

    # 5) Devolver info mínima (útil para debug en middlewares/handlers)
    return {"user_id": user_id, "org_id": org_id, "role": role}
