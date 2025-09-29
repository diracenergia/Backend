# app/auth/deps.py
from __future__ import annotations

from typing import Generator, Optional

import psycopg
from fastapi import HTTPException

from app.core.db import get_conn
from app.core.tenancy import require_org, get_user_id, get_role


def conn_with_rls() -> Generator[psycopg.Connection, None, None]:
    """
    Devuelve una conexión de Postgres con el contexto multi-tenant aplicado.
    Importante:
      - app.core.db.get_conn() ya setea los GUCs (app.org_id, app.user_id, app.role)
        a **nivel de sesión** usando los contextvars que cargó el middleware/tenancy.
      - Por eso acá NO volvemos a llamar set_config() ni abrimos transacciones.

    Uso típico en endpoints:
        @router.get("/algo")
        def handler(conn = Depends(conn_with_rls)):
            with conn.cursor() as cur:
                ...
    """
    with get_conn() as conn:
        yield conn


# --- Dependencias auxiliares (opcionales) ------------------------------------

def require_user_dep() -> int:
    """
    Exige que exista un usuario autenticado en el contexto actual.
    Devuelve el user_id o levanta 401.
    """
    uid: Optional[int] = get_user_id()
    if uid is None:
        raise HTTPException(status_code=401, detail="auth required")
    return int(uid)


def require_org_dep() -> int:
    """
    Devuelve el org_id resuelto por el tenancy (o el default si está configurado
    para entorno de dev). Útil si querés inyectar org_id explícito en un endpoint.
    """
    return int(require_org())


# Alias por compatibilidad hacia atrás (si algún import esperaba 'conn')
conn = conn_with_rls
