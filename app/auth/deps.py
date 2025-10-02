# app/auth/deps.py
from __future__ import annotations

from typing import Generator, Optional, Tuple
import logging

import psycopg
from fastapi import Depends, Header, HTTPException, Query

from app.core.db import get_conn
from app.core.tenancy import require_org, get_user_id, get_role

logger = logging.getLogger("rdls.http")


# ---------- Resolución del org_id (tenancy/JWT > query > header > default) ----------

def _org_from_tenancy() -> Optional[int]:
    """
    Intenta obtener org_id desde el contexto de tenancy/JWT.
    Retorna None si no hay contexto cargado.
    """
    try:
        oid = require_org()
        return int(oid) if oid is not None else None
    except Exception:
        return None


def _resolve_org_id(
    org_id_q: Optional[int] = Query(None, alias="org_id"),
    org_id_header: Optional[int] = Header(None, alias="X-Org-Id"),
) -> Tuple[int, str]:
    """
    Prioridad:
      1) Tenancy/JWT (si el middleware ya lo cargó)
      2) Query string ?org_id=...
      3) Header X-Org-Id
      4) Default = 1
    Devuelve (org_id, source).
    """
    # 1) tenancy/JWT
    t_org = _org_from_tenancy()
    if t_org:
        return t_org, "tenancy"

    # 2) query
    if org_id_q:
        return int(org_id_q), "query"

    # 3) header
    if org_id_header:
        return int(org_id_header), "header"

    # 4) default
    return 1, "default"


def _apply_rls_vars(conn: psycopg.Connection, org_id: int, user_id: Optional[int], role: Optional[str]) -> None:
    """
    Aplica variables GUC por transacción.
    - app.org_id    (siempre)
    - app.user_id   (si hay usuario)
    - app.role      (si hay rol, si no: 'viewer' por defecto)
    """
    with conn.cursor() as cur:
        # org siempre
        cur.execute("SET LOCAL app.org_id = %s", (org_id,))

        # role y user_id son opcionales
        eff_role = role or "viewer"
        cur.execute("SET LOCAL app.role = %s", (eff_role,))

        if user_id is None:
            # dejar explícito NULL por si el SQL lo consulta
            cur.execute("SET LOCAL app.user_id = NULL")
        else:
            cur.execute("SET LOCAL app.user_id = %s", (int(user_id),))


# ---------- Dependencia principal: conexión con RLS aplicada ----------

def conn_with_rls(
    resolved: Tuple[int, str] = Depends(_resolve_org_id),
) -> Generator[psycopg.Connection, None, None]:
    """
    Devuelve una conexión psycopg con las GUCs de multi-tenant aplicadas
    para la transacción actual (SET LOCAL ...).

    - No depende de que un middleware previo haya seteado el org_id.
    - Si el middleware/tenancy ya cargó user_id/role, se usan; si no, se ignoran.
    - Siempre loguea el org_id resuelto y su origen.

    Uso típico:
        @router.get("/algo")
        def handler(conn = Depends(conn_with_rls)):
            with conn.cursor() as cur:
                cur.execute("SELECT ... WHERE org_id = current_setting('app.org_id')::bigint")
                ...
    """
    org_id, source = resolved
    user_id = get_user_id()
    role = get_role()

    # Abrimos conexión y aplicamos las GUCs por transacción
    with get_conn() as conn:
        _apply_rls_vars(conn, org_id, user_id, role)

        # Log de diagnóstico (parecido al que mostrabas en Render)
        try:
            logger.info(
                "[RLS] org_id=%s source=%s user_id=%s role=%s",
                org_id, source, user_id, role
            )
        except Exception:
            pass

        yield conn


# ---------- Auxiliares opcionales (compatibilidad/atajos) ----------

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
    Devuelve el org_id resuelto por el tenancy o 1 si no hay contexto y se usa default.
    Útil si querés inyectar org_id explícito en un endpoint.
    """
    org, _ = _resolve_org_id()
    return int(org)


# Alias por compatibilidad hacia atrás
conn = conn_with_rls
