# app/auth/deps.py
import os
from fastapi import Depends, HTTPException, Request
from psycopg.rows import dict_row
from typing import Any, Dict

from app.core.db import get_conn
from app.core.tenancy import get_org_id, get_user_id, DEFAULT_ORG_ID

def conn_with_rls():
    """
    Devuelve una conexión con GUCs seteadas según el contexto de la request.
    tenant_ctx_dep (middleware) ya resolvió org_id/user_id y los guardó en ContextVars.
    """
    org = get_org_id() or DEFAULT_ORG_ID
    user = get_user_id()

    if org is None:
        # Si tu app permite DEFAULT_ORG_ID, podés no lanzar error acá
        raise HTTPException(400, "org_id no resuelto en el contexto")

    conn = get_conn()
    cur = conn.cursor(row_factory=dict_row)
    # SET LOCAL aplica al scope de la transacción/conn de esta request
    cur.execute("set local app.org_id = %s;", (str(org),))
    if user is not None:
        cur.execute("set local app.user_id = %s;", (str(user),))
    else:
        # si preferís limpiar:
        cur.execute("set local app.user_id = DEFAULT;")  # o simplemente no setearlo
    return conn
