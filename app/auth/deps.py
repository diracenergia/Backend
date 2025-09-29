from typing import Optional
from fastapi import HTTPException
from psycopg.rows import dict_row

from app.core.db import get_conn
from app.core.tenancy import get_org_id, get_user_id, DEFAULT_ORG_ID

def conn_with_rls():
    """
    Abre conexión, fija GUCs (app.org_id / app.user_id), la cede a la ruta y
    la cierra automáticamente al finalizar.
    """
    org = get_org_id() or DEFAULT_ORG_ID
    user = get_user_id()
    if org is None:
        raise HTTPException(400, "org_id no resuelto")

    cm = get_conn()             # <- context manager
    conn = cm.__enter__()       # <- entrar
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("set local app.org_id = %s;", (str(org),))
            if user is not None:
                cur.execute("set local app.user_id = %s;", (str(user),))
        yield conn              # <- usar esta conexión en la ruta
    finally:
        cm.__exit__(None, None, None)  # <- salir/cerrar
