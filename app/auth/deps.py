# app/auth/deps.py
from __future__ import annotations

from typing import Generator, Optional
import psycopg
from psycopg.rows import dict_row

from app.core.db import get_conn
from app.core.tenancy import require_org, get_user_id, get_role

def conn_with_rls() -> Generator[psycopg.Connection, None, None]:
    """
    Devuelve una conexión de Postgres con el contexto RLS seteado
    (app.org_id, app.user_id, app.role). FastAPI cierra la conexión
    cuando termina el endpoint.

    Uso en endpoint:
        def handler(conn = Depends(conn_with_rls)):
            with conn.cursor(row_factory=dict_row) as cur:
                ...
    """
    # get_conn ya abre la conexión y aplica contexto base; acá reforzamos por request
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            org_id = require_org()  # lanza 401/403 si no hay org válida
            # set_config() con is_local=true => scope a la transacción actual
            cur.execute("SELECT set_config('app.org_id', %s, true)", (str(org_id),))

            uid: Optional[int] = get_user_id()
            if uid is not None:
                cur.execute("SELECT set_config('app.user_id', %s, true)", (str(uid),))

            role = get_role()
            if role:
                cur.execute("SELECT set_config('app.role', %s, true)", (str(role),))

        # Entregamos la conexión REAL al endpoint
        yield conn
