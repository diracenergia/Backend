# app/auth/deps.py
from typing import Optional
from psycopg.rows import dict_row
from app.core.db import get_conn
from app.core.tenancy import require_org, get_user_id, get_role

def conn_with_rls():
    """
    Devuelve una conexión con los GUCs (RLS) seteados para el request.
    FastAPI cierra la conexión al terminar el endpoint.
    """
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            org_id = require_org()
            cur.execute("select set_config('app.org_id', %s, true);", (str(org_id),))

            uid: Optional[int] = get_user_id()
            if uid is not None:
                cur.execute("select set_config('app.user_id', %s, true);", (str(uid),))

            role = get_role()
            if role:
                cur.execute("select set_config('app.role', %s, true);", (str(role),))

        # ⬇ Esto entrega la conexión REAL al endpoint
        yield conn
