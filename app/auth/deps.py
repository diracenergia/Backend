# app/auth/deps.py
from __future__ import annotations

from contextlib import contextmanager
from typing import Optional
from psycopg.rows import dict_row

from app.core.db import get_conn
from app.core.tenancy import require_org, get_user_id, get_role


@contextmanager
def conn_with_rls():
    """
    Abre una conexión y setea GUCs (RLS) para este request.
    Usa set_config(..., ..., true) que equivale a SET LOCAL.
    Devuelve la conexión lista para usar (y se cierra al salir del endpoint).
    """
    # get_conn() es un context manager -> entramos y extraemos la conn real
    with get_conn() as conn:
        # seteamos los GUCs una sola vez al inicio
        with conn.cursor(row_factory=dict_row) as cur:
            org_id = require_org()
            cur.execute("select set_config('app.org_id', %s, true);", (str(org_id),))

            uid: Optional[int] = get_user_id()
            if uid is not None:
                cur.execute("select set_config('app.user_id', %s, true);", (str(uid),))

            role = get_role()
            if role is not None:
                cur.execute("select set_config('app.role', %s, true);", (str(role),))

        try:
            yield conn
        finally:
            # al salir cerramos; el cierre resetea cualquier setting local
            try:
                conn.close()
            except Exception:
                pass
