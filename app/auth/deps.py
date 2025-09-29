# app/auth/deps.py
import os
from typing import Any, Dict
from fastapi import Depends, HTTPException, Request
from jose import jwt, JWTError
from psycopg.rows import dict_row

from app.core.db import get_conn  # ← ya lo tenés en tu proyecto

SECRET = os.getenv("AUTH_SECRET", "devsecret")
ALGO = "HS256"

def get_current_user(request: Request) -> Dict[str, Any]:
    """Lee el Bearer token y devuelve el payload (user/org/role)."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(401, "Falta token")
    token = auth.removeprefix("Bearer ").strip()
    try:
        payload = jwt.decode(token, SECRET, algorithms=[ALGO])
    except JWTError:
        raise HTTPException(401, "Token inválido")

    # payload esperado: { sub: user_id, org_id, role, ... }
    if "sub" not in payload or "org_id" not in payload:
        raise HTTPException(401, "Token incompleto")
    return payload

def conn_with_rls(user=Depends(get_current_user)):
    """
    Devuelve una conexión con las GUCs seteadas para que las RLS
    basadas en app.org_id / app.user_id funcionen.
    Usá esta dependencia en tus endpoints protegidos.
    """
    conn = get_conn()
    cur = conn.cursor(row_factory=dict_row)
    # SET LOCAL = válido solo para la transacción/sesión de esta conexión
    cur.execute("set local app.org_id = %s;", (str(user["org_id"]),))
    cur.execute("set local app.user_id = %s;", (str(user["sub"]),))
    return conn
