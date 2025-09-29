# app/auth/router.py
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
import jwt
from psycopg.rows import dict_row

from app.core.db import get_conn

router = APIRouter(prefix="/auth", tags=["auth"])

AUTH_SECRET = os.getenv("AUTH_SECRET", "devsecret")
AUTH_ALGO   = os.getenv("AUTH_ALGO", "HS256")
AUTH_TTL    = int(os.getenv("AUTH_TTL_MINUTES", "720"))  # minutos

class LoginIn(BaseModel):
    username: str
    password: str
    org_id: Optional[int] = None  # opcional: si viene, debe matchear la del usuario

class LoginOut(BaseModel):
    access_token: str
    token_type: str = "bearer"
    exp: int  # epoch seconds

@router.post("/login", response_model=LoginOut)
def login(body: LoginIn):
    if not body.username or not body.password:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="username/password requeridos")

    # Validamos credenciales directamente en Postgres usando pgcrypto (crypt)
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id AS user_id, username, org_id, role, is_active
            FROM public.users
            WHERE username = %s
              AND password_hash = crypt(%s, password_hash)
            LIMIT 1
            """,
            (body.username, body.password),
        )
        u = cur.fetchone()

        if not u:
            # Usuario inexistente o password incorrecta
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="credenciales inválidas")

        if not u["is_active"]:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="usuario inactivo")

        db_org_id = u["org_id"]
        if db_org_id is None:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="usuario sin organización asignada")

        # Si el cliente mandó org_id, debe coincidir con la del usuario
        if body.org_id is not None and int(body.org_id) != int(db_org_id):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="org mismatch")

        user_id = int(u["user_id"])
        org_id  = int(db_org_id)
        role    = u.get("role")

    now = datetime.now(timezone.utc)
    exp = now + timedelta(minutes=AUTH_TTL)
    token = jwt.encode(
        {
            "sub": user_id,
            "org_id": org_id,
            "role": role,
            "iat": int(now.timestamp()),
            "exp": int(exp.timestamp()),
        },
        AUTH_SECRET,
        algorithm=AUTH_ALGO,
    )

    return LoginOut(access_token=token, exp=int(exp.timestamp()))
