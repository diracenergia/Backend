from __future__ import annotations
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import jwt
import bcrypt
from psycopg.rows import dict_row

from app.core.db import get_conn

router = APIRouter(prefix="/auth", tags=["auth"])

AUTH_SECRET = os.getenv("AUTH_SECRET", "devsecret")
AUTH_ALGO   = os.getenv("AUTH_ALGO", "HS256")
AUTH_TTL    = int(os.getenv("AUTH_TTL_MINUTES", "720"))

class LoginIn(BaseModel):
    username: str
    password: str
    org_id: Optional[int] = None

class LoginOut(BaseModel):
    access_token: str
    token_type: str = "bearer"
    exp: int

@router.post("/login", response_model=LoginOut)
def login(body: LoginIn):
    if not body.username or not body.password:
        raise HTTPException(400, "username/password requeridos")

    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "select id as user_id, password_hash, is_active from public.users where username = %s",
            (body.username,)
        )
        row = cur.fetchone()
        if not row or not row["is_active"]:
            raise HTTPException(401, "credenciales inválidas")

        hashed: str = row["password_hash"] or ""
        try:
            ok = bcrypt.checkpw(body.password.encode("utf-8"), hashed.encode("utf-8"))
        except Exception:
            ok = False
        if not ok:
            raise HTTPException(401, "credenciales inválidas")

        user_id = int(row["user_id"])

        # resolver org
        if body.org_id is None:
            cur.execute("""
              select org_id, role
              from public.user_organizations
              where user_id = %s
              order by org_id
              limit 1
            """, (user_id,))
        else:
            cur.execute("""
              select org_id, role
              from public.user_organizations
              where user_id = %s and org_id = %s
              limit 1
            """, (user_id, body.org_id))
        org = cur.fetchone()
        if not org:
            raise HTTPException(403, "usuario sin organización válida")
        org_id = int(org["org_id"]); role = org.get("role")

    now = datetime.now(timezone.utc); exp = now + timedelta(minutes=AUTH_TTL)
    token = jwt.encode({
        "sub": user_id, "org_id": org_id, "role": role,
        "iat": int(now.timestamp()), "exp": int(exp.timestamp()),
    }, AUTH_SECRET, algorithm=AUTH_ALGO)

    return LoginOut(access_token=token, exp=int(exp.timestamp()))
