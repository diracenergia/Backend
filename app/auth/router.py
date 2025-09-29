from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from jose import jwt
import os, time
import psycopg

SECRET = os.getenv("AUTH_SECRET", "devsecret")  # poné algo fuerte en prod
ALGO   = "HS256"
TTL    = 60 * 60 * 8  # 8h

router = APIRouter(prefix="/auth", tags=["auth"])

class LoginIn(BaseModel):
    username: str
    password: str
    org_id: int | None = None  # si no viene, usamos la primera del usuario

@router.post("/login")
def login(body: LoginIn):
    # abrí conexión como lo hacés en tu proyecto (psycopg3 sync por sencillez)
    with psycopg.connect(os.getenv("DATABASE_URL")) as conn:
        with conn.cursor() as cur:
            # 1) validar credenciales con pgcrypto (crypt)
            cur.execute("""
                select u.id, u.name, u.is_active
                  from public.users u
                 where u.username = %s
                   and u.password_hash = crypt(%s, u.password_hash)
            """, (body.username, body.password))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=401, detail="Usuario o contraseña inválidos")
            user_id, name, is_active = row
            if not is_active:
                raise HTTPException(status_code=403, detail="Usuario deshabilitado")

            # 2) organizaciones y rol
            cur.execute("""
                select org_id, role
                  from public.user_organizations
                 where user_id = %s
                 order by org_id
            """, (user_id,))
            orgs = cur.fetchall()
            if not orgs:
                raise HTTPException(status_code=403, detail="Usuario sin organizaciones")
            if body.org_id:
                # validar que pertenezca
                roles = {o: r for (o, r) in orgs}
                if body.org_id not in roles:
                    raise HTTPException(status_code=403, detail="No pertenece a esa organización")
                org_id, role = body.org_id, roles[body.org_id]
            else:
                org_id, role = orgs[0]

            # 3) token
            now = int(time.time())
            payload = {
                "sub": str(user_id),
                "name": name,
                "org_id": org_id,
                "role": role,
                "iat": now, "exp": now + TTL,
            }
            token = jwt.encode(payload, SECRET, algorithm=ALGO)

            return {
                "access_token": token,
                "token_type": "bearer",
                "user": {"id": user_id, "name": name, "username": body.username},
                "org": {"id": org_id, "role": role},
                "orgs": [{"org_id": o, "role": r} for (o, r) in orgs],
            }
