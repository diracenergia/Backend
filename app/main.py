# app/main.py
from __future__ import annotations

import os
import sys
import time
import logging
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

# ---- starlette ClientDisconnect (compat) ----
try:
    from starlette.requests import ClientDisconnect  # Starlette recientes
except Exception:
    try:
        from starlette.exceptions import ClientDisconnect  # fallback
    except Exception:  # último fallback
        class ClientDisconnect(Exception):
            pass

import anyio

# =========================
# LOGGING
# =========================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("ops")

# =========================
# APP
# =========================
APP_TITLE = os.getenv("APP_TITLE", "SCADA API")
APP_VERSION = os.getenv("APP_VERSION") or os.getenv("RENDER_GIT_COMMIT", "")[:8] or None
app = FastAPI(title=APP_TITLE, version=APP_VERSION)

# =========================
# MIDDLEWARES
# =========================
class LoggingMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self._log = logging.getLogger("ops.http")

    async def dispatch(self, request: Request, call_next):
        t0 = time.time()
        hdr = request.headers
        safe_headers = {
            "origin": hdr.get("origin"),
            "x-org-id": hdr.get("x-org-id"),
            "authorization-present": bool(hdr.get("authorization")),
        }
        self._log.info(f"[REQ] {request.method} {request.url.path} {safe_headers}")
        try:
            response = await call_next(request)
            dt = (time.time() - t0) * 1000
            self._log.info(f"[RES] {request.method} {request.url.path} status={response.status_code} {dt:.1f}ms")
            return response
        except (ClientDisconnect, anyio.EndOfStream):
            self._log.info(f"[DISCONNECT] {request.method} {request.url.path}")
            return Response(status_code=499)
        except Exception:
            self._log.exception(f"[ERR] {request.method} {request.url.path}")
            raise

# CORS sin credenciales -> ACAO: *
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Fuerza headers CORS incluso en errores
class ForceCorsStarMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        try:
            response = await call_next(request)
        except Exception as e:
            response = JSONResponse(status_code=500, content={"detail": f"internal error: {e.__class__.__name__}"})
        h = response.headers
        h.setdefault("Access-Control-Allow-Origin", "*")
        h.setdefault("Access-Control-Allow-Methods", "*")
        req_hdrs = request.headers.get("access-control-request-headers")
        h.setdefault("Access-Control-Allow-Headers", req_hdrs or "*")
        return response

app.add_middleware(ForceCorsStarMiddleware)
app.add_middleware(GZipMiddleware, minimum_size=1024)
app.add_middleware(LoggingMiddleware)

# =========================
# TENANT CONTEXT (liviano)
# =========================
try:
    from app.core.tenancy import tenant_ctx_dep  # setea contexto global por request
except Exception:
    tenant_ctx_dep = None  # tipo: ignore

class TenantContextMiddleware(BaseHTTPMiddleware):
    PUBLIC_PATHS = {"/", "/health", "/db/ping", "/health/db", "/openapi.json", "/docs", "/redoc", "/favicon.ico"}
    PUBLIC_PREFIXES = ("/ui", "/static", "/assets")

    async def dispatch(self, request: Request, call_next):
        if request.method == "OPTIONS":
            return await call_next(request)
        path = request.url.path
        if path in self.PUBLIC_PATHS or any(path.startswith(p) for p in self.PUBLIC_PREFIXES):
            return await call_next(request)

        if tenant_ctx_dep is not None:
            # Derivar org de header o del JWT (sin verificar firma, solo para extraer claim)
            x_org: Optional[str] = request.headers.get("x-org-id")
            auth = request.headers.get("authorization")
            if not x_org and auth:
                try:
                    import jwt  # PyJWT
                    token = auth.split(" ", 1)[1] if " " in auth else auth
                    payload = jwt.decode(token, options={"verify_signature": False})
                    claim_org = payload.get("org_id")
                    if claim_org is not None:
                        x_org = str(claim_org)
                except Exception:
                    pass
            try:
                await tenant_ctx_dep(
                    request,
                    authorization=auth,
                    x_org_id=x_org,
                    x_user_id=request.headers.get("x-user-id"),
                    x_role=request.headers.get("x-role"),
                )
            except HTTPException as e:
                return JSONResponse(status_code=e.status_code, content={"detail": e.detail})
        return await call_next(request)

app.add_middleware(TenantContextMiddleware)

# Catch-all OPTIONS (por si algún proxy corta preflight)
@app.options("/{rest_of_path:path}")
def _catchall_options(rest_of_path: str, request: Request):
    resp = Response(status_code=200)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "*"
    req_hdrs = request.headers.get("access-control-request-headers")
    resp.headers["Access-Control-Allow-Headers"] = req_hdrs or "*"
    return resp

# =========================
# UI estática (opcional)
# =========================
REPO_ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = REPO_ROOT / "web"
if WEB_DIR.exists():
    app.mount("/ui", StaticFiles(directory=str(WEB_DIR), html=True), name="ui")
else:
    print(f"⚠️ /ui deshabilitado: no existe {WEB_DIR}")

# =========================
# ROUTER ÚNICO: Operaciones
# =========================
from app.routes.ops import router as ops_router
app.include_router(ops_router)  # -> GET /ops/overview

# =========================
# HEALTH / PING
# =========================
from app.core.db import get_conn

@app.get("/")
def root():
    return {
        "ok": True,
        "service": APP_TITLE,
        "version": APP_VERSION,
        "docs": "/docs",
        "health": "/health",
        "overview": "/ops/overview",
    }

@app.get("/favicon.ico")
def favicon_noop():
    return {}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/db/ping")
def db_ping():
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("select 1")
            cur.fetchone()
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

# Compat con lo que ya usabas:
@app.get("/health/db")
def health_db():
    return db_ping()
