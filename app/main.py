# app/main.py
import os
import sys
import time
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException, Body, Request
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.middleware.cors import CORSMiddleware

# === ClientDisconnect compatible con distintas versiones de Starlette
try:
    from starlette.requests import ClientDisconnect
except Exception:
    try:
        from starlette.exceptions import ClientDisconnect
    except Exception:  # último fallback
        class ClientDisconnect(Exception):
            pass

import anyio

# ===== Routers de la app (sin alarmas ni poller) =====
from app.routes.control import control_router
from app.routes.kpi import router as kpi_router
from app.routes.graph_api import router as graph_router
from app.routes.locations import router as locations_router

from app.routes.ingest import router as ingest_tank_router
from app.routes.latest import router as latest_tank_router
from app.routes.history import router as history_tank_router
from app.routes.configs import router as configs_tank_router
from app.routes.commands_tanks import router as commands_tank_router

from app.routes.ingest_pump import router as ingest_pump_router
from app.routes.latest_pump import router as latest_pump_router
from app.routes.history_pump import router as history_pump_router
from app.routes.configs_pump import router as configs_pump_router
from app.routes.commands_pumps import router as commands_pump_router

from app.routes.audit import router as audit_router
from app.ws import router as ws_router
from app.routes.live_view import viz_router

from app.auth.router import router as auth_router
from app.auth.deps import conn_with_rls  # noqa: F401

# ===== LOGGING GLOBAL =====
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("rdls")
logger.info(f"[boot] LOG_LEVEL={LOG_LEVEL}")

# ===== Config centralizada (opcional) =====
try:
    from app.core.config import settings  # opcional
except Exception:
    settings = None
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass


def _get_env(name: str, default: str = "") -> str:
    if settings and hasattr(settings, name):
        return str(getattr(settings, name))
    return os.getenv(name, default)


APP_TITLE = _get_env("APP_TITLE", "ESP32 Tank/Pump API")
APP_VERSION = _get_env("APP_VERSION", "") or _get_env("RENDER_GIT_COMMIT", "")[:8]
app = FastAPI(title=APP_TITLE, version=APP_VERSION or None)

# ===== Middleware de LOG =====
class LoggingMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self._log = logging.getLogger("rdls.http")

    async def dispatch(self, request: Request, call_next):
        t0 = time.time()
        hdr = request.headers
        safe_headers = {
            "origin": hdr.get("origin"),
            "x-org-id": hdr.get("x-org-id"),
            "x-device-id": hdr.get("x-device-id"),
            "x-api-key-present": bool(hdr.get("x-api-key")),
            "authorization-present": bool(hdr.get("authorization")),
            "content-type": hdr.get("content-type"),
        }

        body_text = None
        if (request.url.path, request.method) in {("/ingest/tank", "POST"), ("/ingest/pump", "POST")}:
            try:
                body_bytes = await request.body()
                body_text = body_bytes.decode("utf-8")[:2000]
            except Exception as e:
                body_text = f"<no-body: {e}>"

        self._log.info(f"[REQ] {request.method} {request.url.path} {safe_headers} body={body_text}")
        try:
            response = await call_next(request)
            dt = (time.time() - t0) * 1000
            self._log.info(f"[RES] {request.method} {request.url.path} status={response.status_code} {dt:.1f}ms")
            return response

        except (ClientDisconnect, anyio.EndOfStream):
            self._log.info(f"[DISCONNECT] {request.method} {request.url.path} (client closed connection)")
            return Response(status_code=499)
        except Exception:
            self._log.exception(f"[ERR] {request.method} {request.url.path} crashed")
            raise


# ===== Tenancy (RLS) =====
from app.core.tenancy import tenant_ctx_dep

_PUBLIC_PATHS = {
    "/", "/health", "/health/db", "/favicon.ico",
    "/__config", "/openapi.json", "/docs", "/redoc",
}
# NO incluir "/infra" acá para que aplique tenant_ctx_dep
_PUBLIC_PREFIXES = ("/ui", "/static", "/assets", "/ws", "/ingest", "/auth")

class TenantContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Preflight: dejar pasar
        if request.method == "OPTIONS":
            return await call_next(request)

        path = request.url.path
        if path in _PUBLIC_PATHS or any(path.startswith(p) for p in _PUBLIC_PREFIXES):
            return await call_next(request)

        # Derivar org_id del JWT si no vino x-org-id
        auth = request.headers.get("authorization")
        x_org = request.headers.get("x-org-id")
        if not x_org and auth:
            try:
                import jwt
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


# ===== CORS: modo MVP súper permisivo (sin credenciales) =====
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # ACAO: *
    allow_credentials=False,   # clave: con False, el browser acepta '*'
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Forzar headers CORS también en errores 4xx/5xx
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

# ===== Trusted hosts (opcional) =====
_trusted_hosts_raw = _get_env("TRUSTED_HOSTS", "").strip()
TRUSTED_HOSTS = [h.strip() for h in _trusted_hosts_raw.split(",") if h.strip()]
if TRUSTED_HOSTS:
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=TRUSTED_HOSTS)
    print("[TrustedHost] enabled ->", TRUSTED_HOSTS)

# ===== GZip + Logging + Tenant =====
app.add_middleware(GZipMiddleware, minimum_size=1024)
app.add_middleware(LoggingMiddleware)
app.add_middleware(TenantContextMiddleware)

# ===== Catch-all OPTIONS =====
@app.options("/{rest_of_path:path}")
def _catchall_options(rest_of_path: str, request: Request):
    resp = Response(status_code=200)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "*"
    req_hdrs = request.headers.get("access-control-request-headers")
    resp.headers["Access-Control-Allow-Headers"] = req_hdrs or "*"
    return resp

# ===== UI estática =====
REPO_ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = REPO_ROOT / "web"
if WEB_DIR.exists():
    app.mount("/ui", StaticFiles(directory=str(WEB_DIR), html=True), name="ui")
else:
    print(f"⚠️ /ui deshabilitado: no existe {WEB_DIR}")

# ===== Routers (sin alarmas ni diag_listener ni poller) =====
# (si tu front pide /alarms, dejamos un stub para no romper)
@app.get("/alarms")
def alarms_stub(active: bool | None = None):
    return []  # stub vacío (sin ir a DB)

# Resto de routers
app.include_router(ingest_tank_router)
app.include_router(latest_tank_router)
app.include_router(history_tank_router)
app.include_router(configs_tank_router)
app.include_router(commands_tank_router)

app.include_router(ingest_pump_router)
app.include_router(latest_pump_router)
app.include_router(history_pump_router)
app.include_router(configs_pump_router)
app.include_router(commands_pumps_router)

try:
    from app.routes.tanks import router as tanks_router
    app.include_router(tanks_router)
except Exception as e:
    print(f"⚠️ tanks router no disponible: {e}")

app.include_router(audit_router)
app.include_router(graph_router, prefix="/infra", tags=["infra-graph"])
app.include_router(locations_router)
app.include_router(ws_router)
app.include_router(kpi_router)
app.include_router(auth_router, prefix="")
app.include_router(viz_router)

# ===== Utilitarios =====
from app.core.db import get_conn

@app.get("/")
def root():
    return {
        "ok": True,
        "service": APP_TITLE,
        "version": APP_VERSION or None,
        "docs": "/docs",
        "health": "/health",
    }

@app.get("/favicon.ico")
def favicon_noop():
    return {}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/health/db")
def health_db():
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return {"ok": True, "db": "up"}
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")

@app.get("/__config")
def cfg_echo():
    return {
        "cors": {"mode": "star_no_credentials", "allow_origin": "*"},
        "trusted_hosts": TRUSTED_HOSTS or None,
        "version": APP_VERSION or None,
    }
