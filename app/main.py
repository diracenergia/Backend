# app/main.py
import os
import sys
import time
import json
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from app.routes.control import control_router

# ===== LOGGING GLOBAL =====
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("rdls")
logger.info(f"[boot] LOG_LEVEL={LOG_LEVEL}")

# ===== Routers propios =====
from app.routes.kpi import router as kpi_router

# Infra
from app.routes.graph_api import router as graph_router       # <- lo montamos con prefix="/infra"
from app.routes.locations import router as locations_router   # ya trae prefix="/infra"

# Tanks (ingesta / latest / history / configs / commands)
from app.routes.ingest import router as ingest_tank_router
from app.routes.latest import router as latest_tank_router
from app.routes.history import router as history_tank_router
from app.routes.configs import router as configs_tank_router
from app.routes.commands_tanks import router as commands_tank_router

# Pumps (ingesta / latest / history / configs / commands)
from app.routes.ingest_pump import router as ingest_pump_router
from app.routes.latest_pump import router as latest_pump_router
from app.routes.history_pump import router as history_pump_router
from app.routes.configs_pump import router as configs_pump_router
from app.routes.commands_pumps import router as commands_pump_router

# Alarmas / Auditoría
from app.routes.alarms import router as alarms_router
from app.routes.audit import router as audit_router

# Diag listener
from app.routes.diag_listener import router as diag_listener_router

# WebSocket
from app.ws import router as ws_router

# Visor en vivo (WS /viz/ws y GET /viz/state)
from app.routes.live_view import viz_router

# ===== Config centralizada (pydantic Settings) con fallback a .env =====
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
    """Lee primero de settings (si existe) y si no, del entorno."""
    if settings and hasattr(settings, name):
        return str(getattr(settings, name))
    return os.getenv(name, default)


# ===== App metadata =====
APP_TITLE = _get_env("APP_TITLE", "ESP32 Tank/Pump API")
APP_VERSION = _get_env("APP_VERSION", "") or _get_env("RENDER_GIT_COMMIT", "")[:8]
app = FastAPI(title=APP_TITLE, version=APP_VERSION or None)

# ===== Middleware de LOG de request/response (antes de tenancy para ver TODO) =====
class LoggingMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self._log = logging.getLogger("rdls.http")

    async def dispatch(self, request: Request, call_next):
        t0 = time.time()
        hdr = request.headers

        # headers “seguros” (sin exponer secretos)
        safe_headers = {
            "x-org-id": hdr.get("x-org-id"),
            "x-device-id": hdr.get("x-device-id"),
            "x-api-key-present": bool(hdr.get("x-api-key")),
            "authorization-present": bool(hdr.get("authorization")),
            "content-type": hdr.get("content-type"),
        }

        # Sólo muestreamos body de ingest para no saturar
        body_text = None
        if (request.url.path, request.method) in {
            ("/ingest/tank", "POST"),
            ("/ingest/pump", "POST"),
        }:
            try:
                body_bytes = await request.body()
                body_text = body_bytes.decode("utf-8")[:2000]  # truncado
            except Exception as e:
                body_text = f"<no-body: {e}>"

        self._log.info(f"[REQ] {request.method} {request.url.path} {safe_headers} body={body_text}")

        try:
            response = await call_next(request)
            dt = (time.time() - t0) * 1000
            self._log.info(f"[RES] {request.method} {request.url.path} status={response.status_code} {dt:.1f}ms")
            return response
        except Exception:
            self._log.exception(f"[ERR] {request.method} {request.url.path} crashed")
            raise

app.add_middleware(LoggingMiddleware)

# ===== CORS =====
_raw = _get_env("CORS_ALLOW_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173,http://localhost:5174,http://127.0.0.1:5174,https://frontend-app.onrender.com").strip()

_origin_regex = _get_env("CORS_ALLOW_ORIGIN_REGEX", "").strip()

if _raw == "*":
    ALLOW_ALL_ORIGINS = True
    ALLOWED_ORIGINS = ["*"]
else:
    ALLOW_ALL_ORIGINS = False
    ALLOWED_ORIGINS = [o.strip() for o in _raw.split(",") if o.strip()]

ALLOW_CREDENTIALS = False
ALLOW_METHODS = ["*"]
ALLOW_HEADERS = ["*"]

print("[CORS] allow_all          =", ALLOW_ALL_ORIGINS)
print("[CORS] allow_origins      =", ALLOWED_ORIGINS)
print("[CORS] allow_origin_regex =", _origin_regex or "(none)")
print("[CORS] allow_credentials  =", ALLOW_CREDENTIALS)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_origin_regex=_origin_regex or None,
    allow_credentials=ALLOW_CREDENTIALS,
    allow_methods=ALLOW_METHODS,
    allow_headers=ALLOW_HEADERS,
)

# ===== Trusted hosts (opcional) =====
_trusted_hosts_raw = _get_env("TRUSTED_HOSTS", "").strip()
TRUSTED_HOSTS = [h.strip() for h in _trusted_hosts_raw.split(",") if h.strip()]
if TRUSTED_HOSTS:
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=TRUSTED_HOSTS)
    print("[TrustedHost] enabled ->", TRUSTED_HOSTS)

# ===== Compresión =====
app.add_middleware(GZipMiddleware, minimum_size=1024)

# ===== Contexto multi-tenant (RLS) =====
from app.core.tenancy import tenant_ctx_dep


app.include_router(control_router)

_PUBLIC_PATHS = {
    "/", "/health", "/health/db", "/favicon.ico",
    "/__config", "/openapi.json", "/docs", "/redoc",
    "/__alarm_poller_status", "/__which_alarms_eval", "/__which_alarm_events",
}
_PUBLIC_PREFIXES = ("/ui", "/static", "/assets", "/ws", "/ingest", "/infra")

@app.middleware("http")
async def _tenant_context_middleware(request: Request, call_next):
    # Preflight CORS: dejar pasar
    if request.method == "OPTIONS":
        return await call_next(request)

    path = request.url.path
    if path in _PUBLIC_PATHS or any(path.startswith(p) for p in _PUBLIC_PREFIXES):
        return await call_next(request)

    try:
        # Pasamos headers tal cual
        await tenant_ctx_dep(
            request,
            authorization=request.headers.get("authorization"),
            x_org_id=request.headers.get("x-org-id"),
            x_user_id=request.headers.get("x-user-id"),
            x_role=request.headers.get("x-role"),
        )
    except HTTPException as e:
        logger.warning(f"[tenant] reject {path} -> {e.status_code} {e.detail}")
        return JSONResponse(status_code=e.status_code, content={"detail": e.detail})

    return await call_next(request)

# ===== Montaje de UI estática (/ui) =====
REPO_ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = REPO_ROOT / "web"
if WEB_DIR.exists():
    app.mount("/ui", StaticFiles(directory=str(WEB_DIR), html=True), name="ui")
else:
    print(f"⚠️ /ui deshabilitado: no existe {WEB_DIR}")

# ===== Incluir Routers =====
# Diag listener
app.include_router(diag_listener_router)

# Tanques
app.include_router(ingest_tank_router)
app.include_router(latest_tank_router)
app.include_router(history_tank_router)
app.include_router(configs_tank_router)
app.include_router(commands_tank_router)

# Bombas
app.include_router(ingest_pump_router)
app.include_router(latest_pump_router)
app.include_router(history_pump_router)
app.include_router(configs_pump_router)
app.include_router(commands_pump_router)

# CRUD Tanques (opcional)
try:
    from app.routes.tanks import router as tanks_router
    app.include_router(tanks_router)
except Exception as e:
    print(f"⚠️ tanks router no disponible: {e}")

# Alarmas / Auditoría
app.include_router(alarms_router)
app.include_router(audit_router)

# Infra / Graph API
app.include_router(graph_router, prefix="/infra", tags=["infra-graph"])  # <- clave para /infra/graph
app.include_router(locations_router)  # ya tiene prefix="/infra"

# WebSocket
app.include_router(ws_router)

# KPI
app.include_router(kpi_router)

# Visor en vivo (estado + websocket)
app.include_router(viz_router)

# ===== Endpoints utilitarios =====
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
    # Evita 404s ruidosos del navegador si no hay favicon
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
        "cors": {
            "allow_all": ALLOW_ALL_ORIGINS,
            "allow_origins": ALLOWED_ORIGINS,
            "allow_origin_regex": _origin_regex or None,
            "allow_credentials": ALLOW_CREDENTIALS,
        },
        "trusted_hosts": TRUSTED_HOSTS or None,
        "version": APP_VERSION or None,
    }

@app.get("/__tg_env")
def tg_env():
    token = _get_env("TELEGRAM_BOT_TOKEN", "")
    return {
        "ENABLED": _get_env("TELEGRAM_ENABLED", ""),
        "BOT_head": (token[:8] + "...") if token else "",
        "CHAT": _get_env("TELEGRAM_CHAT_ID", ""),
    }

# ===== Conexión (diagnóstico) =====
try:
    from app.routes.conn import router as conn_router
    app.include_router(conn_router)
except Exception as e:
    print(f"⚠️ conn router no disponible: {e}")

# ===== Alarm Poller (sin LISTEN/NOTIFY) =====
try:
    from app.services.alarm_poller import start_alarm_poller, stop_alarm_poller
    _HAS_ALARM_POLLER = True
except Exception as e:
    print(f"⚠️ alarm-poller no disponible: {e}")
    start_alarm_poller = None
    stop_alarm_poller = None
    _HAS_ALARM_POLLER = False

@app.on_event("startup")
def _startup_listeners():
    if _HAS_ALARM_POLLER and callable(start_alarm_poller):
        try:
            start_alarm_poller()
            print("[alarm-poller] started")
        except Exception as e:
            print(f"⚠️ error al iniciar alarm-poller: {e}")

@app.on_event("shutdown")
def _shutdown_listeners():
    if _HAS_ALARM_POLLER and callable(stop_alarm_poller):
        try:
            stop_alarm_poller()
            print("[alarm-poller] stopped")
        except Exception as e:
            print(f"⚠️ error al detener alarm-poller: {e}")

# ===== Endpoints de diagnóstico del poller =====
@app.get("/__alarm_poller_status")
def poller_status():
    try:
        from app.services import alarm_poller as ap
    except Exception as e:
        return {"alive": False, "error": f"import_error: {e}"}

    alive = bool(getattr(ap, "_thread", None) and getattr(ap._thread, "is_alive", lambda: False)())
    batch = getattr(ap, "BATCH", None)
    sleep_empty = getattr(ap, "SLEEP_EMPTY", None)
    sleep_busy = getattr(ap, "SLEEP_BUSY", None)

    return {
        "alive": alive,
        "batch": batch,
        "sleep_empty": sleep_empty,
        "sleep_busy": sleep_busy,
    }

@app.post("/__alarm_poller_stop")
def poller_stop():
    if _HAS_ALARM_POLLER and callable(stop_alarm_poller):
        try:
            stop_alarm_poller()
            return {"stopped": True}
        except Exception as e:
            return {"stopped": False, "error": str(e)}
    return {"stopped": False, "error": "poller no disponible"}

# ===== Qué versión de alarms_eval está cargada =====
@app.get("/__which_alarms_eval")
def which_alarms_eval():
    import importlib
    try:
        mod = importlib.import_module("app.services.alarms_eval")
        return {
            "file": getattr(mod, "__file__", None),
            "version": getattr(mod, "__VERSION__", None),
            "has_eval": hasattr(mod, "eval_tank_alarm"),
            "is_callable": callable(getattr(mod, "eval_tank_alarm", None)),
        }
    except Exception as e:
        return {"error": str(e)}

# ===== Qué versión de alarm_events está cargada =====
@app.get("/__which_alarm_events")
def which_alarm_events():
    import importlib, inspect
    try:
        mod = importlib.import_module("app.services.alarm_events")
        try:
            src = inspect.getsource(mod._notify)
            uses_pg = "pg_notify(" in src
            preview = src.strip().splitlines()[:5]
        except Exception:
            uses_pg = None
            preview = ["<no source>"]
        return {
            "file": getattr(mod, "__file__", None),
            "version": getattr(mod, "__VERSION__", None),
            "uses_pg_notify": uses_pg,
            "notify_src_preview": preview
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/__diag_publish")
def __diag_publish(payload: dict = Body(...)):
    """
    Empuja un evento a alarm_events._notify(payload).
    Útil para probar el template de Telegram sin depender de otros módulos.
    """
    try:
        from app.services import alarm_events
        alarm_events._notify(payload)
        return {"ok": True, "published": payload}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"publish failed: {e}")
