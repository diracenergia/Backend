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

# === Import robusto para ClientDisconnect (Starlette moderno -> requests; viejo -> exceptions)
try:
    from starlette.requests import ClientDisconnect  # Starlette >= ~0.14 en adelante
except Exception:
    try:
        from starlette.exceptions import ClientDisconnect  # fallback p/ versiones antiguas
    except Exception:  # último fallback: definimos la clase para evitar NameError
        class ClientDisconnect(Exception):
            pass

import anyio

# Routers varios
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

from app.routes.alarms import router as alarms_router
from app.routes.audit import router as audit_router
from app.routes.diag_listener import router as diag_listener_router
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

# ===== Config centralizada =====
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
            # Cliente abortó la conexión (abort() en fetch, navegación, etc.)
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
    "/__alarm_poller_status", "/__which_alarms_eval", "/__which_alarm_events",
}
# ⚠️ Importante: NO incluir "/infra" acá para que aplique el tenant_ctx_dep a esos endpoints
_PUBLIC_PREFIXES = ("/ui", "/static", "/assets", "/ws", "/ingest", "/auth")


class TenantContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Preflight CORS: dejar pasar
        if request.method == "OPTIONS":
            return await call_next(request)

        path = request.url.path
        if path in _PUBLIC_PATHS or any(path.startswith(p) for p in _PUBLIC_PREFIXES):
            return await call_next(request)

        try:
            await tenant_ctx_dep(
                request,
                authorization=request.headers.get("authorization"),
                x_org_id=request.headers.get("x-org-id"),
                x_user_id=request.headers.get("x-user-id"),
                x_role=request.headers.get("x-role"),
            )
        except HTTPException as e:
            return JSONResponse(status_code=e.status_code, content={"detail": e.detail})

        return await call_next(request)


# ===== Trusted hosts (lista y flag; el add_middleware va más abajo) =====
_trusted_hosts_raw = _get_env("TRUSTED_HOSTS", "").strip()
TRUSTED_HOSTS = [h.strip() for h in _trusted_hosts_raw.split(",") if h.strip()]

# ===== CORS (debe ser el MÁS EXTERNO) =====
def _env_bool(name: str, default: bool = False) -> bool:
    v = (_get_env(name, "1" if default else "0") or "").strip().lower()
    return v in ("1", "true", "t", "yes", "y")

_raw = _get_env("CORS_ALLOW_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173").strip()
_origin_regex = _get_env("CORS_ALLOW_ORIGIN_REGEX", "").strip() or None
allow_credentials = _env_bool("CORS_ALLOW_CREDENTIALS", True)

if _raw == "*":
    # Con credenciales, NO se puede usar '*' → usá regex o lista explícita
    allowed_origins = []  # vacío si vas a usar regex
    if allow_credentials and not _origin_regex:
        # Acepta localhost/127.0.0.1 en cualquier puerto 51xx (ajustá a gusto)
        _origin_regex = r"^https?://(localhost|127\.0\.0\.1):51\d{2}$"
else:
    allowed_origins = [o.strip() for o in _raw.split(",") if o.strip()]

# Middleware CORS primero (outermost)
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,          # lista explícita
    allow_origin_regex=_origin_regex,       # o regex, si lo definiste
    allow_credentials=allow_credentials,    # <- lee de env
    allow_methods=["*"],
    allow_headers=["*"],
)

# Derivados para /__config
ALLOW_ALL_ORIGINS = (_raw == "*")
ALLOWED_ORIGINS = allowed_origins
ALLOW_CREDENTIALS = allow_credentials

# ===== Resto de middlewares (orden recomendado) =====
if TRUSTED_HOSTS:
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=TRUSTED_HOSTS)
    print("[TrustedHost] enabled ->", TRUSTED_HOSTS)

app.add_middleware(GZipMiddleware, minimum_size=1024)
app.add_middleware(LoggingMiddleware)
app.add_middleware(TenantContextMiddleware)

# ===== UI estática =====
REPO_ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = REPO_ROOT / "web"
if WEB_DIR.exists():
    app.mount("/ui", StaticFiles(directory=str(WEB_DIR), html=True), name="ui")
else:
    print(f"⚠️ /ui deshabilitado: no existe {WEB_DIR}")

# ===== Routers =====
app.include_router(diag_listener_router)

app.include_router(ingest_tank_router)
app.include_router(latest_tank_router)
app.include_router(history_tank_router)
app.include_router(configs_tank_router)
app.include_router(commands_tank_router)

app.include_router(ingest_pump_router)
app.include_router(latest_pump_router)
app.include_router(history_pump_router)
app.include_router(configs_pump_router)
app.include_router(commands_pump_router)

try:
    from app.routes.tanks import router as tanks_router
    app.include_router(tanks_router)
except Exception as e:
    print(f"⚠️ tanks router no disponible: {e}")

app.include_router(alarms_router)
app.include_router(audit_router)

# Infra graph bajo /infra (pasa por TenantContextMiddleware)
app.include_router(graph_router, prefix="/infra", tags=["infra-graph"])
app.include_router(locations_router)

app.include_router(ws_router)
app.include_router(kpi_router)
app.include_router(auth_router, prefix="")  # /auth/...
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

# ===== Alarm Poller opcional =====
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


@app.get("/__alarm_poller_status")
def poller_status():
    try:
        from app.services import alarm_poller as ap
    except Exception as e:
        return {"alive": False, "error": f"import_error: {e}"}
    alive = bool(getattr(ap, "_thread", None) and getattr(ap._thread, "is_alive", lambda: False)())
    return {
        "alive": alive,
        "batch": getattr(ap, "BATCH", None),
        "sleep_empty": getattr(ap, "SLEEP_EMPTY", None),
        "sleep_busy": getattr(ap, "SLEEP_BUSY", None),
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
            "notify_src_preview": preview,
        }
    except Exception as e:
        return {"error": str(e)}


@app.post("/__diag_publish")
def __diag_publish(payload: dict = Body(...)):
    try:
        from app.services import alarm_events
        alarm_events._notify(payload)
        return {"ok": True, "published": payload}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"publish failed: {e}")
