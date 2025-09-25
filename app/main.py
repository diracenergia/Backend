# app/main.py
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from fastapi.staticfiles import StaticFiles

# --- Routers core (estos deberían existir en tu repo) ---
from app.routes.graph_api import router as graph_router

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

# --- Routers opcionales: los envolvemos en try/except por si no están ---
try:
    from app.routes.diag_listener import router as diag_listener_router
except Exception:
    diag_listener_router = None

try:
    from app.routes.conn import router as conn_router   # /conn/simple, etc.
except Exception:
    conn_router = None

try:
    from app.ws import router as ws_router              # WebSocket
except Exception:
    ws_router = None


# --- Config centralizada con fallback a .env ---
try:
    from app.core.config import settings  # (opcional) Pydantic Settings
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


# ===== CORS =====
_raw = _get_env("CORS_ALLOW_ORIGINS", "http://localhost:5172,http://127.0.0.1:5173").strip()
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

# ===== Trusted hosts (opcional) =====
_trusted_hosts_raw = _get_env("TRUSTED_HOSTS", "").strip()
TRUSTED_HOSTS = [h.strip() for h in _trusted_hosts_raw.split(",") if h.strip()]

APP_TITLE = _get_env("APP_TITLE", "ESP32 Tank/Pump API")
APP_VERSION = _get_env("APP_VERSION", "") or _get_env("RENDER_GIT_COMMIT", "")[:8]

# ===== Crear app (UNA sola vez) =====
app = FastAPI(title=APP_TITLE, version=APP_VERSION or None)

# Logs de arranque
print("[CORS] allow_all          =", ALLOW_ALL_ORIGINS)
print("[CORS] allow_origins      =", ALLOWED_ORIGINS)
print("[CORS] allow_origin_regex =", _origin_regex or "(none)")
print("[CORS] allow_credentials  =", ALLOW_CREDENTIALS)
if TRUSTED_HOSTS:
    print("[TrustedHost] enabled ->", TRUSTED_HOSTS)

# ===== Middlewares =====
if TRUSTED_HOSTS:
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=TRUSTED_HOSTS)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_origin_regex=_origin_regex or None,
    allow_credentials=ALLOW_CREDENTIALS,
    allow_methods=ALLOW_METHODS,
    allow_headers=ALLOW_HEADERS,
)
app.add_middleware(GZipMiddleware, minimum_size=1024)

# ===== UI estática (sirve /ui si existe carpeta web/) =====
REPO_ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = REPO_ROOT / "web"
if WEB_DIR.exists():
    app.mount("/ui", StaticFiles(directory=str(WEB_DIR), html=True), name="ui")
else:
    print(f"⚠️ /ui deshabilitado: no existe {WEB_DIR}")

# ===== Incluir routers =====
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

# Alarmas / Auditoría
app.include_router(alarms_router)
app.include_router(audit_router)

# Infra / Graph API (una sola vez, con prefijo)
app.include_router(graph_router, prefix="/infra")

# Diagnóstico listener (si está): lo dejamos en /infra/debug
if diag_listener_router:
    app.include_router(diag_listener_router, prefix="/infra/debug", tags=["debug"])

# Conexión (presence /conn/simple) si está
if conn_router:
    app.include_router(conn_router)

# WebSocket si está
if ws_router:
    app.include_router(ws_router)

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

# --- DEBUG TELEGRAM inline (evita problemas de import/routers) ---
from app.core.telegram import send_telegram as _send_tg

@app.get("/infra/debug/__tg_env")
def __tg_env_inline():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    return {
        "ENABLED": os.getenv("TELEGRAM_ENABLED", ""),
        "BOT_head": (token[:8] + "...") if token else "",
        "CHAT": os.getenv("TELEGRAM_CHAT_ID", ""),
    }

@app.get("/infra/debug/__ping_telegram")
async def __ping_telegram_inline():
    res = await _send_tg("✅ Telegram OK (inline test)")
    return {"result": res}

# --- Listar rutas publicadas (para verificar deploy) ---
@app.get("/__routes")
def __routes():
    return [
        {
            "path": getattr(r, "path", None),
            "name": getattr(r, "name", None),
            "methods": list(getattr(r, "methods", []) or []),
        }
        for r in app.router.routes
    ]

# ===== Alarm Poller (sin LISTEN/NOTIFY): si existe, lo levantamos =====
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
