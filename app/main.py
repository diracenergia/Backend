# app/main.py
import os
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from fastapi.staticfiles import StaticFiles

# --- Config centralizada con fallback a .env ---
try:
    from app.core.config import settings  # opcional (pydantic settings)
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
_raw = _get_env("CORS_ALLOW_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173").strip()
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

# ===== Routers principales =====
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

# Router opcional: CRUD de metadatos de tanques
try:
    from app.routes.tanks import router as tanks_router
except Exception:
    tanks_router = None

# 🔌 WebSocket telemetry router
from app.ws import router as ws_router

# ===== Montaje de UI estática =====
REPO_ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = REPO_ROOT / "web"
if WEB_DIR.exists():
    app.mount("/ui", StaticFiles(directory=str(WEB_DIR), html=True), name="ui")
else:
    print(f"⚠️ /ui deshabilitado: no existe {WEB_DIR}")

# ===== Incluir Routers =====
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
if tanks_router:
    app.include_router(tanks_router)

# Alarmas / Auditoría
app.include_router(alarms_router)
app.include_router(audit_router)

# 🔧 Routers de test / diagnóstico
try:
    from app.routes.test_telegram import router as test_telegram_router
    app.include_router(test_telegram_router)
except Exception as e:
    print(f"⚠️ test_telegram router no disponible: {e}")

try:
    from app.routes.test_alarm import router as test_alarm_router
    app.include_router(test_alarm_router)
except Exception as e:
    print(f"⚠️ test_alarm router no disponible: {e}")

try:
    from app.routes.debug_alarm import router as debug_alarm_router
    app.include_router(debug_alarm_router)
except Exception as e:
    print(f"⚠️ debug_alarm router no disponible: {e}")

# 🔌 WebSocket
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

# ===== Alarm Listener (LISTEN/NOTIFY → Telegram) =====
try:
    from app.services.alarm_listener import start_alarm_listener, stop_alarm_listener
    _HAS_ALARM_LISTENER = True
except Exception as e:
    print(f"⚠️ alarm-listener no disponible: {e}")
    start_alarm_listener = None
    stop_alarm_listener = None
    _HAS_ALARM_LISTENER = False

@app.on_event("startup")
def _startup_listeners():
    if _HAS_ALARM_LISTENER and callable(start_alarm_listener):
        try:
            start_alarm_listener()
            print("[alarm-listener] started")
        except Exception as e:
            print(f"⚠️ error al iniciar alarm-listener: {e}")

@app.on_event("shutdown")
def _shutdown_listeners():
    if _HAS_ALARM_LISTENER and callable(stop_alarm_listener):
        try:
            stop_alarm_listener()
            print("[alarm-listener] stopped")
        except Exception as e:
            print(f"⚠️ error al detener alarm-listener: {e}")

# ===== Endpoints de diagnóstico del listener =====
@app.get("/__alarm_listener_status")
def listener_status():
    try:
        from app.services import alarm_listener as al
    except Exception as e:
        return {"alive": False, "channel": None, "error": f"import_error: {e}"}

    alive = bool(getattr(al, "_thread", None) and getattr(al._thread, "is_alive", lambda: False)())
    channel = getattr(al, "CHANNEL", None) or getattr(al, "CHAN", None)

    sent_cache = None
    try:
        sent_cache = len(getattr(al, "_last_sent", []))
    except Exception:
        pass

    return {
        "alive": alive,
        "channel": channel,
        "sent_cache": sent_cache,
    }

@app.post("/__alarm_listener_stop")
def listener_stop():
    if _HAS_ALARM_LISTENER and callable(stop_alarm_listener):
        try:
            stop_alarm_listener()
            return {"stopped": True}
        except Exception as e:
            return {"stopped": False, "error": str(e)}
    return {"stopped": False, "error": "listener no disponible"}

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
