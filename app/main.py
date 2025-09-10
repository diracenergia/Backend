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

# >>> CREA LA APP ANTES DE INCLUIR ROUTERS <<<
app = FastAPI(title=APP_TITLE, version=APP_VERSION or None)

# Logs de arranque
print("[CORS] allow_all       =", ALLOW_ALL_ORIGINS)
print("[CORS] allow_origins   =", ALLOWED_ORIGINS)
print("[CORS] allow_origin_regex =", _origin_regex or "(none)")
print("[CORS] allow_credentials =", ALLOW_CREDENTIALS)
if TRUSTED_HOSTS:
    print("[TrustedHost] enabled ->", TRUSTED_HOSTS)

# Middlewares
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

# --- Montaje de UI estática (opcional) ---
REPO_ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = REPO_ROOT / "web"
if WEB_DIR.exists():
    app.mount("/ui", StaticFiles(directory=str(WEB_DIR), html=True), name="ui")
else:
    print(f"⚠️ /ui deshabilitado: no existe {WEB_DIR}")

# --- Routers TANQUES ---
from app.routes.ingest import router as ingest_tank_router
from app.routes.latest import router as latest_tank_router
from app.routes.history import router as history_tank_router
from app.routes.configs import router as configs_tank_router
from app.routes.commands_tanks import router as commands_tank_router

# --- Routers BOMBAS ---
from app.routes.ingest_pump import router as ingest_pump_router
from app.routes.latest_pump import router as latest_pump_router
from app.routes.history_pump import router as history_pump_router
from app.routes.configs_pump import router as configs_pump_router
from app.routes.commands_pumps import router as commands_pump_router

# --- Routers ALARMAS / AUDIT ---
from app.routes.alarms import router as alarms_router
from app.routes.audit import router as audit_router

# --- Router opcional: CRUD de metadatos de tanques (/tanks GET/POST/PUT) ---
try:
    from app.routes.tanks import router as tanks_router
except Exception:
    tanks_router = None

# --- 🔌 WebSocket telemetry router (ruta exacta /ws/telemetry) ---
from app.ws import router as ws_router

# --- 🔹 Router de pruebas (Telegram directo + notify) ---
#  -> Debes tener un archivo app/routes/test_routes.py con __ping_telegram y __test_alarm_notify
from app.routes.test_routes import router as test_routes_router

# --- Conexión (diagnóstico) ---
from app.routes.conn import router as conn_router

# --- Incluir Routers (TANQUES) ---
app.include_router(ingest_tank_router)
app.include_router(latest_tank_router)
app.include_router(history_tank_router)
app.include_router(configs_tank_router)
app.include_router(commands_tank_router)

# --- Incluir Routers (BOMBAS) ---
app.include_router(ingest_pump_router)
app.include_router(latest_pump_router)
app.include_router(history_pump_router)
app.include_router(configs_pump_router)
app.include_router(commands_pump_router)

# --- Incluir Router opcional (CRUD metadatos de tanques) ---
if tanks_router:
    app.include_router(tanks_router)

# --- Incluir Routers (ALARMAS / AUDIT / TEST / CONN / WS) ---
app.include_router(alarms_router)
app.include_router(audit_router)
app.include_router(test_routes_router)   # 👈 aquí están /__ping_telegram y /__test_alarm_notify
app.include_router(conn_router)
app.include_router(ws_router)

# --- Endpoints utilitarios ---
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
    """Pequeño eco de configuración segura para diagnóstico."""
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
