import os
import sys
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from app.db import get_conn

# Rutas existentes
from app.routes.tanks import router as tanks_router
from app.routes.pumps import router as pumps_router
from app.routes.ingest import router as ingest_router
from app.routes.arduino_controler import router as arduino_router
from app.routes.infraestructura import router as infraestructura_router

# >>> NUEVO: importamos las rutas KPI
from app.routes.kpi import router as kpi_router   # 👈 👈 👈

# ===== Logging simple =====
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)

app = FastAPI(
    title="Backend MIN API",
    version=(os.getenv("RENDER_GIT_COMMIT", "")[:8] or None),
)

# ===== CORS y GZIP =====
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
    expose_headers=["*"],
    max_age=3600,
)
app.add_middleware(GZipMiddleware, minimum_size=1024)

# ===== Health =====
@app.get("/")
def root():
    return {
        "ok": True,
        "service": "Backend MIN API",
        "docs": "/docs",
        "health": "/health",
        "health_db": "/health/db",
    }

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/health/db")
def health_db():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("select 1")
        cur.fetchone()
    return {"ok": True, "db": "up"}

# ===== Rutas que realmente usamos =====
app.include_router(tanks_router)            
app.include_router(pumps_router)            
app.include_router(ingest_router)           
app.include_router(arduino_router)          
app.include_router(infraestructura_router)

# >>> NUEVO: montamos KPI (usa las vistas v_pumps_with_status, v_tanks_with_config, etc.)
app.include_router(kpi_router)               # 👈 👈 👈  /kpi/*
