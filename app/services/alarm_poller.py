# app/services/alarm_poller.py
"""
Stub del alarm poller (deshabilitado).
- Mantiene la misma interfaz pública para no romper imports/diagnósticos.
- No abre conexiones a DB ni lanza hilos.
"""

from __future__ import annotations
import logging
import os
from typing import Optional

# ===== Logging básico =====
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="ts=%(asctime)s level=%(levelname)s module=%(name)s msg=%(message)s",
)
log = logging.getLogger("alarm-poller")

# ===== API “compatible” con el módulo original =====
__VERSION__ = "stubbed-0.1"

# Variables que tu /__alarm_poller_status podría leer con getattr(...)
_thread: Optional[object] = None
BATCH = 0
SLEEP_EMPTY = 0.0
SLEEP_BUSY = 0.0

def start_alarm_poller():
    """
    No-op: no inicia nada. Deja rastros en log para confirmarlo.
    """
    global _thread
    _thread = None  # explícito: no hay hilo
    log.info("alarm-poller: DISABLED (stub). No se inicia ningún hilo.")

def stop_alarm_poller():
    """
    No-op: no hay hilo que detener.
    """
    log.info("alarm-poller: DISABLED (stub). Nada que detener.")
