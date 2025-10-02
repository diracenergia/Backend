# app/services/alarm_poller.py
from __future__ import annotations
import os, logging

log = logging.getLogger("alarm-poller")

_ENABLED = os.getenv("ALARM_POLLER_ENABLED", "0").lower() in ("1", "true", "yes")

def start_alarm_poller():
    if not _ENABLED:
        log.info("alarm-poller DISABLED by ALARM_POLLER_ENABLED")
        return
    # Si algún día lo reactivás, traé acá la versión “real”.
    log.info("alarm-poller requested to start but feature is disabled in this build.")

def stop_alarm_poller():
    log.info("alarm-poller stop (noop)")
