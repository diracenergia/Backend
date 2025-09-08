# app/routes/conn.py
import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException
from app.core.security import device_id_dep
from app.repos import tanks as repo

# Importamos helpers del WS para consultar presencia en memoria
from app.ws import presence_snapshot  # asegúrate de exponer esta función en ws.py

router = APIRouter(prefix="/tanks", tags=["tanks"])

WS_WARN_SEC = int(os.getenv("WS_WARN_SEC", "30"))
WS_CRIT_SEC = int(os.getenv("WS_CRIT_SEC", "120"))

def _parse_iso(dt: Any) -> Optional[datetime]:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        # asumimos UTC si viene naive
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    try:
        # soporta "Z"
        s = str(dt).replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None

@router.get("/{tank_id}/conn")
def tank_conn(tank_id: int, _=Depends(device_id_dep)):
    """
    Estado de conexión de un tanque:
      - Usa presence del WS (si existe) para 'online' y 'last_seen'
      - Fallback: staleness de la última lectura en DB
      - Tone por thresholds WS_WARN_SEC / WS_CRIT_SEC
    """
    latest = repo.latest_tank_row(tank_id)
    if not latest:
        raise HTTPException(404, "No hay lecturas para este tanque")

    dev: Optional[str] = latest.get("device_id")
    last_seen_dt: Optional[datetime] = None
    online = False

    # 1) Presence en memoria (si hay device_id y está cacheado como online)
    if dev:
        p: Optional[Dict[str, Any]] = presence_snapshot(dev)  # {'online': bool, 'last_seen': iso}
        if p:
            online = bool(p.get("online"))
            last_seen_dt = _parse_iso(p.get("last_seen"))

    # 2) Fallback: si no tenemos last_seen del WS, usamos ts de la última lectura
    if not last_seen_dt:
        last_seen_dt = _parse_iso(latest.get("ts"))

    # 3) Calcular edad (segundos) respecto del ahora (UTC)
    if not last_seen_dt:
        age_sec = 10**9  # sin datos: muy viejo
    else:
        now = datetime.now(timezone.utc)
        if last_seen_dt.tzinfo is None:
            last_seen_dt = last_seen_dt.replace(tzinfo=timezone.utc)
        age_sec = max(0, int((now - last_seen_dt).total_seconds()))

    # 4) Tono por staleness
    tone = "ok" if age_sec < WS_WARN_SEC else "warn" if age_sec < WS_CRIT_SEC else "bad"

    # 5) Si WS no lo marcó online, usamos staleness como heurística
    if not online:
        online = age_sec < WS_CRIT_SEC

    return {
        "device_id": dev,
        "online": bool(online),
        "age_sec": age_sec if age_sec < 10**8 else None,
        "tone": tone,  # "ok" | "warn" | "bad"
    }
