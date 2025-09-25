# app/routes/ingest_pump.py
from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional, Dict

from fastapi import APIRouter, Header, Request, status, HTTPException

from app.schemas.pumps import PumpPayload

from app.repos.pumps import insert_pump_reading

# Visor en vivo (best-effort)
try:
    from app.routes.live_view import apply_pump_ingest  # actualiza cache para /viz/ws y /viz/state
except Exception:
    def apply_pump_ingest(_: Dict[str, Any]) -> None:
        pass

# Para formatear errores de psycopg (diag)
try:
    from psycopg import Error as PGError  # psycopg3
except Exception:
    PGError = Exception  # fallback

log = logging.getLogger("rdls.ingest.pump")
router = APIRouter(prefix="/ingest", tags=["ingest"])


# ----------------------------
# Helpers de logging/diagnóstico
# ----------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _safe_json(obj: Any, max_len: int = 4000) -> str:
    """Convierte a JSON para logs, truncado."""
    try:
        s = json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        s = repr(obj)
    return (s[:max_len] + "…") if len(s) > max_len else s

def _pg_diag(exc: Exception) -> str:
    """Intenta extraer mensaje detallado de psycopg3."""
    if isinstance(exc, PGError):
        d = getattr(exc, "diag", None)
        primary = getattr(d, "message_primary", None)
        detail = getattr(d, "message_detail", None)
        hint = getattr(d, "message_hint", None)
        ctx = getattr(d, "context", None)
        code = getattr(d, "sqlstate", None)
        tbl = getattr(d, "table_name", None)
        col = getattr(d, "column_name", None)
        sch = getattr(d, "schema_name", None)
        con = getattr(d, "constraint_name", None)
        parts = []
        if primary: parts.append(f"primary={primary}")
        if detail:  parts.append(f"detail={detail}")
        if hint:    parts.append(f"hint={hint}")
        if code:    parts.append(f"code={code}")
        if ctx:     parts.append(f"context={ctx}")
        if sch:     parts.append(f"schema={sch}")
        if tbl:     parts.append(f"table={tbl}")
        if col:     parts.append(f"column={col}")
        if con:     parts.append(f"constraint={con}")
        return " | ".join(parts) or repr(exc)
    return repr(exc)

def _hdr_bool(v: Optional[str]) -> bool:
    return bool(v and v.strip())

def _to_int_or_none(v: Optional[str]) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


# ----------------------------
# Rutas
# ----------------------------
@router.post("/pump", status_code=status.HTTP_201_CREATED)
def ingest_pump(
    payload: PumpPayload,
    request: Request,
    # Headers tal cual (sin underscores)
    x_org_id: Optional[str] = Header(default=None, convert_underscores=False),
    x_device_id: Optional[str] = Header(default=None, convert_underscores=False),
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False),
    authorization: Optional[str] = Header(default=None),
):
    """
    Inserta una lectura de bomba en public.pump_readings (vía repositorio) y publica al visor.
    - Headers: X-Org-Id (opcional), X-Device-Id (opcional), X-API-Key (opcional), Authorization (opcional)
    - Body: PumpPayload (validado por Pydantic)
    - Respuesta: {"ok": true, "reading_id": <int>, "source_ip": "..."}
    """
    req_id = str(uuid.uuid4())
    t0 = time.perf_counter()

    # Datos básicos del request para logs
    try:
        client_ip = request.client.host if request and request.client else "unknown"
    except Exception:
        client_ip = "unknown"

    # Parseo “seguro” de headers que nos interesan
    org_id_int = _to_int_or_none(x_org_id)
    device_id_int = _to_int_or_none(x_device_id)

    # Log inicial de request
    safe_headers = {
        "x-org-id": x_org_id,
        "x-device-id": x_device_id,
        "x-api-key-present": _hdr_bool(x_api_key),
        "authorization-present": _hdr_bool(authorization),
        "content-type": request.headers.get("content-type"),
        "req-id": req_id,
        "client-ip": client_ip,
    }
    log.info("[ingest/pump][REQ] %s headers=%s", req_id, _safe_json(safe_headers))

    # Log del payload (ya validado por Pydantic)
    try:
        payload_dict = payload.model_dump(exclude_none=False)
    except Exception:
        payload_dict = {"<payload-repr>": repr(payload)}
    log.debug("[ingest/pump][REQ] %s payload=%s", req_id, _safe_json(payload_dict))

    # Inserción en DB (repositorio)
    t1 = time.perf_counter()
    try:
        reading_id = insert_pump_reading(
            device_id=device_id_int,
            payload=payload,
            org_id=org_id_int,   # 👈 setea app.org_id en la conexión (en el repo) y/o inserta org_id explícito
        )
    except HTTPException as e:
        # Errores HTTP upstream (no debería pasar aquí)
        log.error("[ingest/pump][DB][HTTPEXC] %s status=%s detail=%s", req_id, e.status_code, e.detail)
        raise
    except Exception as exc:
        # Log bien detallado (psycopg diag si aplica)
        diag = _pg_diag(exc)
        log.exception("[ingest/pump][DB][EXC] %s insert failed | diag: %s", req_id, diag, exc_info=exc)
        # Devolver detalle útil para depurar (temporal o dejar así según políticas)
        raise HTTPException(
            status_code=500,
            detail=f"db_error: insert_failed | {diag}"
        ) from exc
    t2 = time.perf_counter()

    # Publicación best-effort al visor en vivo
    try:
        data = payload.model_dump(exclude_none=True)
        data.setdefault("ts", _now_iso())
        publish = {
            "pump_id": data.get("pump_id"),
            "is_on": data.get("is_on"),
            "flow_lpm": data.get("flow_lpm"),
            "pressure_bar": data.get("pressure_bar"),
            "speed_pct": data.get("speed_pct"),
            "ts": data.get("ts"),
        }
        log.debug("[ingest/pump][PUB] %s publish=%s", req_id, _safe_json(publish))
        apply_pump_ingest(publish)
    except Exception as e:
        log.warning("[ingest/pump][PUB][WARN] %s apply_pump_ingest failed: %s", req_id, repr(e))

    # Métricas de tiempo
    dt_insert_ms = (t2 - t1) * 1000.0
    dt_total_ms = (time.perf_counter() - t0) * 1000.0
    log.info(
        "[ingest/pump][OK] %s reading_id=%s insert=%.1fms total=%.1fms client-ip=%s",
        req_id, reading_id, dt_insert_ms, dt_total_ms, client_ip
    )

    return {"ok": True, "reading_id": reading_id, "source_ip": client_ip, "req_id": req_id}


@router.get("/pump/ping", status_code=status.HTTP_200_OK)
def pump_ping():
    return {"ok": True, "service": "ingest_pump"}
