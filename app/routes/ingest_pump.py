# app/routes/ingest_pump.py
from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import datetime, timezone
import inspect
import logging

from fastapi import APIRouter, Depends, HTTPException, Request, status
from psycopg.rows import dict_row
from psycopg import Error as PGError

from app.core.security import device_id_dep
from app.core.db import get_conn
from app.schemas.pumps import PumpPayload

# Visor en vivo (si no existe, no rompe)
try:
    from app.routes.live_view import apply_pump_ingest  # actualiza cache para /viz/ws y /viz/state
except Exception:
    def apply_pump_ingest(_: Dict[str, Any]) -> None:
        pass

log = logging.getLogger("rdls.ingest.pump")
router = APIRouter(prefix="/ingest", tags=["ingest"])


# ----------------------------
# Helpers locales
# ----------------------------
def _extract_device_id(auth_obj: Any) -> Optional[int]:
    """
    Extrae device_id desde la dependencia de auth (dict/obj/string).
    Retorna None si no existe o no es convertible.
    """
    raw = auth_obj.get("device_id") if isinstance(auth_obj, dict) else getattr(auth_obj, "device_id", None)
    if raw is None:
        return None
    try:
        s = str(raw).strip()
        return int(s) if s and s.lstrip("-").isdigit() else None
    except Exception:
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _table_columns(schema: str, table: str) -> set[str]:
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        return {r["column_name"] for r in cur.fetchall()}


def _norm_mode(v: Optional[str]) -> str:
    if not v:
        return "manual"
    v = str(v).strip().lower()
    return v if v in ("manual", "auto") else "manual"


def _insert_pump_reading_inline(
    device_id: Optional[int],
    payload: PumpPayload,
    cached_cols: Optional[set[str]] = None,
) -> int:
    """
    Inserta en public.pump_readings SIN GUCs ni triggers.
    - org_id se obtiene de public.pumps(id = payload.pump_id)
    - Inserta solo las columnas que existan realmente
    Devuelve el id de la lectura creada.
    """
    data = payload.model_dump(exclude_none=True)

    pump_id: int = int(data["pump_id"])
    is_on: Optional[bool] = data.get("is_on")
    control_mode: str = _norm_mode(data.get("control_mode"))
    manual_lockout: Optional[bool] = data.get("manual_lockout")
    flow_lpm = data.get("flow_lpm")
    pressure_bar = data.get("pressure_bar")
    voltage_v = data.get("voltage_v")
    current_a = data.get("current_a")
    ts = data.get("ts")  # datetime | None
    extra = data.get("extra")  # dict | None

    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cols = cached_cols or _table_columns("public", "pump_readings")

        insert_cols: list[str] = ["pump_id", "org_id"]
        select_exprs: list[str] = ["%s", "p.org_id"]
        params: list[Any] = [pump_id]

        def add_col(col: str, value: Any, cast: str | None = None):
            if col in cols:
                insert_cols.append(col)
                select_exprs.append("%s" + (f"::{cast}" if cast else ""))
                params.append(value)

        # Campos básicos
        add_col("is_on", is_on)
        add_col("control_mode", control_mode)
        add_col("manual_lockout", manual_lockout)

        # Métricas opcionales
        add_col("flow_lpm", flow_lpm)
        add_col("pressure_bar", pressure_bar)
        add_col("voltage_v", voltage_v)
        add_col("current_a", current_a)

        # Timestamp si la columna 'ts' existe
        add_col("ts", ts)

        # JSONB extra si existe
        if "extra" in cols:
            insert_cols.append("extra")
            select_exprs.append("%s::jsonb")
            params.append(extra if extra is not None else None)

        # device_id si existe
        add_col("device_id", device_id)

        insert_cols_sql = ", ".join(insert_cols)
        select_sql = ", ".join(select_exprs)

        sql = f"""
            INSERT INTO public.pump_readings ({insert_cols_sql})
            SELECT {select_sql}
            FROM public.pumps p
            WHERE p.id = %s
            RETURNING id
        """
        params.append(pump_id)

        try:
            cur.execute(sql, params)
            row = cur.fetchone()
        except PGError as exc:
            # Devolver diagnóstico útil
            primary = getattr(getattr(exc, "diag", None), "message_primary", None)
            detail = getattr(getattr(exc, "diag", None), "message_detail", None)
            msg = primary or detail or str(exc)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"db_error: {msg}",
            ) from exc

        if not row:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"pump_id {pump_id} no existe",
            )

        return int(row["id"])


async def _await_maybe(fn, *args, **kwargs):
    """Permite usar funciones sync/async indistintamente."""
    res = fn(*args, **kwargs)
    if inspect.isawaitable(res):
        return await res
    return res


# ----------------------------
# Rutas
# ----------------------------
@router.post("/pump", status_code=status.HTTP_201_CREATED)
async def ingest_pump(
    payload: PumpPayload,
    auth: Any = Depends(device_id_dep),
    request: Request | None = None,
):
    """
    Inserta una lectura de bomba y publica en el visor en vivo (si está disponible).

    - Body: PumpPayload (ver app.schemas.pumps)
    - Header opcional: X-Device-Id: <int> (según tu device_id_dep)
    - Devuelve: {"ok": true, "reading_id": <int>, "source_ip": "..."}
    """
    device_id = _extract_device_id(auth)

    try:
        client_ip = request.client.host if request and request.client else "unknown"
    except Exception:
        client_ip = "unknown"

    # Persistencia directa (sin repo) para evitar dependencias y GUCs
    try:
        reading_id = await _await_maybe(_insert_pump_reading_inline, device_id, payload)
    except HTTPException:
        raise
    except Exception as exc:
        log.exception("DB insert failed", exc_info=exc)
        # fallback genérico
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="db_error: no se pudo insertar la lectura de bomba",
        ) from exc

    # Publicación best-effort en visor
    try:
        data = payload.model_dump(exclude_none=True)
        if "ts" not in data or data.get("ts") is None:
            data["ts"] = _now_iso()
        publish = {
            "pump_id": data.get("pump_id"),
            "is_on": data.get("is_on"),
            "flow_lpm": data.get("flow_lpm"),
            "pressure_bar": data.get("pressure_bar"),
            "speed_pct": data.get("speed_pct"),  # si tu schema lo trae
            "ts": data.get("ts"),
        }
        apply_pump_ingest(publish)
    except Exception as e:
        log.warning("[viz] apply_pump_ingest failed: %s", e)

    return {"ok": True, "reading_id": reading_id, "source_ip": client_ip}


@router.get("/pump/ping", status_code=status.HTTP_200_OK)
def pump_ping():
    return {"ok": True, "service": "ingest_pump"}
