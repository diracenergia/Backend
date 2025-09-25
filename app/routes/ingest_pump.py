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

# Visor en vivo (best-effort)
try:
    # actualiza cache para /viz/ws y /viz/state
    from app.routes.live_view import apply_pump_ingest
except Exception:
    def apply_pump_ingest(_: Dict[str, Any]) -> None:
        pass

log = logging.getLogger("rdls.ingest.pump")
router = APIRouter(prefix="/ingest", tags=["ingest"])


# ----------------------------
# Helpers
# ----------------------------
def _extract_device_id(auth_obj: Any) -> Optional[int]:
    """
    Intenta extraer device_id desde el objeto de autenticación (dep).
    Acepta dict o objeto con atributo .device_id
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
    """
    Devuelve el set de nombres de columnas reales para (schema, table)
    """
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
    - Incluye org_id SOLO si existe en ambas tablas (pumps y pump_readings).
    - Inserta únicamente las columnas que existan realmente en pump_readings.
    - Si la tabla tiene 'extra' se usa; si no, y existe 'raw_json', se mapea extra -> raw_json.
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
    speed_pct = data.get("speed_pct")  # opcional, si existe en el schema/tabla
    ts = data.get("ts")                # datetime | None
    extra = data.get("extra")          # dict | None

    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cols = cached_cols or _table_columns("public", "pump_readings")
        pump_cols = _table_columns("public", "pumps")

        # SELECT basado en columnas reales
        insert_cols: list[str] = ["pump_id"]
        select_exprs: list[str] = ["%s"]
        params: list[Any] = [pump_id]

        # org_id solo si existe en ambas tablas
        if "org_id" in cols and "org_id" in pump_cols:
            insert_cols.append("org_id")
            select_exprs.append("p.org_id")

        def add_col(col: str, value: Any, cast: str | None = None):
            """
            Agrega la columna al INSERT solo si existe en 'cols'.
            """
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
        add_col("speed_pct", speed_pct)  # si no existe en la tabla, se ignora

        # Timestamp si existe
        add_col("ts", ts)

        # JSONB extra si existe; si no, usar raw_json si está disponible
        if "extra" in cols:
            insert_cols.append("extra")
            select_exprs.append("%s::jsonb")
            params.append(extra if extra is not None else None)
        elif "raw_json" in cols:
            insert_cols.append("raw_json")
            select_exprs.append("%s::jsonb")
            params.append(extra if extra is not None else None)

        # device_id si existe
        add_col("device_id", device_id)

        insert_cols_sql = ", ".join(insert_cols)
        select_sql = ", ".join(select_exprs)

        # Siempre referenciamos public.pumps para:
        #  - tomar p.org_id si aplica
        #  - y validar existencia del pump_id (si no existe, no retorna fila)
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
    res = fn(*args, **kwargs)
    if inspect.isawaitable(res):
        return await res
    return res


# ----------------------------
# Rutas
# ----------------------------
@router.post("/pump", status_code=status.HTTP_201_CREATED, response_model=None)
async def ingest_pump(
    payload: PumpPayload,
    auth: Any = Depends(device_id_dep),
    request: Request = None,   # 👈 OJO: NO usar Union/Optional aquí
):
    """
    Inserta una lectura de bomba y publica en el visor en vivo (si está disponible).

    - Body: PumpPayload (ver app.schemas.pumps)
    - Headers opcionales: X-Device-Id, X-API-Key, X-Org-Id (según tus deps de seguridad)
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
            "speed_pct": data.get("speed_pct"),
            "ts": data.get("ts"),
        }
        apply_pump_ingest(publish)
    except Exception as e:
        log.warning("[viz] apply_pump_ingest failed: %s", e)

    return {"ok": True, "reading_id": reading_id, "source_ip": client_ip}


@router.get("/pump/ping", status_code=status.HTTP_200_OK, response_model=None)
def pump_ping():
    return {"ok": True, "service": "ingest_pump"}
