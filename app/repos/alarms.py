# app/repos/alarms.py
from __future__ import annotations
from typing import Optional, Any, Dict
from types import SimpleNamespace as NS

from psycopg.rows import dict_row
from app.core.db import get_conn

# Columnas que vamos a leer/devolver
ALARM_COLS = (
    "id",
    "asset_type",
    "asset_id",
    "code",
    "severity",
    "message",
    "is_active",
    "ts_raised",
    "ts_cleared",
    "extra",         # jsonb nullable
)

def _row_to_obj(row: Dict[str, Any]) -> NS:
    return NS(**row)

def get_by_id(alarm_id: int) -> Optional[NS]:
    sql_q = f"SELECT {','.join(ALARM_COLS)} FROM public.alarms WHERE id = %s;"
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_q, (alarm_id,))
        row = cur.fetchone()
        return _row_to_obj(row) if row else None

def get_active(*, asset_type: str, asset_id: int, code: str) -> Optional[NS]:
    """
    Devuelve la alarma activa más reciente para asset+code (o None).
    Usado por eval_tank_alarm para decidir escalado/limpieza.
    """
    sql_q = f"""
        SELECT {','.join(ALARM_COLS)}
        FROM public.alarms
        WHERE asset_type = %s AND asset_id = %s AND code = %s AND is_active = true
        ORDER BY ts_raised DESC NULLS LAST, id DESC
        LIMIT 1;
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_q, (asset_type, asset_id, code))
        row = cur.fetchone()
        return _row_to_obj(row) if row else None

def create(
    *,
    asset_type: str,
    asset_id: int,
    code: str,
    severity: str,
    message: str,
    ts_raised,                 # datetime (UTC) provisto por el servicio
    is_active: bool = True,
    extra: Optional[Dict[str, Any]] = None,
) -> NS:
    """
    Crea una alarma activa. Devuelve el row como objeto con atributos (a.id, a.code, etc.).
    """
    sql_q = f"""
        INSERT INTO public.alarms
            (asset_type, asset_id, code, severity, message, is_active, ts_raised, extra)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING {','.join(ALARM_COLS)};
    """
    params = (asset_type, asset_id, code, severity, message, is_active, ts_raised, extra)
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_q, params)
        conn.commit()
        row = cur.fetchone()
        return _row_to_obj(row)

def clear(alarm_id: int, *, ts_cleared) -> Optional[NS]:
    """
    Limpia (desactiva) una alarma por id. Devuelve la fila actualizada (o None si no existe).
    """
    sql_q = f"""
        UPDATE public.alarms
           SET is_active = false,
               ts_cleared = %s
         WHERE id = %s AND is_active = true
        RETURNING {','.join(ALARM_COLS)};
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_q, (ts_cleared, alarm_id))
        conn.commit()
        row = cur.fetchone()
        return _row_to_obj(row) if row else None
