# app/repos/alarms.py
from __future__ import annotations
from typing import Optional, Any, Dict
from types import SimpleNamespace as NS
from psycopg.rows import dict_row
from app.core.db import get_conn

ALARM_COLS = (
    "id","asset_type","asset_id","code","severity","message",
    "ts_raised","ts_cleared","ack_by","ts_ack","is_active","extra"
)

def _obj(row: Dict[str, Any]) -> NS:
    return NS(**row)

def get_active(*, asset_type: str, asset_id: int, code: str) -> Optional[NS]:
    """
    Devuelve la alarma ACTIVA más reciente para ese asset+code (o None).
    """
    sql = f"""
      SELECT {','.join(ALARM_COLS)}
        FROM public.alarms
       WHERE asset_type=%s AND asset_id=%s AND code=%s AND is_active=true
       ORDER BY ts_raised DESC NULLS LAST, id DESC
       LIMIT 1;
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (asset_type, asset_id, code))
        row = cur.fetchone()
        return _obj(row) if row else None

def create(
    *, asset_type: str, asset_id: int, code: str,
    severity: str, message: str, ts_raised,  # datetime (UTC)
    is_active: bool = True, extra: Optional[Dict[str, Any]] = None
) -> NS:
    """
    Inserta una alarma (activa por defecto). OJO: tu tabla valida 'severity'
    en minúscula ('critical'/'warning'/'info'); mantenelo en lower-case.
    """
    sql = f"""
      INSERT INTO public.alarms
        (asset_type,asset_id,code,severity,message,ts_raised,is_active,extra)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
      RETURNING {','.join(ALARM_COLS)};
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (asset_type, asset_id, code, severity, message, ts_raised, is_active, extra))
        conn.commit()
        return _obj(cur.fetchone())

def clear(alarm_id: int, *, ts_cleared):
    """
    Marca la alarma como inactiva y setea ts_cleared.
    """
    sql = f"""
      UPDATE public.alarms
         SET is_active=false, ts_cleared=%s
       WHERE id=%s AND is_active=true
      RETURNING {','.join(ALARM_COLS)};
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (ts_cleared, alarm_id))
        conn.commit()
        row = cur.fetchone()
        return _obj(row) if row else None
