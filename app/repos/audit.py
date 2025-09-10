# app/repos/audit.py
from __future__ import annotations
from typing import Optional, Any, Dict, List
from datetime import datetime, timezone
from psycopg.rows import dict_row
from app.core.db import get_conn

_TABLE = "public.audit_events"
_COLS = ("id","ts","user","role","action","asset","details","result",
         "domain","asset_type","asset_id","code","severity","state")

def log(
    *, ts: Optional[datetime]=None,
    asset_type: str, asset_id: int,
    code: str, severity: str, state: str,
    details: Optional[Dict[str, Any]] = None,
    user: Optional[str] = None, role: Optional[str] = None,
    action: Optional[str] = None, asset: Optional[str] = None,
    result: Optional[str] = None, domain: str = "AUDIT",
):
    """
    Inserta una fila en audit_events. Los campos no usados quedan NULL.
    """
    ts = ts or datetime.now(timezone.utc)
    sql = f"""
      INSERT INTO {_TABLE}
        (ts, "user", role, action, asset, details, result, domain,
         asset_type, asset_id, code, severity, state)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
      RETURNING {",".join(_COLS)};
    """
    params = (ts, user, role, action, asset, details, result, domain,
              asset_type, asset_id, code, severity, state)
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        conn.commit()
        return cur.fetchone() or {}

def list_recent(limit: int = 100) -> List[Dict[str, Any]]:
    sql = f"SELECT {','.join(_COLS)} FROM {_TABLE} ORDER BY ts DESC, id DESC LIMIT %s;"
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (limit,))
        return cur.fetchall()
