# app/repos/audit.py  (agregar)
from typing import Optional, Any, Dict, List
from psycopg.rows import dict_row
from app.core.db import get_conn

_TABLE = "public.audit_events"
_COLS = ("id","ts","user","role","action","asset","details","result",
         "domain","asset_type","asset_id","code","severity","state")

def list_audit(
    asset_type: Optional[str] = None,
    asset_id: Optional[int] = None,
    code: Optional[str] = None,
    state: Optional[str] = None,
    since: Optional[str] = None,   # ISO-8601 o 'YYYY-MM-DD'
    until: Optional[str] = None,   # idem
    limit: int = 100,
) -> List[Dict[str, Any]]:
    sql = f"SELECT {','.join(_COLS)} FROM {_TABLE} WHERE 1=1"
    params: List[Any] = []

    if asset_type:
        sql += " AND asset_type = %s"; params.append(asset_type)
    if asset_id is not None:
        sql += " AND asset_id = %s"; params.append(asset_id)
    if code:
        sql += " AND code = %s"; params.append(code)
    if state:
        sql += " AND state = %s"; params.append(state)
    if since:
        sql += " AND ts >= %s"; params.append(since)
    if until:
        sql += " AND ts < %s"; params.append(until)

    sql += " ORDER BY ts DESC, id DESC LIMIT %s"
    params.append(limit)

    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, tuple(params))
        return cur.fetchall()
