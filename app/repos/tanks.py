# app/repos/tanks.py
from typing import Optional, List, Dict, Any
from psycopg.rows import dict_row
from app.core.db import get_conn

SQL_TANKS_CONFIG = """
SELECT
  t.id            AS tank_id,
  t.name          AS tank_name,
  t.org_id,
  t.capacity_liters,
  t.capacity_m3,
  tc.low_pct,
  tc.low_low_pct,
  tc.high_pct,
  tc.high_high_pct,
  al.location_id,
  l.code          AS location_code,
  l.name          AS location_name
FROM public.tanks t
LEFT JOIN public.tank_config tc
  ON tc.tank_id = t.id
LEFT JOIN public.asset_locations al
  ON al.asset_type = 'tank' AND al.asset_id = t.id
LEFT JOIN public.locations l
  ON l.id = al.location_id
WHERE t.org_id = %s
ORDER BY t.id
"""

def list_tanks_config(org_id: int, user_id: Optional[int] = None) -> List[Dict[str, Any]]:
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(SQL_TANKS_CONFIG, (org_id,))
        return cur.fetchall()
