# app/repos/pumps.py
from typing import Optional, List, Dict, Any
from psycopg.rows import dict_row
from app.core.db import get_conn

SQL_PUMPS_CONFIG = """
SELECT
  p.id            AS pump_id,
  p.name          AS pump_name,
  p.org_id,
  COALESCE(pc.remote_enabled, p.remote_enabled)              AS remote_enabled,
  COALESCE(pc.drive_type,     p.drive_type::text)            AS drive_type,
  COALESCE(pc.vfd_min_speed_pct,     p.vfd_min_speed_pct)    AS vfd_min_speed_pct,
  COALESCE(pc.vfd_max_speed_pct,     p.vfd_max_speed_pct)    AS vfd_max_speed_pct,
  COALESCE(pc.vfd_default_speed_pct, p.vfd_default_speed_pct) AS vfd_default_speed_pct,
  al.location_id,
  l.code          AS location_code,
  l.name          AS location_name
FROM public.pumps p
LEFT JOIN public.pump_configs pc
  ON pc.pump_id = p.id
  AND (pc.org_id = p.org_id OR pc.org_id IS NULL)
LEFT JOIN public.asset_locations al
  ON al.asset_type = 'pump' AND al.asset_id = p.id
LEFT JOIN public.locations l
  ON l.id = al.location_id
WHERE p.org_id = %s
ORDER BY p.id
"""

def list_pumps_config(org_id: int, user_id: Optional[int] = None) -> List[Dict[str, Any]]:
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(SQL_PUMPS_CONFIG, (org_id,))
        return cur.fetchall()
