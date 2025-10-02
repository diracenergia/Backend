from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, Query
from psycopg import OperationalError, errors as psy_errors
from psycopg.rows import dict_row

from app.core.db import get_conn
from app.core.security import device_id_dep
from app.core.tenancy import require_org

router = APIRouter(prefix="/pumps", tags=["pumps"])

@router.get("/config")
def pumps_config(user_id: Optional[int] = Query(default=None), _=Depends(device_id_dep)) -> List[Dict[str, Any]]:
    org_id = require_org()
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            # 🔑 Seteamos GUC para RLS
            cur.execute("select set_config('app.org_id', %s, true)", (str(int(org_id)),))

            cur.execute(
                """
                SELECT
                  p.id    AS pump_id,
                  p.name  AS pump_name,
                  p.org_id,
                  COALESCE(pc.remote_enabled, p.remote_enabled)              AS remote_enabled,
                  COALESCE(pc.drive_type,     p.drive_type::text)            AS drive_type,
                  COALESCE(pc.vfd_min_speed_pct,     p.vfd_min_speed_pct)    AS vfd_min_speed_pct,
                  COALESCE(pc.vfd_max_speed_pct,     p.vfd_max_speed_pct)    AS vfd_max_speed_pct,
                  COALESCE(pc.vfd_default_speed_pct, p.vfd_default_speed_pct) AS vfd_default_speed_pct,
                  al.location_id,
                  l.code  AS location_code,
                  l.name  AS location_name
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
                """,
                (org_id,),
            )
            return cur.fetchall()
    except (OperationalError, psy_errors.AdminShutdown, psy_errors.CannotConnectNow, TimeoutError):
        return []
    except Exception:
        return []
