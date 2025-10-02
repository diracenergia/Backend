from typing import List, Dict, Any
from fastapi import APIRouter, Depends
from psycopg import OperationalError, errors as psy_errors
from psycopg.rows import dict_row

from app.core.db import get_conn
from app.core.security import device_id_dep
from app.core.tenancy import require_org

router = APIRouter(prefix="/infra", tags=["infra"])

@router.get("/locations")
def list_locations(_=Depends(device_id_dep)) -> List[Dict[str, Any]]:
    org_id = require_org()
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            # 🔑 Seteamos GUC para RLS (por si hay RLS en assets/locations)
            cur.execute("select set_config('app.org_id', %s, true)", (str(int(org_id)),))

            cur.execute(
                """
                WITH pump_counts AS (
                  SELECT al.location_id, COUNT(*)::int AS pumps_count
                  FROM public.asset_locations al
                  JOIN public.locations l2 ON l2.id = al.location_id
                  WHERE al.asset_type = 'pump' AND l2.org_id = %s
                  GROUP BY al.location_id
                ),
                tank_counts AS (
                  SELECT al.location_id, COUNT(*)::int AS tanks_count
                  FROM public.asset_locations al
                  JOIN public.locations l2 ON l2.id = al.location_id
                  WHERE al.asset_type = 'tank' AND l2.org_id = %s
                  GROUP BY al.location_id
                )
                SELECT
                  l.id, l.code, l.name,
                  COALESCE(pc.pumps_count, 0) AS pumps_count,
                  COALESCE(tc.tanks_count, 0) AS tanks_count
                FROM public.locations l
                LEFT JOIN pump_counts pc ON pc.location_id = l.id
                LEFT JOIN tank_counts tc ON tc.location_id = l.id
                WHERE l.org_id = %s
                ORDER BY l.name
                """,
                (org_id, org_id, org_id),
            )
            return cur.fetchall()
    except (OperationalError, psy_errors.AdminShutdown, psy_errors.CannotConnectNow, TimeoutError):
        return []
    except Exception:
        return []
