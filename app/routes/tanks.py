from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, Query
from psycopg import OperationalError, errors as psy_errors
from psycopg.rows import dict_row

from app.core.db import get_conn
from app.core.security import device_id_dep
from app.core.tenancy import require_org

router = APIRouter(prefix="/tanks", tags=["tanks"])

@router.get("/config")
def tanks_config(user_id: Optional[int] = Query(default=None), _=Depends(device_id_dep)) -> List[Dict[str, Any]]:
    org_id = require_org()
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            # 🔑 Seteamos GUC para que RLS deje ver filas
            cur.execute("select set_config('app.org_id', %s, true)", (str(int(org_id)),))

            cur.execute(
                """
                SELECT
                  t.id    AS tank_id,
                  t.name  AS tank_name,
                  t.org_id,
                  t.capacity_liters,
                  t.capacity_m3,
                  tc.low_pct, tc.low_low_pct, tc.high_pct, tc.high_high_pct,
                  al.location_id,
                  l.code  AS location_code,
                  l.name  AS location_name
                FROM public.tanks t
                LEFT JOIN public.tank_config tc
                  ON tc.tank_id = t.id
                LEFT JOIN public.asset_locations al
                  ON al.asset_type = 'tank' AND al.asset_id = t.id
                LEFT JOIN public.locations l
                  ON l.id = al.location_id
                WHERE t.org_id = %s
                ORDER BY t.id
                """,
                (org_id,),
            )
            return cur.fetchall()
    except (OperationalError, psy_errors.AdminShutdown, psy_errors.CannotConnectNow, TimeoutError):
        return []
    except Exception:
        return []
