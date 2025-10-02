# al comienzo del archivo
from fastapi import APIRouter, HTTPException, Query, Header
from psycopg import OperationalError, errors as psy_errors
# ...

@router.get("/locations")
def list_locations():
    try:
        # (tu SQL actual)
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
              WITH pump_counts AS (
                SELECT al.location_id, COUNT(*)::int AS pumps_count
                FROM public.asset_locations al
                WHERE al.asset_type = 'pump'
                GROUP BY al.location_id
              ),
              tank_counts AS (
                SELECT al.location_id, COUNT(*)::int AS tanks_count
                FROM public.asset_locations al
                WHERE al.asset_type = 'tank'
                GROUP BY al.location_id
              )
              SELECT l.id, l.code, l.name,
                     COALESCE(pc.pumps_count,0) AS pumps_count,
                     COALESCE(tc.tanks_count,0) AS tanks_count
              FROM public.locations l
              LEFT JOIN pump_counts pc ON pc.location_id = l.id
              LEFT JOIN tank_counts tc ON tc.location_id = l.id
              ORDER BY l.name
            """)
            return cur.fetchall()
    except (OperationalError, psy_errors.AdminShutdown, psy_errors.CannotConnectNow, TimeoutError):
        return []
    except Exception:
        return []
