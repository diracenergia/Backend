from fastapi import APIRouter
from app.db import get_conn

router = APIRouter(prefix="/pumps", tags=["pumps"])

@router.get("/config")
def list_pumps_with_config():
    sql = """
        SELECT pump_id, name,
               location_id, location_name,
               low_pct, low_low_pct, high_pct, high_high_pct,
               updated_by, updated_at
        FROM public.v_pumps_with_config
        ORDER BY pump_id
    """
    cols = ["pump_id","name","location_id","location_name",
            "low_pct","low_low_pct","high_pct","high_high_pct",
            "updated_by","updated_at"]
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        return [dict(zip(cols, r)) for r in rows]
