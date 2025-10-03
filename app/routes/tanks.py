# app/routes/tanks.py
from fastapi import APIRouter
from app.db import get_conn

router = APIRouter(prefix="/tanks", tags=["tanks"])

@router.get("/config")
def list_tanks_config():
    sql = """
    SELECT
      tank_id,
      name,
      location_id,
      location_name,
      low_pct,
      low_low_pct,
      high_pct,
      high_high_pct,
      updated_by,
      updated_at
    FROM v_tanks_with_config
    ORDER BY tank_id;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = [dict(zip([c[0] for c in cur.description], r)) for r in cur.fetchall()]
    return rows
