from fastapi import APIRouter
from app.db import get_conn

router = APIRouter(prefix="/tanks", tags=["tanks"])

@router.get("/config")
def list_tanks_with_config():
    sql = """
    select
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
    from public.v_tanks_with_config
    order by tank_id
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        return rows
