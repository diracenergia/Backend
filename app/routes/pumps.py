from fastapi import APIRouter
from app.db import get_conn
from app.schemas import PumpConfigOut

router = APIRouter(prefix="/pumps", tags=["pumps"])

@router.get("/config", response_model=list[PumpConfigOut])
def list_pumps_with_config():
    sql = """
    select pump_id, name, low_pct, low_low_pct, high_pct, high_high_pct, updated_by, updated_at
    from public.v_pumps_with_config
    order by pump_id asc
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()
