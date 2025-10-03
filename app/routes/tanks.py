
from fastapi import APIRouter
from app.db import get_conn
from app.schemas import TankConfigOut

router = APIRouter(prefix="/tanks", tags=["tanks"])

@router.get("/config", response_model=list[TankConfigOut])
def list_tanks_with_config():
    sql = '''
    select tank_id, name, low_pct, low_low_pct, high_pct, high_high_pct, updated_by, updated_at
    from public.v_tanks_with_config
    order by tank_id asc
    '''
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()
from fastapi import APIRouter
from app.db import get_conn
from app.schemas import TankConfigOut

router = APIRouter(prefix="/tanks", tags=["tanks"])

@router.get("/config", response_model=list[TankConfigOut])
def list_tanks_with_config():
    sql = """
    select tank_id, name, low_pct, low_low_pct, high_pct, high_high_pct, updated_by, updated_at
    from public.v_tanks_with_config
    order by tank_id asc
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()
