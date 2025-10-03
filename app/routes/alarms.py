from fastapi import APIRouter, Query
from app.db import get_conn
from app.schemas import Alarm

router = APIRouter(prefix="/alarms", tags=["alarms"])

@router.get("", response_model=list[Alarm])
def list_alarms(active: bool | None = Query(None)):
    base = """
    select id, asset_type, asset_id, code, severity, message, ts_raised, is_active, telegram, tg_notified_at
    from public.alarms
    """
    where = ""
    params: tuple = ()
    if active is not None:
        where = " where is_active = %s"
        params = (active,)

    sql = base + where + " order by ts_raised desc, id desc limit 500"

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()
