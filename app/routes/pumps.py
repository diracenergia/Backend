from fastapi import APIRouter
from app.db import get_conn
from psycopg.rows import dict_row

router = APIRouter(prefix="/pumps", tags=["pumps"])

@router.get("/config")
def list_pumps_config():
    sql = """
    select
      pump_id,
      name,
      location_id,
      location_name,
      low_pct,
      low_low_pct,
      high_pct,
      high_high_pct,
      updated_by,
      updated_at
    from public.v_pumps_with_config
    order by pump_id
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    out = []
    for r in rows:
        out.append({
            "pump_id":        r["pump_id"],
            "name":           r["name"],
            "location_id":    r["location_id"],
            "location_name":  r["location_name"],
            "low_pct":        float(r["low_pct"])        if r["low_pct"]        is not None else None,
            "low_low_pct":    float(r["low_low_pct"])    if r["low_low_pct"]    is not None else None,
            "high_pct":       float(r["high_pct"])       if r["high_pct"]       is not None else None,
            "high_high_pct":  float(r["high_high_pct"])  if r["high_high_pct"]  is not None else None,
            "updated_by":     r["updated_by"],
            "updated_at":     r["updated_at"],  # ISO se serializa ok
        })
    return out
