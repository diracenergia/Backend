from fastapi import APIRouter
from app.db import get_conn
from psycopg.rows import dict_row

router = APIRouter(prefix="/tanks", tags=["tanks"])

@router.get("/config")
def list_tanks_config():
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
      updated_at,
      level_pct            -- << nueva columna desde v_tanks_with_config
    from public.v_tanks_with_config
    order by tank_id
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    out = []
    for r in rows:
        out.append({
            "tank_id":        r["tank_id"],
            "name":           r["name"],
            "location_id":    r["location_id"],
            "location_name":  r["location_name"],
            "low_pct":        float(r["low_pct"])        if r["low_pct"]        is not None else None,
            "low_low_pct":    float(r["low_low_pct"])    if r["low_low_pct"]    is not None else None,
            "high_pct":       float(r["high_pct"])       if r["high_pct"]       is not None else None,
            "high_high_pct":  float(r["high_high_pct"])  if r["high_high_pct"]  is not None else None,
            "updated_by":     r["updated_by"],
            "updated_at":     r["updated_at"],
            "level_pct":      float(r["level_pct"])      if r.get("level_pct")  is not None else None,  # << agregado
        })
    return out
