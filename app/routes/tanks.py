# app/routes/tanks.py
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
      level_pct,        -- último nivel (de v_tank_latest)
      age_sec,          -- antigüedad de la última lectura (segundos)
      online,           -- true/false según umbral de 60s
      alarma            -- (puede venir NULL si la vista aún no la tiene)
    from public.v_tanks_with_config
    order by tank_id
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    def compute_alarm(level_pct, low_low, low, high, high_high):
        # Devuelve "normal" | "alerta" | "critico"
        if level_pct is None:
            return "normal"
        # defaults por si faltan en la fila
        low_low   = float(low_low)    if low_low    is not None else 10.0
        low       = float(low)        if low        is not None else 25.0
        high      = float(high)       if high       is not None else 80.0
        high_high = float(high_high)  if high_high  is not None else 90.0
        x = float(level_pct)
        if x <= low_low or x >= high_high: return "critico"
        if x <= low     or x >= high:      return "alerta"
        return "normal"

    out = []
    for r in rows:
        alarm_txt = r.get("alarma")
        if alarm_txt is None:
            alarm_txt = compute_alarm(
                r.get("level_pct"),
                r.get("low_low_pct"),
                r.get("low_pct"),
                r.get("high_pct"),
                r.get("high_high_pct"),
            )

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

            "level_pct":      float(r["level_pct"]) if r.get("level_pct") is not None else None,
            "age_sec":        int(r["age_sec"])     if r.get("age_sec")   is not None else None,
            "online":         bool(r["online"])     if r.get("online")    is not None else None,

            # Solo texto
            "alarma":         str(alarm_txt),
        })
    return out
