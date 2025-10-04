# app/routes/pumps.py
from fastapi import APIRouter
from app.db import get_conn
from psycopg.rows import dict_row

router = APIRouter(prefix="/pumps", tags=["pumps"])

@router.get("/config")
def list_pumps_config():
    """
    Devuelve bombas con estado (run/stop) y conectividad (online/age_sec)
    leyendo de public.v_pumps_with_status.
    """
    sql = """
    SELECT
      pump_id,
      name,
      location_id,
      location_name,

      -- estado del relé (último pump_events)
      state,
      latest_event_id,
      event_ts,

      -- conectividad (último pump_heartbeat)
      latest_hb_id,
      hb_ts,
      age_sec,
      online
    FROM public.v_pumps_with_status
    ORDER BY pump_id
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

            # estado relé
            "state":          r.get("state") or "stop",
            "latest_event_id": r.get("latest_event_id"),
            "event_ts":       r.get("event_ts"),

            # heartbeat / conectividad
            "latest_hb_id":   r.get("latest_hb_id"),
            "hb_ts":          r.get("hb_ts"),
            "age_sec":        int(r["age_sec"]) if r.get("age_sec") is not None else None,
            "online":         bool(r["online"]) if r.get("online") is not None else False,
        })
    return out
