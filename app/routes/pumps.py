from typing import List, Optional, Dict, Any
from psycopg.rows import dict_row
from app.core.db import get_conn

def list_pumps_with_config() -> List[Dict[str, Any]]:
    sql = """
        select
          pump_id, name, location_id, location_name,
          low_pct, low_low_pct, high_pct, high_high_pct,
          updated_by, updated_at
        from public.v_pumps_with_config
        order by pump_id
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        return cur.fetchall()

def get_pump_config(pump_id: int) -> Optional[Dict[str, Any]]:
    sql = """
        select
          pump_id, name, location_id, location_name,
          low_pct, low_low_pct, high_pct, high_high_pct,
          updated_by, updated_at
        from public.v_pumps_with_config
        where pump_id = %s
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (pump_id,))
        return cur.fetchone()

def upsert_pump_config(
    pump_id: int,
    low_pct: Optional[float],
    low_low_pct: Optional[float],
    high_pct: Optional[float],
    high_high_pct: Optional[float],
    updated_by: Optional[str],
) -> Dict[str, Any]:
    sql_upsert = """
        insert into public.pump_configs (pump_id, low_pct, low_low_pct, high_pct, high_high_pct, updated_by)
        values (%s, %s, %s, %s, %s, %s)
        on conflict (pump_id) do update
        set low_pct = excluded.low_pct,
            low_low_pct = excluded.low_low_pct,
            high_pct = excluded.high_pct,
            high_high_pct = excluded.high_high_pct,
            updated_by = excluded.updated_by,
            updated_at = now()
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_upsert, (pump_id, low_pct, low_low_pct, high_pct, high_high_pct, updated_by))
        conn.commit()

    return get_pump_config(pump_id) or {
        "pump_id": pump_id,
        "name": None,
        "location_id": None,
        "location_name": None,
        "low_pct": low_pct,
        "low_low_pct": low_low_pct,
        "high_pct": high_pct,
        "high_high_pct": high_high_pct,
        "updated_by": updated_by,
        "updated_at": None,
    }
