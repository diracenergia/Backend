from typing import List, Optional, Dict, Any
from psycopg.rows import dict_row
from app.core.db import get_conn

def list_tanks_with_config() -> List[Dict[str, Any]]:
    """
    Lee directamente de v_tanks_with_config para que el front tenga:
      tank_id, name, location_id, location_name, low_pct, low_low_pct, high_pct, high_high_pct, updated_by, updated_at
    """
    sql = """
        select
          tank_id, name, location_id, location_name,
          low_pct, low_low_pct, high_pct, high_high_pct,
          updated_by, updated_at
        from public.v_tanks_with_config
        order by tank_id
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        return cur.fetchall()

def get_tank_config(tank_id: int) -> Optional[Dict[str, Any]]:
    sql = """
        select
          tank_id, name, location_id, location_name,
          low_pct, low_low_pct, high_pct, high_high_pct,
          updated_by, updated_at
        from public.v_tanks_with_config
        where tank_id = %s
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (tank_id,))
        return cur.fetchone()

def upsert_tank_config(
    tank_id: int,
    low_pct: Optional[float],
    low_low_pct: Optional[float],
    high_pct: Optional[float],
    high_high_pct: Optional[float],
    updated_by: Optional[str],
) -> Dict[str, Any]:
    """
    Upsert en tank_configs y luego devolvemos la fila desde la view (con location_*).
    """
    sql_upsert = """
        insert into public.tank_configs (tank_id, low_pct, low_low_pct, high_pct, high_high_pct, updated_by)
        values (%s, %s, %s, %s, %s, %s)
        on conflict (tank_id) do update
        set low_pct = excluded.low_pct,
            low_low_pct = excluded.low_low_pct,
            high_pct = excluded.high_pct,
            high_high_pct = excluded.high_high_pct,
            updated_by = excluded.updated_by,
            updated_at = now()
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_upsert, (tank_id, low_pct, low_low_pct, high_pct, high_high_pct, updated_by))
        conn.commit()

    # devolvemos vista
    return get_tank_config(tank_id) or {
        "tank_id": tank_id,
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
