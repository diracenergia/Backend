# app/repos/tanks.py
from typing import Any, Dict, List, Optional, Sequence
from datetime import datetime
from psycopg.rows import dict_row
from psycopg import sql

from app.core.db import get_conn

# =======================
# Constantes de columnas
# =======================
TANK_COLS: Sequence[str] = (
    "id", "user_id", "name", "location_text", "created_at",
    "material", "fluid", "install_year", "capacity_m3", "height_m", "diameter_m",
)

READING_COLS: Sequence[str] = (
    "id", "tank_id", "ts", "level_percent", "volume_l",
    "temperature_c", "device_id", "raw_json",
)

# =======================
# Tanks (metadatos)
# =======================
def list_tanks(user_id: Optional[int] = None) -> List[Dict[str, Any]]:
    base = f"""
        SELECT {",".join(TANK_COLS)}
        FROM public.tanks
        {{where}}
        ORDER BY id;
    """
    where = "WHERE user_id = %s" if user_id is not None else ""
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        if user_id is not None:
            cur.execute(base.format(where=where), (user_id,))
        else:
            cur.execute(base.format(where=where))
        return cur.fetchall()

def get_tank(tank_id: int) -> Dict[str, Any]:
    sql_q = f"""
        SELECT {",".join(TANK_COLS)}
        FROM public.tanks
        WHERE id = %s;
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_q, (tank_id,))
        return cur.fetchone() or {}

def create_tank(data: Dict[str, Any]) -> Dict[str, Any]:
    sql_q = f"""
        INSERT INTO public.tanks
            (user_id, name, location_text, material, fluid,
             install_year, capacity_m3, height_m, diameter_m)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING {",".join(TANK_COLS)};
    """
    params = (
        data.get("user_id"),
        data["name"],
        data.get("location_text"),
        data.get("material"),
        data.get("fluid"),
        data.get("install_year"),
        data.get("capacity_m3"),
        data.get("height_m"),
        data.get("diameter_m"),
    )
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_q, params)
        conn.commit()
        return cur.fetchone() or {}

def update_tank(tank_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
    sql_q = f"""
        UPDATE public.tanks SET
          name         = COALESCE(%s, name),
          location_text= COALESCE(%s, location_text),
          material     = COALESCE(%s, material),
          fluid        = COALESCE(%s, fluid),
          install_year = COALESCE(%s, install_year),
          capacity_m3  = COALESCE(%s, capacity_m3),
          height_m     = COALESCE(%s, height_m),
          diameter_m   = COALESCE(%s, diameter_m)
        WHERE id = %s
        RETURNING {",".join(TANK_COLS)};
    """
    params = (
        data.get("name"),
        data.get("location_text"),
        data.get("material"),
        data.get("fluid"),
        data.get("install_year"),
        data.get("capacity_m3"),
        data.get("height_m"),
        data.get("diameter_m"),
        tank_id,
    )
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_q, params)
        conn.commit()
        return cur.fetchone() or {}

# =======================
# Configs (tank_config)
# =======================
def get_tank_config(tank_id: int) -> Dict[str, Any]:
    sql_q = """
        SELECT tank_id, low_pct, low_low_pct, high_pct, high_high_pct, updated_by, updated_at
        FROM public.tank_config
        WHERE tank_id = %s;
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_q, (tank_id,))
        return cur.fetchone() or {}

def upsert_tank_config(
    tank_id: int,
    low_pct: Optional[float] = None,
    low_low_pct: Optional[float] = None,
    high_pct: Optional[float] = None,
    high_high_pct: Optional[float] = None,
    updated_by: Optional[int] = None,
) -> Dict[str, Any]:
    """
    UPSERT respetando valores existentes si vienen como NULL (COALESCE).
    """
    sql_q = """
        INSERT INTO public.tank_config
            (tank_id, low_pct, low_low_pct, high_pct, high_high_pct, updated_by)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (tank_id) DO UPDATE SET
            low_pct       = COALESCE(EXCLUDED.low_pct,       public.tank_config.low_pct),
            low_low_pct   = COALESCE(EXCLUDED.low_low_pct,   public.tank_config.low_low_pct),
            high_pct      = COALESCE(EXCLUDED.high_pct,      public.tank_config.high_pct),
            high_high_pct = COALESCE(EXCLUDED.high_high_pct, public.tank_config.high_high_pct),
            updated_by    = COALESCE(EXCLUDED.updated_by,    public.tank_config.updated_by),
            updated_at    = now()
        RETURNING tank_id, low_pct, low_low_pct, high_pct, high_high_pct, updated_by, updated_at;
    """
    params = (tank_id, low_pct, low_low_pct, high_pct, high_high_pct, updated_by)
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_q, params)
        conn.commit()
        return cur.fetchone() or {}

def list_tanks_with_config_view() -> List[Dict[str, Any]]:
    """
    Usa la vista v_tanks_with_config (no trae material/fluid/install_year salvo que
    la hayas extendido). Si querés “ficha técnica” completa, usá list_tanks_with_config().
    """
    sql_q = """
        SELECT id, name, capacity_m3, height_m, diameter_m,
               location_text, created_at, low_pct, low_low_pct, high_pct, high_high_pct
        FROM public.v_tanks_with_config
        ORDER BY id;
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_q)
        return cur.fetchall()

def list_tanks_with_config() -> List[Dict[str, Any]]:
    """
    JOIN directo tanks + tank_config → incluye material, fluid, install_year para la ficha técnica.
    """
    sql_q = """
        SELECT
          t.id, t.name, t.location_text, t.created_at,
          t.capacity_m3, t.height_m, t.diameter_m,
          t.material::text AS material, t.fluid, t.install_year,
          c.low_pct, c.low_low_pct, c.high_pct, c.high_high_pct
        FROM public.tanks t
        LEFT JOIN public.tank_config c ON c.tank_id = t.id
        ORDER BY t.id;
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_q)
        return cur.fetchall()

def get_tank_with_config(tank_id: int) -> Dict[str, Any]:
    """
    Una sola fila de tank + config (útil si querés la ficha técnica de un tanque puntual).
    """
    sql_q = """
        SELECT
          t.id, t.name, t.location_text, t.created_at,
          t.capacity_m3, t.height_m, t.diameter_m,
          t.material::text AS material, t.fluid, t.install_year,
          c.low_pct, c.low_low_pct, c.high_pct, c.high_high_pct
        FROM public.tanks t
        LEFT JOIN public.tank_config c ON c.tank_id = t.id
        WHERE t.id = %s;
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_q, (tank_id,))
        return cur.fetchone() or {}

# =======================
# Lecturas (tank_readings)
# =======================
_ALLOWED_READING_COLS = {
    "tank_id", "level_percent", "ts", "device_id", "volume_l", "temperature_c", "raw_json"
}

def insert_tank_reading(
    tank_id: int,
    level_percent: float,
    *,
    ts: Optional[str] = None,            # ISO-8601 opcional; si None, DB usa default (UTC)
    device_id: Optional[str] = None,     # <-- string
    volume_l: Optional[float] = None,
    temperature_c: Optional[float] = None,
    raw_json: Optional[dict] = None,
) -> Dict[str, Any]:
    """
    Inserta lectura en public.tank_readings. Solo incluye columnas provistas (lista blanca).
    Si 'ts' no se incluye, debe existir DEFAULT en la columna (NOW() AT TIME ZONE 'UTC').
    """
    cols: List[str] = ["tank_id", "level_percent"]
    vals: List[Any] = [tank_id, level_percent]

    maybe = {
        "ts": ts,
        "device_id": device_id,
        "volume_l": volume_l,
        "temperature_c": temperature_c,
        "raw_json": raw_json,
    }
    for k, v in maybe.items():
        if v is not None and k in _ALLOWED_READING_COLS:
            cols.append(k)
            vals.append(v)

    placeholders = ",".join(["%s"] * len(vals))
    sql_q = f"""
        INSERT INTO public.tank_readings ({",".join(cols)})
        VALUES ({placeholders})
        RETURNING {",".join(READING_COLS)};
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_q, tuple(vals))
        conn.commit()
        return cur.fetchone() or {}

def latest_tank_row(tank_id: int) -> Dict[str, Any]:
    """
    Última lectura por tiempo (ts DESC) y como desempate id DESC.
    Calcula volume_l al LEER si no fue medido (usando capacity_m3 del tanque).
    """
    sql_q = """
        SELECT
          r.id, r.tank_id, r.ts, r.level_percent, r.temperature_c, r.device_id, r.raw_json,
          COALESCE(
            r.volume_l,
            CASE
              WHEN t.capacity_m3 IS NOT NULL THEN (r.level_percent * (t.capacity_m3 * 1000.0) / 100.0)
              ELSE NULL
            END
          ) AS volume_l,
          CASE
            WHEN r.volume_l IS NOT NULL THEN 'measured'
            WHEN t.capacity_m3 IS NOT NULL THEN 'computed'
            ELSE NULL
          END AS volume_source
        FROM public.tank_readings r
        LEFT JOIN public.tanks t ON t.id = r.tank_id
        WHERE r.tank_id = %s
        ORDER BY r.ts DESC, r.id DESC
        LIMIT 1;
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_q, (tank_id,))
        return cur.fetchone() or {}

def history_tank_rows(
    tank_id: int,
    date_from: Optional[str] = None,  # 'YYYY-MM-DD' o ISO 8601
    date_to: Optional[str] = None,
    limit: int = 500,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """
    Historial con volume_l calculado al LEER si no fue medido (capacity_m3 del tanque).
    """
    base = """
        SELECT
          r.id, r.tank_id, r.ts, r.level_percent, r.temperature_c, r.device_id, r.raw_json,
          COALESCE(
            r.volume_l,
            CASE
              WHEN t.capacity_m3 IS NOT NULL THEN (r.level_percent * (t.capacity_m3 * 1000.0) / 100.0)
              ELSE NULL
            END
          ) AS volume_l,
          CASE
            WHEN r.volume_l IS NOT NULL THEN 'measured'
            WHEN t.capacity_m3 IS NOT NULL THEN 'computed'
            ELSE NULL
          END AS volume_source
        FROM public.tank_readings r
        LEFT JOIN public.tanks t ON t.id = r.tank_id
        WHERE r.tank_id = %s
    """
    params: List[Any] = [tank_id]
    if date_from:
        base += " AND r.ts >= %s"
        params.append(date_from)
    if date_to:
        base += " AND r.ts < %s"
        params.append(date_to)
    base += " ORDER BY r.ts DESC, r.id DESC LIMIT %s OFFSET %s;"
    params.extend([limit, offset])

    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(base, tuple(params))
        return cur.fetchall()

# --- Extra: capacidad del tanque ---
def get_tank_capacity_m3(tank_id: int) -> Optional[float]:
    sql_q = "SELECT capacity_m3 FROM public.tanks WHERE id = %s;"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql_q, (tank_id,))
        row = cur.fetchone()
        if not row:
            return None
        cap = row[0]
        return float(cap) if cap is not None else None
