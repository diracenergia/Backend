# --- reemplazar en app/routes/kpi.py ---

from psycopg.errors import UndefinedColumn, UndefinedTable

def _as_float(x): return float(x) if x is not None else None

@router.get("/tanks/latest")
def tanks_latest() -> List[dict]:
    """
    Soporta vista 'v_tanks_with_config' con tank_id o con pump_id por compatibilidad.
    """
    sql = "SELECT * FROM public.v_tanks_with_config"
    rows = fetch_all(sql)

    out = []
    for r in rows:
        # Detectamos el nombre real de la PK en la vista actual
        id_key = "tank_id" if "tank_id" in r else ("pump_id" if "pump_id" in r else None)

        out.append({
            # Normalizamos a tank_id para el front, aunque venga como pump_id
            "tank_id":        r.get("tank_id") if id_key == "tank_id" else r.get("pump_id"),
            "name":           r.get("name"),
            "location_id":    r.get("location_id"),
            "location_name":  r.get("location_name"),

            "low_pct":        _as_float(r.get("low_pct")),
            "low_low_pct":    _as_float(r.get("low_low_pct")),
            "high_pct":       _as_float(r.get("high_pct")),
            "high_high_pct":  _as_float(r.get("high_high_pct")),

            # Campos “operativos”; si tu vista actual no los trae, queda None
            "level_pct":      _as_float(r.get("level_pct")),
            "age_sec":        int(r["age_sec"]) if r.get("age_sec") is not None else None,
            "online":         bool(r["online"]) if r.get("online") is not None else None,
            "alarma":         (str(r["alarma"]) if r.get("alarma") is not None else None),
        })
    return out


@router.get("/tanks/{tank_id}/latest")
def tank_latest(tank_id: int) -> dict:
    """
    Intenta filtrar por tank_id; si la vista en realidad usa pump_id, hace fallback.
    """
    try:
        sql = "SELECT * FROM public.v_tanks_with_config WHERE tank_id = %s"
        r = fetch_one(sql, (tank_id,))
    except UndefinedColumn:
        r = None

    if not r:
        try:
            sql2 = "SELECT * FROM public.v_tanks_with_config WHERE pump_id = %s"
            r = fetch_one(sql2, (tank_id,))
        except UndefinedColumn:
            r = None

    if not r:
        raise HTTPException(status_code=404, detail="Tank not found")

    return {
        "tank_id":        r.get("tank_id", r.get("pump_id")),
        "name":           r.get("name"),
        "location_id":    r.get("location_id"),
        "location_name":  r.get("location_name"),
        "low_pct":        _as_float(r.get("low_pct")),
        "low_low_pct":    _as_float(r.get("low_low_pct")),
        "high_pct":       _as_float(r.get("high_pct")),
        "high_high_pct":  _as_float(r.get("high_high_pct")),
        "level_pct":      _as_float(r.get("level_pct")),
        "age_sec":        int(r["age_sec"]) if r.get("age_sec") is not None else None,
        "online":         bool(r["online"]) if r.get("online") is not None else None,
        "alarma":         (str(r["alarma"]) if r.get("alarma") is not None else None),
    }


@router.get("/tanks/{tank_id}/levels")
def tank_levels_timeseries(
    tank_id: int,
    date_from: datetime = Query(..., alias="from"),
    date_to:   datetime = Query(..., alias="to"),
) -> List[dict]:
    """
    Si no existe la vista v_tank_levels_timeseries, caemos a la query base sobre tank_ingest.
    """
    if date_from >= date_to:
        raise HTTPException(status_code=400, detail="'from' must be before 'to'")

    # 1) Intentamos la vista “bonita”
    try:
        sql_view = """
            SELECT tank_id, tank_name, level_pct, ts
            FROM public.v_tank_levels_timeseries
            WHERE tank_id = %s AND ts >= %s AND ts < %s
            ORDER BY ts ASC
        """
        rows = fetch_all(sql_view, (tank_id, date_from, date_to))
    except UndefinedTable:
        rows = None

    # 2) Fallback a tablas reales si la vista no existe
    if rows is None:
        sql_base = """
            SELECT ti.tank_id,
                   t.name AS tank_name,
                   ti.level_pct,
                   ti.created_at AS ts
            FROM public.tank_ingest ti
            JOIN public.tanks t ON t.id = ti.tank_id
            WHERE ti.tank_id = %s
              AND ti.created_at >= %s
              AND ti.created_at <  %s
            ORDER BY ti.created_at ASC
        """
        rows = fetch_all(sql_base, (tank_id, date_from, date_to))

    for r in rows:
        if r.get("level_pct") is not None:
            r["level_pct"] = float(r["level_pct"])
    return rows
