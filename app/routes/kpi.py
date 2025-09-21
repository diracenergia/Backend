# app/routes/kpi.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Header, HTTPException, Query
from psycopg.rows import dict_row

from app.core.db import get_conn

router = APIRouter(prefix="/kpi", tags=["kpi"])

# ------------------------------------------------------------
# Ventanas aceptadas para series
# ------------------------------------------------------------
WINDOWS = {"24h": "24 hours", "7d": "7 days", "30d": "30 days"}


def _window_to_interval(window: str) -> str:
    return WINDOWS.get(window, "7 days")


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def _set_org(cur, org_id: Optional[int]) -> None:
    """
    Opcional: fija app.org_id para que las vistas filtren por organización.
    Requiere que tus vistas usen current_setting('app.org_id', true).
    """
    if org_id:
        cur.execute("SELECT set_config('app.org_id', %s, false);", (str(org_id),))


def _get_location_meta(cur, loc_id: int) -> Optional[Dict[str, Any]]:
    cur.execute(
        "SELECT id, code, name FROM public.locations WHERE id=%s LIMIT 1;",
        (loc_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "id": int(row["id"]),
        "code": row["code"],
        "name": row["name"],
    }


# ------------------------------------------------------------
# 1) OVERVIEW — paquete por ubicación (tolerante a falta de datos)
# ------------------------------------------------------------
@router.get("/overview")
def kpi_overview(
    loc_id: int,
    window: str = Query("7d", pattern="^(24h|7d|30d)$"),
    x_org_id: Optional[int] = Header(default=None, convert_underscores=False),
) -> Dict[str, Any]:
    """
    Devuelve el paquete completo para una ubicación:
    - summary30d (si no hay datos: ceros)
    - assets, latest, timeseries, analytics30d, topology, alarms
    Sólo devuelve 404 si la location NO existe.
    """
    win = _window_to_interval(window)

    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        _set_org(cur, x_org_id)

        # Summary (puede no existir si aún no hay lecturas)
        cur.execute(
            "SELECT * FROM public.v_location_summary_30d WHERE location_id=%s;",
            (loc_id,),
        )
        summary = cur.fetchone()

        # Metadatos mínimos de la location
        loc_meta = (
            {
                "id": int(summary["location_id"]),
                "code": summary["location_code"],
                "name": summary["location_name"],
            }
            if summary
            else _get_location_meta(cur, loc_id)
        )
        if not loc_meta:
            raise HTTPException(status_code=404, detail=f"location {loc_id} not found")

        # Activos en la ubicación
        cur.execute(
            """
            SELECT type AS asset_type, asset_id, name, code
            FROM public.v_asset_nodes_loc
            WHERE location_id=%s
            ORDER BY asset_type, name;
            """,
            (loc_id,),
        )
        assets_rows = cur.fetchall()
        assets: Dict[str, Any] = {"tanks": [], "pumps": [], "valves": [], "manifolds": []}
        tank_ids: List[int] = []
        pump_ids: List[int] = []
        for r in assets_rows:
            t = r["asset_type"]
            item = {"id": int(r["asset_id"]), "name": r["name"], "code": r["code"]}
            if t == "tank":
                assets["tanks"].append(item)
                tank_ids.append(item["id"])
            elif t == "pump":
                assets["pumps"].append(item)
                pump_ids.append(item["id"])
            elif t == "valve":
                assets["valves"].append(item)
            elif t == "manifold":
                assets["manifolds"].append(item)

        # Últimas lecturas
        latest: Dict[str, Any] = {"tanks": [], "pumps": []}
        if tank_ids:
            cur.execute(
                "SELECT * FROM public.v_tank_latest_full WHERE tank_id = ANY(%s) ORDER BY tank_id;",
                (tank_ids,),
            )
            latest["tanks"] = [dict(r) for r in cur.fetchall()]
        if pump_ids:
            cur.execute(
                """
                SELECT p.id AS pump_id, p.name AS pump_name, p.rated_kw, v.ts, v.is_on, v.flow_lpm, v.pressure_bar,
                       v.voltage_v, v.current_a, v.control_mode, v.manual_lockout, v.raw_json, v.has_data
                FROM public.pumps p
                LEFT JOIN public.v_pump_latest_full v ON v.pump_id = p.id
                WHERE p.id = ANY(%s)
                ORDER BY p.id;
                """,
                (pump_ids,),
            )
            latest["pumps"] = [dict(r) for r in cur.fetchall()]

        # Series por ventana
        timeseries: Dict[str, Any] = {"tanks": {}, "pumps": {}}
        if tank_ids:
            cur.execute(
                f"""
                SELECT t.id AS tank_id, tr.ts, tr.level_percent, tr.volume_l, tr.temperature_c
                FROM public.tanks t
                LEFT JOIN public.tank_readings tr
                  ON tr.tank_id=t.id AND tr.ts >= (now() - INTERVAL '{win}')
                WHERE t.id = ANY(%s)
                ORDER BY t.id, tr.ts;
                """,
                (tank_ids,),
            )
            tmp_t: Dict[int, Any] = {}
            for r in cur.fetchall():
                tid = int(r["tank_id"])
                d = tmp_t.setdefault(
                    tid,
                    {"timestamps": [], "level_percent": [], "volume_l": [], "temperature_c": []},
                )
                if r["ts"] is not None:
                    d["timestamps"].append(r["ts"].isoformat())
                    d["level_percent"].append(float(r["level_percent"]) if r["level_percent"] is not None else None)
                    d["volume_l"].append(float(r["volume_l"]) if r["volume_l"] is not None else None)
                    d["temperature_c"].append(float(r["temperature_c"]) if r["temperature_c"] is not None else None)
            for tid, d in tmp_t.items():
                timeseries["tanks"][str(tid)] = d

        if pump_ids:
            cur.execute(
                f"""
                SELECT p.id AS pump_id, pr.ts, pr.is_on
                FROM public.pumps p
                LEFT JOIN public.pump_readings pr
                  ON pr.pump_id=p.id AND pr.ts >= (now() - INTERVAL '{win}')
                WHERE p.id = ANY(%s)
                ORDER BY p.id, pr.ts;
                """,
                (pump_ids,),
            )
            tmp_p: Dict[int, Any] = {}
            for r in cur.fetchall():
                pid = int(r["pump_id"])
                d = tmp_p.setdefault(pid, {"timestamps": [], "is_on": []})
                if r["ts"] is not None:
                    d["timestamps"].append(r["ts"].isoformat())
                    d["is_on"].append(bool(r["is_on"]) if r["is_on"] is not None else None)
            for pid, d in tmp_p.items():
                timeseries["pumps"][str(pid)] = d

        # Analytics 30d
        analytics30d: Dict[str, Any] = {
            "pump_uptime_pct": {},
            "pump_kwh_30d": {},
            "tank_avg_level_pct_30d": {},
        }
        if pump_ids:
            cur.execute(
                "SELECT pump_id, uptime_percent FROM public.v_pump_uptime_30d WHERE pump_id = ANY(%s);",
                (pump_ids,),
            )
            for r in cur.fetchall():
                analytics30d["pump_uptime_pct"][str(int(r["pump_id"]))] = (
                    float(r["uptime_percent"]) if r["uptime_percent"] is not None else None
                )
            cur.execute(
                "SELECT pump_id, kwh_30d FROM public.v_pump_energy_30d WHERE pump_id = ANY(%s);",
                (pump_ids,),
            )
            for r in cur.fetchall():
                analytics30d["pump_kwh_30d"][str(int(r["pump_id"]))] = (
                    float(r["kwh_30d"]) if r["kwh_30d"] is not None else None
                )
        if tank_ids:
            cur.execute(
                "SELECT tank_id, avg_level_pct_30d FROM public.v_tank_level_avg_30d WHERE tank_id = ANY(%s);",
                (tank_ids,),
            )
            for r in cur.fetchall():
                analytics30d["tank_avg_level_pct_30d"][str(int(r["tank_id"]))] = (
                    float(r["avg_level_pct_30d"]) if r["avg_level_pct_30d"] is not None else None
                )

        # Topología (completa)
        cur.execute("SELECT * FROM public.v_asset_nodes;")
        nodes = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT * FROM public.v_topology_edges;")
        edges = [dict(r) for r in cur.fetchall()]

        # Alarmas activas en la ubicación
        cur.execute(
            """
            SELECT a.*
            FROM public.alarms a
            JOIN public.asset_locations al
              ON al.asset_type=a.asset_type AND al.asset_id=a.asset_id
            WHERE a.is_active IS TRUE AND al.location_id=%s
            ORDER BY a.ts_raised DESC
            LIMIT 200;
            """,
            (loc_id,),
        )
        alarms = [dict(r) for r in cur.fetchall()]

        # Summary por defecto si no había datos
        summary30d = (
            dict(summary)
            if summary
            else {
                "location_id": loc_meta["id"],
                "location_code": loc_meta["code"],
                "location_name": loc_meta["name"],
                "assets_total": 0,
                "tanks_count": 0,
                "pumps_count": 0,
                "valves_count": 0,
                "manifolds_count": 0,
                "alarms_active": 0,
                "alarms_critical_active": 0,
                "avg_flow_lpm_30d": None,
                "avg_pressure_bar_30d": None,
                "avg_level_pct_30d": None,
                "pump_readings_30d": 0,
                "tank_readings_30d": 0,
            }
        )

        return {
            "location": loc_meta,
            "summary30d": summary30d,
            "assets": assets,
            "latest": latest,
            "timeseries": timeseries,
            "analytics30d": analytics30d,
            "topology": {"nodes": nodes, "edges": edges},
            "alarms": alarms,
        }


# ------------------------------------------------------------
# 2) LOCATIONS — para el combo del front
# ------------------------------------------------------------
@router.get("/locations")
def kpi_locations(
    x_org_id: Optional[int] = Header(default=None, convert_underscores=False),
) -> List[Dict[str, Any]]:
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        _set_org(cur, x_org_id)
        cur.execute("SELECT id, code, name FROM public.locations ORDER BY name;")
        return [dict(r) for r in cur.fetchall()]


# ------------------------------------------------------------
# 3) BY-LOCATION — tabla agregada del widget (con fallback)
# ------------------------------------------------------------
@router.get("/by-location")
def kpi_by_location(
    x_org_id: Optional[int] = Header(default=None, convert_underscores=False),
) -> List[Dict[str, Any]]:
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        _set_org(cur, x_org_id)
        cur.execute(
            """
            SELECT
              v.location_id,
              v.location_code,
              v.location_name,
              COALESCE(v.assets_total, 0)            AS assets_total,
              COALESCE(v.tanks_count, 0)             AS tanks_count,
              COALESCE(v.pumps_count, 0)             AS pumps_count,
              COALESCE(v.valves_count, 0)            AS valves_count,
              COALESCE(v.manifolds_count, 0)         AS manifolds_count,
              COALESCE(v.alarms_active, 0)           AS alarms_active,
              COALESCE(v.alarms_critical_active, 0)  AS alarms_critical_active,
              COALESCE(v.pumps_on_now, 0)            AS pumps_on_now,
              COALESCE(v.kwh_30d, 0)::float8         AS kwh_30d
            FROM public.v_by_location_kpi v
            ORDER BY v.location_name;
            """
        )
        return [dict(r) for r in cur.fetchall()]
