# app/routes/kpi.py
from __future__ import annotations
from fastapi import APIRouter, Query, HTTPException
from typing import Dict, Any, List
from psycopg.rows import dict_row
from app.core.db import get_conn

router = APIRouter(prefix="/kpi", tags=["kpi"])

WINDOWS = {"24h": "24 hours", "7d": "7 days", "30d": "30 days"}
def _window_to_interval(window: str) -> str: return WINDOWS.get(window, "7 days")

@router.get("/overview")
def kpi_overview(loc_id: int, window: str = Query("7d", pattern="^(24h|7d|30d)$")) -> Dict[str, Any]:
    win = _window_to_interval(window)
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        # 1) Resumen por ubicación (30d)
        cur.execute("SELECT * FROM public.v_location_summary_30d WHERE location_id=%s;", (loc_id,))
        summary = cur.fetchone()
        if not summary:
            raise HTTPException(404, "location not found")

        # 2) Activos en la ubicación
        cur.execute("""
            SELECT type AS asset_type, asset_id, name, code
            FROM public.v_asset_nodes_loc
            WHERE location_id=%s
            ORDER BY asset_type, name;
        """, (loc_id,))
        assets_rows = cur.fetchall()
        assets = {"tanks": [], "pumps": [], "valves": [], "manifolds": []}
        tank_ids, pump_ids = [], []
        for r in assets_rows:
            item = {"id": int(r["asset_id"]), "name": r["name"], "code": r["code"]}
            assets[r["asset_type"] + "s"].append(item) if r["asset_type"] in ["valve","manifold"] else None
            if r["asset_type"] == "tank": assets["tanks"].append(item); tank_ids.append(item["id"])
            if r["asset_type"] == "pump": assets["pumps"].append(item); pump_ids.append(item["id"])

        # 3) Últimas lecturas (usa v_*_latest_full)
        latest = {"tanks": [], "pumps": []}
        if tank_ids:
            cur.execute("SELECT * FROM public.v_tank_latest_full WHERE tank_id = ANY(%s) ORDER BY tank_id;", (tank_ids,))
            latest["tanks"] = [dict(r) for r in cur.fetchall()]
        if pump_ids:
            cur.execute("""
                SELECT p.id AS pump_id, p.name AS pump_name, p.rated_kw, v.ts, v.is_on, v.flow_lpm, v.pressure_bar,
                       v.voltage_v, v.current_a, v.control_mode, v.manual_lockout, v.raw_json, v.has_data
                FROM public.pumps p
                LEFT JOIN public.v_pump_latest_full v ON v.pump_id = p.id
                WHERE p.id = ANY(%s)
                ORDER BY p.id;
            """, (pump_ids,))
            latest["pumps"] = [dict(r) for r in cur.fetchall()]

        # 4) Series de tiempo (ventana seleccionada)
        timeseries = {"tanks": {}, "pumps": {}}
        if tank_ids:
            cur.execute(f"""
                SELECT t.id AS tank_id, tr.ts, tr.level_percent, tr.volume_l, tr.temperature_c
                FROM public.tanks t
                LEFT JOIN public.tank_readings tr ON tr.tank_id=t.id AND tr.ts >= (now() - INTERVAL '{win}')
                WHERE t.id = ANY(%s)
                ORDER BY t.id, tr.ts;
            """, (tank_ids,))
            tmp = {}
            for r in cur.fetchall():
                tid = int(r["tank_id"]); d = tmp.setdefault(tid, {"timestamps": [], "level_percent": [], "volume_l": [], "temperature_c": []})
                if r["ts"] is not None:
                    d["timestamps"].append(r["ts"].isoformat())
                    d["level_percent"].append(float(r["level_percent"]) if r["level_percent"] is not None else None)
                    d["volume_l"].append(float(r["volume_l"]) if r["volume_l"] is not None else None)
                    d["temperature_c"].append(float(r["temperature_c"]) if r["temperature_c"] is not None else None)
            for tid, d in tmp.items(): timeseries["tanks"][str(tid)] = d

        if pump_ids:
            cur.execute(f"""
                SELECT p.id AS pump_id, pr.ts, pr.is_on
                FROM public.pumps p
                LEFT JOIN public.pump_readings pr ON pr.pump_id=p.id AND pr.ts >= (now() - INTERVAL '{win}')
                WHERE p.id = ANY(%s)
                ORDER BY p.id, pr.ts;
            """, (pump_ids,))
            tmp = {}
            for r in cur.fetchall():
                pid = int(r["pump_id"]); d = tmp.setdefault(pid, {"timestamps": [], "is_on": []})
                if r["ts"] is not None:
                    d["timestamps"].append(r["ts"].isoformat())
                    d["is_on"].append(bool(r["is_on"]) if r["is_on"] is not None else None)
            for pid, d in tmp.items(): timeseries["pumps"][str(pid)] = d

        # 5) Analytics 30d (uptime / energía / nivel medio)
        analytics30d = {"pump_uptime_pct": {}, "pump_kwh_30d": {}, "tank_avg_level_pct_30d": {}}
        if pump_ids:
            cur.execute("SELECT pump_id, uptime_percent FROM public.v_pump_uptime_30d WHERE pump_id = ANY(%s);", (pump_ids,))
            for r in cur.fetchall(): analytics30d["pump_uptime_pct"][str(int(r["pump_id"]))] = float(r["uptime_percent"]) if r["uptime_percent"] is not None else None
            cur.execute("SELECT pump_id, kwh_30d FROM public.v_pump_energy_30d WHERE pump_id = ANY(%s);", (pump_ids,))
            for r in cur.fetchall(): analytics30d["pump_kwh_30d"][str(int(r["pump_id"]))] = float(r["kwh_30d"]) if r["kwh_30d"] is not None else None
        if tank_ids:
            cur.execute("SELECT tank_id, avg_level_pct_30d FROM public.v_tank_level_avg_30d WHERE tank_id = ANY(%s);", (tank_ids,))
            for r in cur.fetchall(): analytics30d["tank_avg_level_pct_30d"][str(int(r["tank_id"]))] = float(r["avg_level_pct_30d"]) if r["avg_level_pct_30d"] is not None else None

        # 6) Topología (completa; si querés filtrar por loc se puede optimizar)
        cur.execute("SELECT * FROM public.v_asset_nodes;")
        nodes = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT * FROM public.v_topology_edges;")
        edges = [dict(r) for r in cur.fetchall()]

        # 7) Alarmas activas en la ubicación
        cur.execute("""
            SELECT a.*
            FROM public.alarms a
            JOIN public.asset_locations al ON al.asset_type=a.asset_type AND al.asset_id=a.asset_id
            WHERE a.is_active IS TRUE AND al.location_id=%s
            ORDER BY a.ts_raised DESC
            LIMIT 200;
        """, (loc_id,))
        alarms = [dict(r) for r in cur.fetchall()]

        return {
            "location": {"id": summary["location_id"], "code": summary["location_code"], "name": summary["location_name"]},
            "summary30d": dict(summary),
            "assets": assets,
            "latest": latest,
            "timeseries": timeseries,
            "analytics30d": analytics30d,
            "topology": {"nodes": nodes, "edges": edges},
            "alarms": alarms,
        }
