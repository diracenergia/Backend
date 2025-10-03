# app/routes/operaciones.py
from __future__ import annotations
from typing import List, Dict, Any, Set

import logging
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row

from app.core.db import get_conn
from app.core.security import device_id_dep
from app.core.tenancy import require_org

log = logging.getLogger("rdls.operaciones")

# ────────────────────────────────────────────────────────────────────────────────
# Reuso de helpers desde graph_api (si existen) con fallbacks seguros
# ────────────────────────────────────────────────────────────────────────────────
try:
    # Nota: importamos funciones "privadas" del módulo porque ya las tenés probadas
    from app.routes.graph_api import (
        _set_org,
        _load_nodes_filtered,
        _node_uid,
        _filter_edges_by_nodes,
        _get_layout_map,
    )
except Exception:
    # Fallbacks mínimos para no romper si no existe graph_api
    def _set_org(cur, org_id: int) -> None:
        cur.execute("SELECT set_config('app.org_id', %s, true)", (str(int(org_id)),))

    def _load_nodes_filtered(conn, cur) -> List[Dict[str, Any]]:
        try:
            cur.execute("SELECT * FROM v_asset_nodes ORDER BY type, name;")
            return cur.fetchall() or []
        except Exception:
            return []

    def _node_uid(r: Dict[str, Any]) -> str:
        t = (r.get("type") or "node").lower()
        aid = r.get("asset_id")
        code = r.get("code")
        return f"{t}:{code}" if code else f"{t}_{aid}"

    def _filter_edges_by_nodes(raw_edges: List[Dict[str, Any]], node_keys: Set[str]) -> List[Dict[str, Any]]:
        kept: List[Dict[str, Any]] = []
        for e in raw_edges or []:
            src = (f"{e.get('from_type')}:{e.get('from_code')}"
                   if e.get("from_code") else f"{e.get('from_type')}_{e.get('from_id')}")
            dst = (f"{e.get('to_type')}:{e.get('to_code')}"
                   if e.get("to_code") else f"{e.get('to_type')}_{e.get('to_id')}")
            if src in node_keys and dst in node_keys and (e.get("is_active", True) is True):
                kept.append(e)
        return kept

    def _get_layout_map(org_id: int) -> Dict[str, tuple[float, float]]:
        return {}

# ────────────────────────────────────────────────────────────────────────────────

router = APIRouter(prefix="/operaciones", tags=["operaciones"])

def _as_iso(ts):
    try:
        return ts.isoformat()
    except Exception:
        return str(ts)

def _collect_latest_pumps(cur, org_id: int, pump_ids: List[int]) -> Dict[int, Any]:
    """v_pump_latest si existe; si no, DISTINCT ON de pump_readings."""
    try:
        cur.execute("SELECT pump_id, is_on FROM v_pump_latest WHERE pump_id = ANY(%s)", (pump_ids,))
        rows = cur.fetchall() or []
        if rows:
            return {int(r["pump_id"]): r.get("is_on") for r in rows}
    except Exception:
        cur.connection.rollback()

    cur.execute(
        """
        WITH latest AS (
          SELECT DISTINCT ON (pump_id) pump_id, is_on, ts
          FROM pump_readings
          WHERE org_id = current_setting('app.org_id')::bigint
            AND pump_id = ANY(%s)
          ORDER BY pump_id, ts DESC
        )
        SELECT pump_id, is_on FROM latest
        """,
        (pump_ids,),
    )
    rows = cur.fetchall() or []
    return {int(r["pump_id"]): r.get("is_on") for r in rows}

def _collect_latest_tanks(cur, org_id: int, tank_ids: List[int]) -> Dict[int, Any]:
    """v_tank_latest si existe; si no, DISTINCT ON de tank_readings."""
    try:
        cur.execute("SELECT tank_id, level_percent FROM v_tank_latest WHERE tank_id = ANY(%s)", (tank_ids,))
        rows = cur.fetchall() or []
        if rows:
            return {int(r["tank_id"]): r.get("level_percent") for r in rows}
    except Exception:
        cur.connection.rollback()

    cur.execute(
        """
        WITH latest AS (
          SELECT DISTINCT ON (tank_id) tank_id, level_percent, ts
          FROM tank_readings
          WHERE org_id = current_setting('app.org_id')::bigint
            AND tank_id = ANY(%s)
          ORDER BY tank_id, ts DESC
        )
        SELECT tank_id, level_percent FROM latest
        """,
        (tank_ids,),
    )
    rows = cur.fetchall() or []
    return {int(r["tank_id"]): r.get("level_percent") for r in rows}

def _collect_ops_payload(org_id: int, include_graph: bool = False) -> Dict[str, Any]:
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        _set_org(cur, org_id)

        # 1) Activos presentes (reusa v_asset_nodes y filtros por org/locations)
        raw_nodes = _load_nodes_filtered(conn, cur)

        node_keys: Set[str] = set()
        tank_ids: Set[int] = set()
        pump_ids: Set[int] = set()
        for r in raw_nodes:
            nid = _node_uid(r)
            node_keys.add(nid)
            t = (r.get("type") or "").lower()
            try:
                aid = int(r.get("asset_id"))
            except Exception:
                aid = None
            if t == "tank" and aid is not None:
                tank_ids.add(aid)
            elif t == "pump" and aid is not None:
                pump_ids.add(aid)

        # 2) Últimas lecturas (para tarjetas inmediatas)
        pump_statuses = _collect_latest_pumps(cur, org_id, list(pump_ids)) if pump_ids else {}
        tank_levels   = _collect_latest_tanks(cur, org_id, list(tank_ids)) if tank_ids else {}

        # Umbrales (si hay config), default 20%
        tank_cfg: Dict[int, Dict[str, Any]] = {}
        if tank_ids:
            try:
                cur.execute(
                    """
                    SELECT tank_id, low_pct, high_pct, low_low_pct, high_high_pct
                    FROM public.tank_config WHERE tank_id = ANY(%s)
                    """,
                    (list(tank_ids),),
                )
                for rr in cur.fetchall() or []:
                    tank_cfg[int(rr["tank_id"])] = rr
            except Exception:
                conn.rollback()

        pumps_on_now = sum(1 for v in pump_statuses.values() if bool(v))

        def is_low(tid: int) -> bool:
            lvl = tank_levels.get(tid)
            if lvl is None:
                return False
            low = tank_cfg.get(tid, {}).get("low_pct")
            try:
                thr = float(low) if low is not None else 20.0
                return float(lvl) < thr
            except Exception:
                return False

        tanks_low_now = sum(1 for tid in tank_ids if is_low(tid))

        # 3) Promedios 15m (filtrados a los IDs presentes)
        flow_lpm_avg_15m = None
        pressure_bar_avg_15m = None
        if pump_ids:
            cur.execute(
                """
                SELECT ROUND(AVG(flow_lpm)::numeric, 1)  AS flow_lpm_avg_15m,
                       ROUND(AVG(pressure_bar)::numeric, 2) AS pressure_bar_avg_15m
                FROM pump_readings
                WHERE org_id = current_setting('app.org_id')::bigint
                  AND ts >= NOW() - INTERVAL '15 minutes'
                  AND pump_id = ANY(%s)
                """,
                (list(pump_ids),),
            )
            rr = cur.fetchone() or {}
            flow_lpm_avg_15m = rr.get("flow_lpm_avg_15m")
            pressure_bar_avg_15m = rr.get("pressure_bar_avg_15m")

        # 4) Alarmas activas (si existe la tabla)
        active_alarms = 0
        try:
            cur.execute(
                "SELECT COALESCE(COUNT(*),0) AS c FROM alarms WHERE org_id = current_setting('app.org_id')::bigint AND active = true"
            )
            active_alarms = int((cur.fetchone() or {}).get("c", 0))
        except Exception:
            conn.rollback()

        # 5) Series 24h
        tank_ts, tank_vals = [], []
        if tank_ids:
            cur.execute(
                """
                SELECT date_trunc('hour', ts) AS bucket,
                       ROUND(AVG(level_percent)::numeric, 2) AS level_percent
                FROM tank_readings
                WHERE org_id = current_setting('app.org_id')::bigint
                  AND ts >= NOW() - INTERVAL '24 hours'
                  AND tank_id = ANY(%s)
                GROUP BY 1
                ORDER BY 1
                """,
                (list(tank_ids),),
            )
            rows = cur.fetchall() or []
            tank_ts = [_as_iso(r["bucket"]) for r in rows]
            tank_vals = [float(r["level_percent"]) if r["level_percent"] is not None else None for r in rows]

        pumps_ts, pumps_vals = [], []
        if pump_ids:
            cur.execute(
                """
                SELECT date_trunc('hour', ts) AS bucket,
                       SUM(CASE WHEN is_on THEN 1 ELSE 0 END) AS count_on
                FROM pump_readings
                WHERE org_id = current_setting('app.org_id')::bigint
                  AND ts >= NOW() - INTERVAL '24 hours'
                  AND pump_id = ANY(%s)
                GROUP BY 1
                ORDER BY 1
                """,
                (list(pump_ids),),
            )
            rows = cur.fetchall() or []
            pumps_ts = [_as_iso(r["bucket"]) for r in rows]
            pumps_vals = [int(r["count_on"] or 0) for r in rows]

        payload: Dict[str, Any] = {
            "cards": {
                "pumps_on_now": int(pumps_on_now or 0),
                "tanks_low_now": int(tanks_low_now or 0),
                "flow_lpm_avg_15m": float(flow_lpm_avg_15m) if flow_lpm_avg_15m is not None else None,
                "pressure_bar_avg_15m": float(pressure_bar_avg_15m) if pressure_bar_avg_15m is not None else None,
                "active_alarms": int(active_alarms or 0),
            },
            "series": {
                "tank_level_24h": {"timestamps": tank_ts,   "level_percent": tank_vals},
                "pumps_on_24h":   {"timestamps": pumps_ts,  "count_on":      pumps_vals},
            },
        }

        if include_graph:
            # Edges filtradas por nodos presentes
            try:
                cur.execute("SELECT * FROM v_topology_edges WHERE is_active ORDER BY id")
                all_edges = cur.fetchall() or []
                kept_edges = _filter_edges_by_nodes(all_edges, node_keys)
            except Exception:
                conn.rollback()
                kept_edges = []

            layout_map = _get_layout_map(org_id)

            nodes: List[Dict[str, Any]] = []
            for r in raw_nodes:
                t = r.get("type")
                if not t:
                    continue
                nid = _node_uid(r)
                node: Dict[str, Any] = {"id": nid, "type": t, "name": r.get("name")}
                if t == "tank":
                    tid = r.get("asset_id")
                    node["level"] = tank_levels.get(tid)
                    cfg = tank_cfg.get(int(tid or -1), {})
                    node.update({
                        "low_pct": cfg.get("low_pct"),
                        "high_pct": cfg.get("high_pct"),
                        "low_low_pct": cfg.get("low_low_pct"),
                        "high_high_pct": cfg.get("high_high_pct"),
                    })
                elif t == "pump":
                    pid = r.get("asset_id")
                    node["status"] = bool(pump_statuses.get(pid, False))
                    rk = r.get("rated_kw")
                    if rk is not None:
                        node["kW"] = rk
                if nid in layout_map:
                    x, y = layout_map[nid]
                    node["x"], node["y"] = x, y
                nodes.append(node)

            edges: List[str] = []
            for e in kept_edges:
                src = (f"{e.get('from_type')}:{e.get('from_code')}"
                       if e.get("from_code") else f"{e.get('from_type')}_{e.get('from_id')}")
                dst = (f"{e.get('to_type')}:{e.get('to_code')}"
                       if e.get("to_code") else f"{e.get('to_type')}_{e.get('to_id')}")
                edges.append(f"{src}>{dst}")

            payload["graph"] = {"nodes": nodes, "edges": edges}

        return payload

@router.get("", summary="KPIs Operaciones (tarjetas + series 24h)")
def get_operaciones(
    with_graph: bool = Query(False, description="Incluir graph (nodes/edges)"),
    _dev=Depends(device_id_dep),
    _org_id: int = Depends(require_org),
):
    """
    Respuesta para la página de Operaciones en una sola llamada.
    - Siempre devuelve: cards + series (24h)
    - Opcional: graph (nodes/edges) si with_graph=true
    """
    try:
        data = _collect_ops_payload(_org_id, include_graph=with_graph)
        return JSONResponse(
            content=data,
            headers={
                "Cache-Control": "no-store, max-age=0",
                "Access-Control-Expose-Headers": "ETag",
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        log.exception("[operaciones] GET failed: %s", e)
        raise HTTPException(500, f"operaciones failed: {e}")
