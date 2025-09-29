# app/routes/graph_api.py
from __future__ import annotations

from typing import Dict, Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from psycopg.rows import dict_row

from app.core.db import get_conn
from app.core.security import device_id_dep

router = APIRouter()

# -------------------
# Helper: layout cache
# -------------------
def _get_layout_map() -> Dict[str, Dict[str, Any]]:
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT node_uid, x, y, updated_by, updated_at
            FROM public.asset_layouts
            WHERE org_id = current_setting('app.org_id')::bigint
            """
        )
        items = cur.fetchall() or []
        return {r["node_uid"]: dict(r) for r in items}

@router.get("/graph")
def graph_all(_=Depends(device_id_dep)):
    """
    Devuelve:
      {
        "nodes": [
          { "id": "type:code" | "type_<asset_id>", "type": "...", "name": "...", (x,y?) ... }
        ],
        "edges": ["SRC>DST", ...]
      }
    Inyecta (x,y) desde public.asset_layouts si existen para la org del token.
    """
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            # Nodos (ya RLS/filtrados por org en la vista)
            cur.execute("SELECT * FROM v_asset_nodes ORDER BY type, name;")
            raw_nodes = cur.fetchall()

            # Edges (ya RLS)
            cur.execute("SELECT * FROM v_topology_edges WHERE is_active ORDER BY id;")
            raw_edges = cur.fetchall()

            # ---- Config de tanques (filtrado por org via v_asset_nodes)
            cur.execute(
                """
                SELECT tc.tank_id, tc.low_pct, tc.high_pct, tc.low_low_pct, tc.high_high_pct
                FROM public.tank_config tc
                JOIN v_asset_nodes n
                  ON n.type = 'tank' AND n.asset_id = tc.tank_id
                """
            )
            tank_config = {row["tank_id"]: row for row in cur.fetchall()}

            # ---- Última lectura de tanques (filtrado por org via v_asset_nodes)
            cur.execute(
                """
                SELECT v.tank_id, v.level_percent
                FROM v_tank_latest v
                JOIN v_asset_nodes n
                  ON n.type = 'tank' AND n.asset_id = v.tank_id
                """
            )
            tank_levels = {row["tank_id"]: row["level_percent"] for row in cur.fetchall()}

            # ---- Estado de bombas (filtrado por org via v_asset_nodes)
            cur.execute(
                """
                SELECT v.pump_id, v.is_on
                FROM v_pump_latest v
                JOIN v_asset_nodes n
                  ON n.type = 'pump' AND n.asset_id = v.pump_id
                """
            )
            pump_statuses = {row["pump_id"]: row["is_on"] for row in cur.fetchall()}

        # Layouts guardados
        layout_map = _get_layout_map()

        nodes = []
        for r in raw_nodes:
            # ID único (coincide con edges y asset_layouts.node_uid)
            code = r.get("code")
            nid = f'{r["type"]}:{code}' if code else f'{r["type"]}_{r["asset_id"]}'
            node = {"id": nid, "type": r["type"], "name": r["name"]}

            # payload por tipo
            if r["type"] == "tank":
                tank_id = r["asset_id"]
                cfg = tank_config.get(tank_id)
                if cfg:
                    node["low_pct"] = cfg["low_pct"]
                    node["high_pct"] = cfg["high_pct"]
                    node["low_low_pct"] = cfg["low_low_pct"]
                    node["high_high_pct"] = cfg["high_high_pct"]
                node["level"] = tank_levels.get(tank_id, None)

            elif r["type"] == "pump":
                node["status"] = pump_statuses.get(r["asset_id"], False)  # False = apagada
                node["kW"] = r.get("rated_kw")

            elif r["type"] == "valve":
                node["state"] = r.get("valve_state")

            # Posiciones (si existen)
            pos = layout_map.get(nid)
            if pos:
                node["x"] = pos["x"]; node["y"] = pos["y"]

            # Info de localización (las vistas suelen traer: location_id/code/name)
            if "location_id" in r: node["location_id"] = r["location_id"]
            if "location_code" in r: node["location_code"] = r["location_code"]
            if "location_name" in r: node["location_name"] = r["location_name"]

            nodes.append(node)

        # Edges
        edges = []
        for e in raw_edges:
            src = f'{e["from_type"]}:{e.get("from_code")}' if e.get("from_code") else f'{e["from_type"]}_{e["from_id"]}'
            dst = f'{e["to_type"]}:{e.get("to_code")}'     if e.get("to_code")   else f'{e["to_type"]}_{e["to_id"]}'
            edges.append(f"{src}>{dst}")

        return {"nodes": nodes, "edges": edges}
    except Exception as e:
        raise HTTPException(500, f"graph_all failed: {e}")

# --- Endpoints auxiliares opcionales (sin cambios) ---

@router.get("/graph/nodes")
def graph_nodes(_=Depends(device_id_dep)):
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                  type,
                  asset_id AS id,
                  name, code,
                  level_ratio, capacity_liters,
                  pump_status, rated_kw, valve_state,
                  location_id, location_code, location_name
                FROM v_asset_nodes
                ORDER BY type, name;
                """
            )
            return cur.fetchall()
    except Exception as e:
        raise HTTPException(500, f"graph_nodes failed: {e}")

@router.get("/graph/edges")
def graph_edges(_=Depends(device_id_dep)):
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id, from_type, from_id, from_code, to_type, to_id, to_code
                FROM v_topology_edges
                WHERE is_active
                ORDER BY id;
                """
            )
            rows = cur.fetchall() or []
            return [
                {
                    "id": r["id"],
                    "from": f'{r["from_type"]}:{r["from_code"]}' if r.get("from_code") else f'{r["from_type"]}_{r["from_id"]}',
                    "to":   f'{r["to_type"]}:{r["to_code"]}'     if r.get("to_code")   else f'{r["to_type"]}_{r["to_id"]}',
                }
                for r in rows
            ]
    except Exception as e:
        raise HTTPException(500, f"graph_edges failed: {e}")

class NodePos(BaseModel):
    id: str
    x: float
    y: float
    updated_by: Optional[str] = None

@router.post("/layout")
def save_layout(items: List[NodePos], _=Depends(device_id_dep)):
    """
    Guarda posiciones (UPSERT) en public.asset_layouts para la org del token.
    Body: [{ "id": "<node_uid>", "x": <num>, "y": <num>, "updated_by": "..." }, ...]
    """
    if not items:
        return {"ok": True, "saved": 0}

    try:
        with get_conn() as conn, conn.cursor() as cur:
            for it in items:
                cur.execute(
                    """
                    INSERT INTO public.asset_layouts(org_id, node_uid, x, y, updated_by)
                    VALUES (current_setting('app.org_id')::bigint, %s, %s, %s, %s)
                    ON CONFLICT (org_id, node_uid) DO UPDATE
                    SET x = EXCLUDED.x, y = EXCLUDED.y, updated_by = EXCLUDED.updated_by, updated_at = now()
                    """,
                    (it.id, it.x, it.y, it.updated_by),
                )
            conn.commit()
        return {"ok": True, "saved": len(items)}
    except Exception as e:
        raise HTTPException(500, f"save_layout failed: {e}")

@router.get("/layout")
def get_layout(_=Depends(device_id_dep)):
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT node_uid AS id, x, y, updated_by, updated_at
                FROM public.asset_layouts
                WHERE org_id = current_setting('app.org_id')::bigint
                ORDER BY updated_at DESC
                """
            )
            return cur.fetchall()
    except Exception as e:
        raise HTTPException(500, f"get_layout failed: {e}")
