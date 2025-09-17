# app/routes/graph_api.py
from fastapi import APIRouter, HTTPException, Request
from psycopg.rows import dict_row
from app.core.db import get_conn

router = APIRouter()

def _set_org(cur, org_id: str | None):
    if not org_id:
        return
    try:
        cur.execute("SELECT set_config('app.org_id', %s, true)", (str(org_id),))
    except Exception:
        # No tiramos abajo el request si falla el set_config
        pass

@router.get("/graph")
def graph_all(request: Request):
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            _set_org(cur, request.headers.get("X-Org-Id"))

            cur.execute("SELECT * FROM v_asset_nodes ORDER BY type, name;")
            raw_nodes = cur.fetchall()

            cur.execute("SELECT * FROM v_topology_edges WHERE is_active ORDER BY id;")
            raw_edges = cur.fetchall()

        nodes = []
        for r in raw_nodes:
            nid = f'{r["type"]}:{r.get("code")}' if r.get("code") else f'{r["type"]}_{r["asset_id"]}'
            node = {"id": nid, "type": r["type"], "name": r["name"]}
            if r["type"] == "tank":
                node["level"]    = r.get("level_ratio")
                node["capacity"] = r.get("capacity_liters")
            elif r["type"] == "pump":
                node["status"] = r.get("pump_status")
                node["kW"]     = r.get("rated_kw")
            elif r["type"] == "valve":
                node["state"]  = r.get("valve_state")
            nodes.append(node)

        edges = []
        for e in raw_edges:
            src = f'{e["from_type"]}:{e.get("from_code")}' if e.get("from_code") else f'{e["from_type"]}_{e["from_id"]}'
            dst = f'{e["to_type"]}:{e.get("to_code")}'     if e.get("to_code")     else f'{e["to_type"]}_{e["to_id"]}'
            edges.append(f"{src}>{dst}")

        return {"nodes": nodes, "edges": edges}
    except Exception as e:
        raise HTTPException(500, f"graph_all failed: {e}")

@router.get("/graph/nodes")
def graph_nodes(request: Request):
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            _set_org(cur, request.headers.get("X-Org-Id"))
            cur.execute("""
                SELECT
                  type,
                  asset_id AS id,
                  name, code,
                  level_ratio, capacity_liters,
                  pump_status, rated_kw, valve_state,
                  location_id, location_code, location_name
                FROM v_asset_nodes
                ORDER BY type, name;
            """)
            return cur.fetchall()
    except Exception as e:
        raise HTTPException(500, f"graph_nodes failed: {e}")

@router.get("/graph/edges")
def graph_edges(request: Request):
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            _set_org(cur, request.headers.get("X-Org-Id"))
            cur.execute("""
                SELECT id, from_type, from_id, from_name, from_code,
                       to_type,   to_id,   to_name,   to_code,
                       pipe_diameter_mm, length_m, is_active
                FROM v_topology_edges
                ORDER BY id;
            """)
            return cur.fetchall()
    except Exception as e:
        raise HTTPException(500, f"graph_edges failed: {e}")
