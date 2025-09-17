# app/routes/graph_api.py (o donde tengas el router)
from fastapi import APIRouter, HTTPException, Request
from psycopg.rows import dict_row
from app.core.db import get_conn

router = APIRouter()

@router.get("/graph/nodes")
def graph_nodes(request: Request):
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            # Si manejás multi-tenant por header
            org_id = request.headers.get("X-Org-Id")
            if org_id:
                cur.execute("SET LOCAL app.org_id = %s", (org_id,))

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
            org_id = request.headers.get("X-Org-Id")
            if org_id:
                cur.execute("SET LOCAL app.org_id = %s", (org_id,))

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

@router.get("/graph")
def graph_all(request: Request):
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            org_id = request.headers.get("X-Org-Id")
            if org_id:
                cur.execute("SET LOCAL app.org_id = %s", (org_id,))

            cur.execute("""SELECT * FROM v_asset_nodes ORDER BY type, name;""")
            raw_nodes = cur.fetchall()
            cur.execute("""SELECT * FROM v_topology_edges WHERE is_active ORDER BY id;""")
            raw_edges = cur.fetchall()

        nodes = []
        for r in raw_nodes:
            # UID único: si hay code => "<type>:<code>", sino "<type>_<asset_id>"
            nid = f'{r["type"]}:{r["code"]}' if r["code"] else f'{r["type"]}_{r["asset_id"]}'
            node = {"id": nid, "type": r["type"], "name": r["name"]}
            if r["type"] == "tank":
                node["level"]    = r["level_ratio"]
                node["capacity"] = r["capacity_liters"]
            elif r["type"] == "pump":
                node["status"] = r["pump_status"]
                node["kW"]     = r["rated_kw"]
            elif r["type"] == "valve":
                node["state"]  = r["valve_state"]
            nodes.append(node)

        edges = []
        for e in raw_edges:
            src = f'{e["from_type"]}:{e["from_code"]}' if e["from_code"] else f'{e["from_type"]}_{e["from_id"]}'
            dst = f'{e["to_type"]}:{e["to_code"]}'     if e["to_code"]   else f'{e["to_type"]}_{e["to_id"]}'
            edges.append(f"{src}>{dst}")

        return {"nodes": nodes, "edges": edges}
    except Exception as e:
        raise HTTPException(500, f"graph_all failed: {e}")
