# app/routes/graph_api.py
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from psycopg.rows import dict_row

from app.core.db import get_conn

router = APIRouter()

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _set_org(cur, org_id: Optional[str]):
    """
    Intenta setear la GUC 'app.org_id' (si tus vistas la usan). No rompe si falla.
    """
    if not org_id:
        return
    try:
        cur.execute("SELECT set_config('app.org_id', %s, true)", (str(org_id),))
    except Exception:
        # No tiramos abajo el request si falla el set_config
        pass

def _get_layout_map(org_id: Optional[str]) -> dict:
    """
    Lee posiciones guardadas en DB para la org dada y devuelve {node_uid: (x, y)}.
    """
    if not org_id:
        return {}
    layout_map: dict[str, tuple[float, float]] = {}
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT node_uid, x, y
            FROM public.asset_layouts
            WHERE org_id = %s
            """,
            (org_id,),
        )
        for r in cur.fetchall():
            # numeric -> float
            layout_map[r["node_uid"]] = (float(r["x"]), float(r["y"]))
    return layout_map


# ---------------------------------------------------------------------
# Graph (combinado)
# ---------------------------------------------------------------------

@router.get("/graph")
def graph_all(request: Request):
    """
    Devuelve:
      {
        "nodes": [
          { "id": "type:code" | "type_<asset_id>", "type": "...", "name": "...", (x,y?) ... }
        ],
        "edges": ["SRC>DST", ...]
      }
    Además, inyecta (x,y) desde public.asset_layouts si existen para esa org.
    """
    try:
        org_id = request.headers.get("X-Org-Id")

        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            _set_org(cur, org_id)

            cur.execute("SELECT * FROM v_asset_nodes ORDER BY type, name;")
            raw_nodes = cur.fetchall()

            cur.execute("SELECT * FROM v_topology_edges WHERE is_active ORDER BY id;")
            raw_edges = cur.fetchall()

        # Mapa de posiciones guardadas en DB
        layout_map = _get_layout_map(org_id)

        nodes = []
        for r in raw_nodes:
            # ID único EXACTO (coincide con edges y con asset_layouts.node_uid)
            # preferimos code si existe, sino fallback a type_id
            code = r.get("code")
            nid = f'{r["type"]}:{code}' if code else f'{r["type"]}_{r["asset_id"]}'
            node = {"id": nid, "type": r["type"], "name": r["name"]}

            # payload específico por tipo (opcional para el front)
            if r["type"] == "tank":
                node["level"]    = r.get("level_ratio")
                node["capacity"] = r.get("capacity_liters")
            elif r["type"] == "pump":
                node["status"] = r.get("pump_status")
                node["kW"]     = r.get("rated_kw")
            elif r["type"] == "valve":
                node["state"]  = r.get("valve_state")

            # Inyectar posiciones si están guardadas
            if nid in layout_map:
                x, y = layout_map[nid]
                node["x"], node["y"] = x, y

            nodes.append(node)

        edges = []
        for e in raw_edges:
            src = f'{e["from_type"]}:{e.get("from_code")}' if e.get("from_code") else f'{e["from_type"]}_{e["from_id"]}'
            dst = f'{e["to_type"]}:{e.get("to_code")}'     if e.get("to_code")     else f'{e["to_type"]}_{e["to_id"]}'
            edges.append(f"{src}>{dst}")

        return {"nodes": nodes, "edges": edges}
    except Exception as e:
        raise HTTPException(500, f"graph_all failed: {e}")


# ---------------------------------------------------------------------
# Graph (endpoints separados: opcional / compat)
# ---------------------------------------------------------------------

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


# ---------------------------------------------------------------------
# Layout Autosave (DB)
# ---------------------------------------------------------------------

class NodePos(BaseModel):
    id: str
    x: float
    y: float
    updated_by: Optional[str] = None  # opcional (ej. email o "ui-embed")

@router.post("/layout")
def save_layout(items: List[NodePos], request: Request):
    """
    Guarda posiciones (UPSERT) en public.asset_layouts para la org indicada por X-Org-Id.
    Body: [{ "id": "<node_uid>", "x": <num>, "y": <num>, "updated_by": "..." }, ...]
    """
    org_id = request.headers.get("X-Org-Id")
    if not org_id:
        raise HTTPException(400, "X-Org-Id requerido")

    if not items:
        return {"ok": True, "saved": 0}

    try:
        with get_conn() as conn, conn.cursor() as cur:
            for it in items:
                cur.execute(
                    """
                    INSERT INTO public.asset_layouts (org_id, node_uid, x, y, updated_by)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (org_id, node_uid)
                    DO UPDATE SET x = EXCLUDED.x,
                                  y = EXCLUDED.y,
                                  updated_by = EXCLUDED.updated_by,
                                  updated_at = now()
                    """,
                    (org_id, it.id, it.x, it.y, it.updated_by or "ui-embed"),
                )
        return {"ok": True, "saved": len(items)}
    except Exception as e:
        raise HTTPException(500, f"save_layout failed: {e}")

@router.get("/layout")
def get_layout(request: Request):
    """
    Devuelve todas las posiciones guardadas para la org (útil para debug).
    """
    org_id = request.headers.get("X-Org-Id")
    if not org_id:
        raise HTTPException(400, "X-Org-Id requerido")

    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT node_uid AS id, x, y, updated_by, updated_at
                FROM public.asset_layouts
                WHERE org_id = %s
                ORDER BY updated_at DESC
                """,
                (org_id,),
            )
            return cur.fetchall()
    except Exception as e:
        raise HTTPException(500, f"get_layout failed: {e}")
