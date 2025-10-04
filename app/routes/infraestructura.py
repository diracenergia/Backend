# app/routes/infraestructura.py
from typing import List
from datetime import datetime, date
from fastapi import APIRouter
from pydantic import BaseModel
from app.db import get_conn

router = APIRouter(prefix="/infraestructura", tags=["infraestructura"])

# --- helpers DB ---
def fetch_all(sql: str, params: tuple = ()):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

def execute(sql: str, params: tuple = ()):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        conn.commit()

# --- inputs ---
class LayoutIn(BaseModel):
    id: str
    x: int
    y: int

class EdgeIn(BaseModel):
    src: str
    dst: str
    relacion: str = "feeds"
    prioridad: int = 0

# --- GETs sin response_model (devuelven list[dict]) ---
@router.get("/nodes")
def get_nodes():
    rows = fetch_all("SELECT * FROM infraestructura.v_graph_nodes ORDER BY type, id")
    for r in rows:
        val = r.get("last_seen")
        if isinstance(val, (datetime, date)):
            r["last_seen"] = val.isoformat()
        # si es None o string, lo dejamos como está
    return rows

@router.get("/edges")
def get_edges():
    return fetch_all("SELECT src, dst FROM infraestructura.v_graph_edges")

@router.get("/graph")
def get_graph():
    return {"nodes": get_nodes(), "edges": get_edges()}

# --- POSTs simples ---
@router.post("/layout")
def save_layout(items: List[LayoutIn]):
    sql = """
    INSERT INTO infraestructura.layout (node_id, x, y)
    VALUES (%s, %s, %s)
    ON CONFLICT (node_id) DO UPDATE
      SET x = EXCLUDED.x, y = EXCLUDED.y, updated_at = now()
    """
    for it in items:
        execute(sql, (it.id, it.x, it.y))
    return {"ok": True, "updated": len(items)}

@router.post("/edges")
def save_edges(edges: List[EdgeIn]):
    upsert = """
    INSERT INTO infraestructura.aristas (src_node_id, dst_node_id, relacion, prioridad)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (src_node_id, dst_node_id, relacion)
    DO UPDATE SET prioridad = EXCLUDED.prioridad
    """
    for e in edges:
        execute(upsert, (e.src, e.dst, e.relacion, e.prioridad))
    return {"ok": True, "upserted": len(edges)}


# en app/routes/infraestructura.py (TEMPORAL)
@router.get("/_debug_viewdefs")
def _debug_viewdefs():
    sql = "SELECT 'v_graph_nodes' AS name, pg_get_viewdef('infraestructura.v_graph_nodes'::regclass, true)"
    sql2 = "SELECT 'v_graph_edges' AS name, pg_get_viewdef('infraestructura.v_graph_edges'::regclass, true)"
    return {
        "nodes": fetch_all(sql)[0],
        "edges": fetch_all(sql2)[0],
    }


@router.get("/_debug_env")
def _debug_env():
    q1 = """
    SELECT current_database() AS db, current_user AS usr,
           current_schema() AS schema, current_setting('search_path') AS search_path,
           version() AS version
    """
    q2 = "SELECT EXISTS (SELECT 1 FROM information_schema.views WHERE table_schema='infraestructura' AND table_name='v_graph_nodes') AS has_nodes"
    q3 = "SELECT EXISTS (SELECT 1 FROM information_schema.views WHERE table_schema='infraestructura' AND table_name='v_graph_edges') AS has_edges"
    return {
        "env": fetch_all(q1)[0],
        "has_v_graph_nodes": fetch_all(q2)[0]["has_nodes"],
        "has_v_graph_edges": fetch_all(q3)[0]["has_edges"],
    }
