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

# --- GETs: leen de las VISTAS ---
@router.get("/nodes")
def get_nodes():
    # Aseguramos que se lea desde el esquema correcto "infraestructura"
    rows = fetch_all("SELECT * FROM infraestructura.v_graph_nodes ORDER BY type, id")
    for r in rows:
        val = r.get("last_seen")
        if isinstance(val, (datetime, date)):
            r["last_seen"] = val.isoformat()
    return rows

@router.get("/edges")
def get_edges():
    # Aseguramos que se lea desde el esquema correcto "infraestructura"
    return fetch_all("SELECT src, dst FROM infraestructura.v_graph_edges ORDER BY 1,2")

@router.get("/graph")
def get_graph():
    # Incluye ambos endpoints de nodes y edges
    return {"nodes": get_nodes(), "edges": get_edges()}

# --- POSTs: escriben en tablas base ---
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
