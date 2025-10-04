# infraestructura.py
from typing import List, Optional, TypedDict
from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel, Field

# Ajustá este import a tu helper real
from app.db import db

router = APIRouter(prefix="/infra", tags=["infraestructura"])

# ---------- Tipos de respuesta ----------
class GraphNode(TypedDict, total=False):
    id: str
    type: str
    name: str | None
    x: int | None
    y: int | None
    level: float | None
    low_pct: float | None
    low_low_pct: float | None
    high_high_pct: float | None
    status: str | None
    location_id: int | None
    location_name: str | None
    alarma: str | None
    online: bool | None
    age_sec: int | None
    last_seen: str | None

class GraphEdge(TypedDict):
    src: str
    dst: str

# ---------- Schemas de entrada ----------
class LayoutIn(BaseModel):
    id: str = Field(..., description="node_id: ej. 'pump:1' o 'tank:2'")
    x: int
    y: int

class EdgeIn(BaseModel):
    src: str = Field(..., description="node_id origen: ej. 'pump:1'")
    dst: str = Field(..., description="node_id destino: ej. 'tank:2'")
    relacion: str = Field('feeds', description="tipo de relación (default: 'feeds')")
    prioridad: int = Field(0, description="prioridad (default: 0)")

# ---------- Endpoints de lectura ----------
@router.get("/nodes", response_model=List[GraphNode])
def list_nodes(
    location_id: Optional[int] = Query(None, description="Filtra por location_id"),
    type: Optional[str] = Query(None, pattern="^(pump|tank)$", description="Filtra por tipo de nodo")
):
    sql = """
        SELECT *
        FROM infraestructura.v_graph_nodes
        WHERE ($1::bigint IS NULL OR location_id = $1)
          AND ($2::text   IS NULL OR type = $2)
        ORDER BY type, id
    """
    rows = db.fetch_all(sql, (location_id, type))
    for r in rows:
        if r.get("last_seen") is not None:
            r["last_seen"] = r["last_seen"].isoformat()
    return rows

@router.get("/edges", response_model=List[GraphEdge])
def list_edges():
    sql = "SELECT src, dst FROM infraestructura.v_graph_edges"
    return db.fetch_all(sql)

@router.get("/graph")
def graph(
    location_id: Optional[int] = Query(None),
    type: Optional[str] = Query(None, pattern="^(pump|tank)$")
):
    nodes = list_nodes(location_id=location_id, type=type)
    edges = list_edges()
    return {"nodes": nodes, "edges": edges}

# ---------- Endpoints de escritura ----------
@router.post("/layout")
def save_layout(items: List[LayoutIn]):
    """
    Upsert de posiciones (x,y) por node_id en infraestructura.layout
    """
    sql = """
    INSERT INTO infraestructura.layout (node_id, x, y)
    VALUES ($1, $2, $3)
    ON CONFLICT (node_id) DO UPDATE
      SET x = EXCLUDED.x,
          y = EXCLUDED.y,
          updated_at = now()
    """
    # Si tu helper tiene executemany, usalo; si no, iteramos:
    count = 0
    for it in items:
        db.execute(sql, (it.id, it.x, it.y))
        count += 1
    return {"ok": True, "updated": count}

@router.post("/edges")
def save_edges(edges: List[EdgeIn], strict: bool = True):
    """
    Upsert de aristas en infraestructura.aristas.
    - strict=True: valida que src/dst existan en v_graph_nodes; si no, 400.
    """
    # Validación (opcional) de node_id existentes
    if strict and edges:
        node_ids = {e.src for e in edges} | {e.dst for e in edges}
        q_check = """
          SELECT id FROM infraestructura.v_graph_nodes
          WHERE id = ANY($1::text[])
        """
        found = {r["id"] for r in db.fetch_all(q_check, (list(node_ids),))}
        missing = sorted(list(node_ids - found))
        if missing:
            raise HTTPException(
                status_code=400,
                detail={"error": "node_id inexistentes", "missing": missing}
            )

    upsert = """
    INSERT INTO infraestructura.aristas (src_node_id, dst_node_id, relacion, prioridad)
    VALUES ($1, $2, COALESCE($3,'feeds'), COALESCE($4,0))
    ON CONFLICT (src_node_id, dst_node_id, relacion)
    DO UPDATE SET prioridad = EXCLUDED.prioridad
    """
    count = 0
    for e in edges:
        db.execute(upsert, (e.src, e.dst, e.relacion, e.prioridad))
        count += 1
    return {"ok": True, "upserted": count}
