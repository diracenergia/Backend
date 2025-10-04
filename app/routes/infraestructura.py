from fastapi import APIRouter, HTTPException
from app.db import get_conn
from psycopg.rows import dict_row
from typing import List

router = APIRouter(prefix="/infraestructura", tags=["infraestructura"])

# -------------------------------------------------------------------
# GET /infraestructura/get_layout_edges
# -------------------------------------------------------------------
@router.get("/get_layout_edges", response_model=List[dict])
async def get_layout_edges():
    """
    Devuelve todas las conexiones entre nodos desde public.layout_edges.
    (Ej.: pump:1 -> manifold:1 -> valve:1 -> tank:1)
    """
    sql = """
    SELECT
        edge_id,
        src_node_id,
        dst_node_id,
        relacion,
        prioridad,
        updated_at
    FROM public.layout_edges
    ORDER BY updated_at DESC;
    """
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            if not rows:
                raise HTTPException(status_code=404, detail="No se encontraron conexiones en public.layout_edges")
            return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------------------------------------------------
# GET /infraestructura/get_layout_combined
# -------------------------------------------------------------------
@router.get("/get_layout_combined", response_model=List[dict])
async def get_layout_combined():
    """
    Devuelve todos los nodos (pump/tank/valve/manifold) desde public.v_layout_combined,
    incluyendo estado básico cuando aplica:
      - online (pumps/tanks)
      - state  (pumps)
      - level_pct, alarma (tanks)
    """
    sql = """
    SELECT
        node_id,
        id,
        type,
        x,
        y,
        updated_at,
        online,
        state,
        level_pct,
        alarma
    FROM public.v_layout_combined
    ORDER BY type, id;
    """
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql)
            nodes = cur.fetchall()
            if not nodes:
                raise HTTPException(status_code=404, detail="No se encontraron nodos en v_layout_combined")
            return nodes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
