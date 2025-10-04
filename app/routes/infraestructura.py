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



# -------------------------------------------------------------------
# POST /infraestructura/update_layout
# -------------------------------------------------------------------
from fastapi import Request

@router.post("/update_layout")
async def update_layout(request: Request):
    """
    Actualiza las coordenadas (x, y) de un nodo en la tabla layout_*
    según el prefijo del node_id ('pump', 'manifold', 'valve', 'tank').
    """
    data = await request.json()
    node_id = data.get("node_id")
    x = data.get("x")
    y = data.get("y")

    if not node_id or not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
        raise HTTPException(status_code=400, detail="Parámetros inválidos: node_id, x, y son requeridos")

    tipo = node_id.split(":", 1)[0]
    table_map = {
        "pump": "layout_pumps",
        "manifold": "layout_manifolds",
        "valve": "layout_valves",
        "tank": "layout_tanks",
    }

    table = table_map.get(tipo)
    if not table:
        raise HTTPException(status_code=400, detail=f"Tipo de nodo no soportado: {tipo}")

    sql = f"UPDATE public.{table} SET x = %s, y = %s, updated_at = now() WHERE node_id = %s"

    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (x, y, node_id))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail=f"node_id no encontrado: {node_id}")
            conn.commit()
            return {"ok": True, "node_id": node_id, "x": x, "y": y, "table": table}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
