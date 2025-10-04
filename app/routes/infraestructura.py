from fastapi import APIRouter, HTTPException, Request
from typing import List
from app.db import get_conn
from psycopg.rows import dict_row

router = APIRouter(prefix="/infraestructura", tags=["infraestructura"])


# -------------------------------------------------------------------
# GET /infraestructura/health_db
# -------------------------------------------------------------------
@router.get("/health_db")
async def health_db():
    """
    Verifica la conexión con la base de datos (health-check interno).
    """
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB down: {e}")


# -------------------------------------------------------------------
# GET /infraestructura/get_layout_edges
# -------------------------------------------------------------------
@router.get("/get_layout_edges", response_model=List[dict])
async def get_layout_edges():
    """
    Devuelve todas las conexiones entre nodos desde public.layout_edges.
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
    except HTTPException:
        raise
    except Exception as e:
        # Devolver el detalle para ver el motivo real (conexión/SQL)
        raise HTTPException(status_code=500, detail=f"DB error (edges): {e}")

# -------------------------------------------------------------------
# GET /infraestructura/get_layout_combined
# -------------------------------------------------------------------
@router.get("/get_layout_combined", response_model=List[dict])
async def get_layout_combined():
    """
    Devuelve todos los nodos (pump/tank/valve/manifold) desde public.v_layout_combined.
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
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error (combined): {e}")

# -------------------------------------------------------------------
# POST /infraestructura/update_layout
# -------------------------------------------------------------------


@router.post("/update_layout")
async def update_layout(request: Request):
    data = await request.json()
    node_id = data.get("node_id")
    x = data.get("x")
    y = data.get("y")

    if not node_id or not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
        raise HTTPException(status_code=400, detail="Parámetros inválidos: node_id, x, y son requeridos")

    tipo, _, sufijo = node_id.partition(":")
    table_map = {
        "pump": ("layout_pumps", "pump_id"),
        "manifold": ("layout_manifolds", "manifold_id"),
        "valve": ("layout_valves", "valve_id"),
        "tank": ("layout_tanks", "tank_id"),
    }
    meta = table_map.get(tipo)
    if not meta:
        raise HTTPException(status_code=400, detail=f"Tipo de nodo no soportado: {tipo}")

    table, id_col = meta

    # Si el sufijo es numérico, actualizamos por *_id (lo que usa la vista).
    try:
        id_numeric = int(sufijo)
        where = f"{id_col} = %s"
        params = (x, y, id_numeric)
    except ValueError:
        # Si no es numérico, caemos a node_id
        where = "node_id = %s"
        params = (x, y, node_id)

    sql = f"""
        UPDATE public.{table}
        SET x = %s::double precision,
            y = %s::double precision,
            updated_at = now()
        WHERE {where}
        RETURNING node_id, {id_col} AS entity_id, x, y, updated_at
    """

    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"no se encontró fila en {table} con {where}")
            conn.commit()
            return {"ok": True, "table": table, "updated": row}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error (update): {e}")


@router.get("/bootstrap_layout")
async def bootstrap_layout():
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT node_id,id,type,x,y,updated_at,online,state,level_pct,alarma
                FROM public.v_layout_combined ORDER BY type,id
            """)
            nodes = cur.fetchall()

            cur.execute("""
                SELECT edge_id,src_node_id,dst_node_id,relacion,prioridad,updated_at
                FROM public.layout_edges ORDER BY updated_at DESC
            """)
            edges = cur.fetchall()

            return {"nodes": nodes, "edges": edges}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error (bootstrap): {e}")
