from fastapi import APIRouter, HTTPException
from psycopg.rows import dict_row
from app.db import get_conn

router = APIRouter(prefix="/infraestructura", tags=["infraestructura"])

# Endpoint para obtener las conexiones entre bombas y tanques
@router.get("/get_connections")
async def get_connections():
    """
    Obtiene todas las conexiones entre las bombas y los tanques.
    """
    sql = """
    SELECT edge_id, src_node_id, dst_node_id, relacion, prioridad, updated_at
    FROM public.layout_edges;
    """
    
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql)
            connections = cur.fetchall()

            # Normalizamos los resultados para asegurarnos de que el JSON sea adecuado
            result = [
                {
                    "edge_id": row["edge_id"],
                    "src_node_id": row["src_node_id"],
                    "dst_node_id": row["dst_node_id"],
                    "relacion": row["relacion"],
                    "prioridad": row["prioridad"],
                    "updated_at": row["updated_at"]
                }
                for row in connections
            ]

            # Si no se encontraron conexiones, devolver un error 404
            if not result:
                raise HTTPException(status_code=404, detail="No se encontraron conexiones")

            return result
    
    except Exception as e:
        # Capturar cualquier error y devolverlo como un error 500
        raise HTTPException(status_code=500, detail=f"Error al obtener las conexiones: {e}")
