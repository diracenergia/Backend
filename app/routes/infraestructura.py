# app/routes/infraestructura.py
from fastapi import APIRouter, HTTPException
from app.db import get_conn  # Asegúrate de que esta importación esté configurada
from typing import List

router = APIRouter(prefix="/infraestructura", tags=["infraestructura"])

# Endpoint para obtener las conexiones entre las bombas y los tanques
@router.get("/get_connections")
async def get_connections():
    # Conexión a la base de datos
    conn = get_conn()
    
    # Usar un cursor para ejecutar la consulta
    cursor = conn.cursor()
    
    try:
        # Consulta SQL para obtener las conexiones de la tabla layout_edges
        query = """
        SELECT edge_id, src_node_id, dst_node_id, relacion, prioridad, updated_at
        FROM public.layout_edges;
        """
        
        # Ejecutar la consulta
        cursor.execute(query)
        
        # Recuperar todos los resultados
        connections = cursor.fetchall()
        
        # Formatear los resultados
        result = [
            {
                "edge_id": row[0],
                "src_node_id": row[1],
                "dst_node_id": row[2],
                "relacion": row[3],
                "prioridad": row[4],
                "updated_at": row[5]
            }
            for row in connections
        ]
        
        return result
    
    except Exception as e:
        # Si ocurre un error, devolver un error 500
        raise HTTPException(status_code=500, detail=f"Error al obtener las conexiones: {e}")
    
    finally:
        # Cerrar el cursor y la conexión
        cursor.close()
        conn.close()
