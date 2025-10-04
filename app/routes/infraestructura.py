from fastapi import APIRouter, HTTPException
from app.db import get_conn
from psycopg.rows import dict_row
from typing import List

router = APIRouter(prefix="/infraestructura", tags=["infraestructura"])

# Endpoint para obtener las conexiones entre bombas y tanques desde layout_edges
@router.get("/get_layout_edges", response_model=List[dict])
async def get_layout_edges():
    """
    Devuelve todas las conexiones entre las bombas y los tanques desde layout_edges.
    """
    sql = """
    SELECT 
        edge_id, 
        src_node_id, 
        dst_node_id, 
        relacion, 
        prioridad, 
        updated_at
    FROM 
        layout_edges
    ORDER BY 
        updated_at DESC;  -- Ordenamos por la fecha más reciente
    """
    
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql)
            layout_edges = cur.fetchall()

            # Si no hay conexiones, lanzar un error 404
            if not layout_edges:
                raise HTTPException(status_code=404, detail="No se encontraron conexiones en layout_edges")

            return layout_edges
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Endpoint para obtener los datos de la vista v_tanks_with_config
@router.get("/get_tanks_with_config", response_model=List[dict])
async def get_tanks_with_config():
    """
    Devuelve todos los datos de la vista v_tanks_with_config.
    """
    sql = """
    SELECT 
        tank_id, 
        name, 
        location_id, 
        location_name, 
        low_pct, 
        low_low_pct, 
        high_pct, 
        high_high_pct, 
        updated_by, 
        updated_at, 
        level_pct, 
        latest_ingest_id, 
        age_sec, 
        online, 
        alarma, 
        node_id, 
        x, 
        y
    FROM 
        public.v_tanks_with_config
    ORDER BY 
        tank_id;  -- Puedes ordenar por cualquier campo que desees, por ejemplo, por tank_id
    """
    
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql)
            tanks = cur.fetchall()

            # Si no hay datos, lanzar un error 404
            if not tanks:
                raise HTTPException(status_code=404, detail="No se encontraron datos en v_tanks_with_config")

            return tanks
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))