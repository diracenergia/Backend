from fastapi import APIRouter, HTTPException
from app.db import get_conn
from psycopg.rows import dict_row
from typing import List

router = APIRouter(prefix="/infraestructura", tags=["infraestructura"])

# Endpoint para obtener los datos de los tanques con configuración, nodos y coordenadas
@router.get("/get_tanks_with_config", response_model=List[dict])
async def get_tanks_with_config():
    """
    Devuelve todos los tanques con sus configuraciones, nodos y coordenadas.
    """
    sql = """
    SELECT 
        t.id AS tank_id,
        t.name AS tank_name,
        t.location_id,
        lct.name AS location_name,
        cfg.low_pct,
        cfg.low_low_pct,
        cfg.high_pct,
        cfg.high_high_pct,
        cfg.updated_by,
        cfg.updated_at,
        vlt.level_pct,
        vlt.latest_ingest_id,
        vlt.age_sec,
        vlt.online,
        CASE
            WHEN (vlt.level_pct IS NULL) THEN 'normal'::text
            WHEN ((vlt.level_pct <= COALESCE(cfg.low_low_pct, (10)::numeric)) OR (vlt.level_pct >= COALESCE(cfg.high_high_pct, (90)::numeric))) THEN 'critico'::text
            WHEN ((vlt.level_pct <= COALESCE(cfg.low_pct, (25)::numeric)) OR (vlt.level_pct >= COALESCE(cfg.high_pct, (80)::numeric))) THEN 'alerta'::text
            ELSE 'normal'::text
        END AS alarma,
        lt_layout.node_id,  -- node_id de layout_tanks
        lt_layout.x, lt_layout.y  -- Coordenadas de layout_tanks
    FROM 
        tanks t
    LEFT JOIN 
        tank_configs cfg ON cfg.tank_id = t.id
    LEFT JOIN 
        v_tank_latest vlt ON vlt.tank_id = t.id
    LEFT JOIN 
        locations lct ON lct.id = t.location_id
    LEFT JOIN 
        layout_tanks lt_layout ON lt_layout.tank_id = t.id
    ORDER BY 
        t.id;
    """
    
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql)
            tanks = cur.fetchall()

            # Si no hay datos, lanzar un error 404
            if not tanks:
                raise HTTPException(status_code=404, detail="No se encontraron tanques")

            return tanks
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Endpoint para obtener las conexiones entre las bombas y los tanques
@router.get("/get_connections", response_model=List[dict])
async def get_connections():
    """
    Devuelve todas las conexiones entre bombas y tanques.
    """
    sql = """
    SELECT 
        le.edge_id,
        le.src_node_id,
        le.dst_node_id,
        le.relacion,
        le.prioridad,
        le.updated_at,
        t.tank_id,
        t.name AS tank_name,
        p.pump_id,
        p.name AS pump_name
    FROM 
        layout_edges le
    LEFT JOIN 
        tanks t ON t.node_id = le.dst_node_id  -- Conectar tanque
    LEFT JOIN 
        pumps p ON p.node_id = le.src_node_id  -- Conectar bomba
    ORDER BY 
        le.edge_id;
    """
    
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql)
            connections = cur.fetchall()

            # Si no hay conexiones, lanzar un error 404
            if not connections:
                raise HTTPException(status_code=404, detail="No se encontraron conexiones")

            return connections
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
