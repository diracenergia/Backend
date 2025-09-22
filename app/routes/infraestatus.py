from fastapi import APIRouter, HTTPException
from app.core.db import get_conn  # Usamos get_conn para obtener la conexión
from typing import List
from pydantic import BaseModel
import logging

# Configurar logging para depuración
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# Modelo de respuesta
class TankStatusResponse(BaseModel):
    id: int
    name: str
    level_percent: float
    low_pct: float
    low_low_pct: float
    high_pct: float
    high_high_pct: float
    status: str

# Consulta para obtener el estado de los tanques
def get_tank_status() -> List[TankStatusResponse]:
    query = """
    SELECT 
        t.id,
        t.name,
        tl.level_percent,
        tc.low_pct,
        tc.low_low_pct,
        tc.high_pct,
        tc.high_high_pct,
        CASE
            WHEN tl.level_percent <= tc.low_low_pct THEN 'critical'
            WHEN tl.level_percent <= tc.low_pct THEN 'warning'
            WHEN tl.level_percent >= tc.high_high_pct THEN 'critical'
            WHEN tl.level_percent >= tc.high_pct THEN 'warning'
            ELSE 'ok'
        END AS status
    FROM 
        public.tanks t
    JOIN 
        public.tank_levels tl ON tl.tank_id = t.id
    JOIN 
        public.tank_config tc ON tc.tank_id = t.id
    WHERE
        t.org_id = 1;
    """
    try:
        logger.info("Connecting to the database...")
        # Cambiamos get_conn() para usar un cursor
        with get_conn() as conn:  # Obtener la conexión
            with conn.cursor() as cur:  # Creamos el cursor
                logger.info("Database connection established.")
                cur.execute(query)  # Ejecutamos la consulta
                rows = cur.fetchall()  # Obtenemos los resultados
                
                logger.info(f"Query executed successfully, fetched {len(rows)} rows.")
        
        return [
            TankStatusResponse(
                id=row[0],
                name=row[1],
                level_percent=row[2],
                low_pct=row[3],
                low_low_pct=row[4],
                high_pct=row[5],
                high_high_pct=row[6],
                status=row[7]
            )
            for row in rows
        ]
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

# Crear el router
router = APIRouter()

# Endpoint para obtener los estados de los tanques
@router.get("/tank_statuses", response_model=List[TankStatusResponse])
async def tank_statuses():
    try:
        logger.info("Fetching tank statuses...")
        return get_tank_status()
    except Exception as e:
        logger.error(f"Error in tank status retrieval: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al obtener los datos de los tanques: {str(e)}")
