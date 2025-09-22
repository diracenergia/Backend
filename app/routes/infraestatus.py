from fastapi import APIRouter, HTTPException
from app.core.db import get_conn  # Usamos el método de conexión del archivo db.py
from typing import List
from pydantic import BaseModel

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
        # Log de conexión
        logger.info("Connecting to the database...")
        
        # Obtener la conexión desde get_conn() y ejecutar la consulta
        with get_conn() as conn:
            with conn.cursor() as cur:  # Usamos un cursor para ejecutar la consulta
                logger.info("Database connection established.")
                cur.execute(query)  # Ejecutamos la consulta
                rows = cur.fetchall()  # Obtenemos los resultados de la consulta
        
        # Mapear los resultados de la base de datos al modelo TankStatusResponse
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
        logger.error(f"Error executing query: {str(e)}")  # Log del error
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
        raise HTTPException(status_code=500, detail="Error al obtener los datos de los tanques")
