from fastapi import APIRouter, HTTPException
from typing import List
from pydantic import BaseModel
from app.core.db import get_conn  # Importamos get_conn para usar el pool de conexiones
import logging

# Configurar logging
logger = logging.getLogger("infraestatus")
logger.setLevel(logging.DEBUG)

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
async def get_tank_status() -> List[TankStatusResponse]:
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
    logger.info("Iniciando la conexión con la base de datos...")
    try:
        async with get_conn() as conn:
            logger.info("Conexión a la base de datos establecida.")
            rows = await conn.fetch(query)
            logger.info(f"Consulta ejecutada correctamente, se obtuvieron {len(rows)} registros.")
            return [
                TankStatusResponse(
                    id=row["id"],
                    name=row["name"],
                    level_percent=row["level_percent"],
                    low_pct=row["low_pct"],
                    low_low_pct=row["low_low_pct"],
                    high_pct=row["high_pct"],
                    high_high_pct=row["high_high_pct"],
                    status=row["status"]
                )
                for row in rows
            ]
    except Exception as e:
        logger.error(f"Error al obtener los datos de los tanques: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al obtener los datos de los tanques: {str(e)}")

# Crear el router
router = APIRouter()

# Endpoint para obtener los estados de los tanques
@router.get("/tank_statuses", response_model=List[TankStatusResponse])
async def tank_statuses():
    logger.info("Iniciando solicitud para obtener los estados de los tanques.")
    try:
        result = await get_tank_status()
        logger.info("Datos de los tanques obtenidos con éxito.")
        return result
    except Exception as e:
        logger.error(f"Error en la obtención de los estados de los tanques: {str(e)}")
        raise HTTPException(status_code=500, detail="Error al obtener los datos de los tanques")
