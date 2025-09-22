from fastapi import APIRouter, HTTPException
from app.core.db import get_conn  # Usamos get_conn para obtener la conexión
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
    try:
        # Usamos get_conn para obtener la conexión
        async with get_conn() as conn:
            rows = await conn.fetch(query)

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
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

# Crear el router
router = APIRouter()

# Endpoint para obtener los estados de los tanques
@router.get("/tank_statuses", response_model=List[TankStatusResponse])
async def tank_statuses():
    try:
        return await get_tank_status()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener los datos de los tanques: {str(e)}")
