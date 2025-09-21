# src/routers/infraestatus.py
from fastapi import APIRouter, HTTPException
from asyncpg import Connection
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
async def get_tank_status(connection: Connection) -> List[TankStatusResponse]:
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
    rows = await connection.fetch(query)
    
    return [TankStatusResponse(
        id=row["id"],
        name=row["name"],
        level_percent=row["level_percent"],
        low_pct=row["low_pct"],
        low_low_pct=row["low_low_pct"],
        high_pct=row["high_pct"],
        high_high_pct=row["high_high_pct"],
        status=row["status"]
    ) for row in rows]

# Crear el router
router = APIRouter()

# Endpoint para obtener los estados de los tanques
@router.get("/tank_statuses", response_model=List[TankStatusResponse])
async def tank_statuses(connection: Connection):
    try:
        return await get_tank_status(connection)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error al obtener los datos de los tanques")
