from pydantic import BaseModel
from typing import List
from fastapi import APIRouter, HTTPException
import asyncpg  # Usamos asyncpg en lugar de psycopg
from typing import List

# Configura tu URL de conexión a la base de datos
DATABASE_URL = "postgresql://postgres:password@localhost/dbname"  # Cambia esto a tu URL de conexión real

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
    # Establecemos la conexión asíncrona con la base de datos
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch(query)  # Usamos `fetch` para obtener resultados de manera asíncrona
    finally:
        await conn.close()  # Aseguramos que la conexión se cierre

    # Procesamos los resultados y los retornamos
    return [
        TankStatusResponse(
            id=row['id'],
            name=row['name'],
            level_percent=row['level_percent'],
            low_pct=row['low_pct'],
            low_low_pct=row['low_low_pct'],
            high_pct=row['high_pct'],
            high_high_pct=row['high_high_pct'],
            status=row['status']
        )
        for row in rows
    ]

# Crear el router
router = APIRouter()

# Endpoint para obtener los estados de los tanques
@router.get("/tank_statuses", response_model=List[TankStatusResponse])
async def tank_statuses():
    try:
        return await get_tank_status()  # Llamamos la función asíncrona
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener los datos de los tanques: {str(e)}")
