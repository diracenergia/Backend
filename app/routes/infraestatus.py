from fastapi import APIRouter, HTTPException
from typing import List
from app.core.db import get_conn  # Asegúrate de usar la conexión sincrónica

# Crea una instancia de APIRouter
router = APIRouter()

# Modelo de respuesta para el estado de los tanques
class TankStatusResponse(BaseModel):
    id: int
    name: str
    level_percent: float
    low_pct: float
    low_low_pct: float
    high_pct: float
    high_high_pct: float
    status: str

# Endpoint para obtener los estados de los tanques
@router.get("/tank_statuses", response_model=List[TankStatusResponse])
def tank_statuses():
    try:
        with get_conn() as conn:  # Usamos la conexión sincrónica
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
            conn.execute(query)
            rows = conn.fetchall()

        # Procesamos los resultados y los retornamos
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
        raise HTTPException(status_code=500, detail="Error al obtener los datos de los tanques")
