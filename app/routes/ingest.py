# app/routes/ingest.py
from fastapi import APIRouter, HTTPException
from psycopg.rows import dict_row
import psycopg
from app.db import get_conn
from app.schemas import TankIngestIn, TankIngestOut

router = APIRouter(prefix="/ingest", tags=["ingest"])

@router.post("/tank", response_model=TankIngestOut)
def ingest_tank(body: TankIngestIn):
    """
    Inserta una lectura para un tanque.
    Requiere: tank_id, level_pct (0..100).
    created_at es opcional (default NOW()).
    """
    sql = """
    insert into public.tank_ingest (tank_id, level_pct, created_at)
    values (%s, %s, coalesce(%s, now()))
    returning id, tank_id, level_pct, created_at
    """

    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (body.tank_id, body.level_pct, body.created_at))
            row = cur.fetchone()
            # Normalizamos level_pct a float para JSON
            row["level_pct"] = float(row["level_pct"]) if row["level_pct"] is not None else None
            return row
    except psycopg.errors.ForeignKeyViolation:
        # Si el tank_id no existe, el FK falla
        raise HTTPException(status_code=400, detail=f"tank_id={body.tank_id} no existe")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tank/latest/{tank_id}", response_model=TankIngestOut)
def get_tank_latest(tank_id: int):
    """
    Devuelve la última lectura (por created_at desc) para un tanque.
    """
    sql = """
    select id, tank_id, level_pct, created_at
    from public.tank_ingest
    where tank_id = %s
    order by created_at desc, id desc
    limit 1
    """
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (tank_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Sin lecturas")
        row["level_pct"] = float(row["level_pct"]) if row["level_pct"] is not None else None
        return row
