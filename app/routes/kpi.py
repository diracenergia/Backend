# routes/kpi.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from typing import Any, List, Optional
from datetime import datetime
import asyncpg

router = APIRouter(prefix="/kpi", tags=["kpi"])

# ---------- Helpers / DI ----------

async def get_pool(request: Request) -> asyncpg.Pool:
    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        raise HTTPException(status_code=500, detail="DB pool not initialized")
    return pool

async def fetch(pool: asyncpg.Pool, sql: str, *params: Any) -> List[dict]:
    async with pool.acquire() as con:
        rows = await con.fetch(sql, *params)
    return [dict(r) for r in rows]

async def fetchrow(pool: asyncpg.Pool, sql: str, *params: Any) -> Optional[dict]:
    async with pool.acquire() as con:
        row = await con.fetchrow(sql, *params)
    return dict(row) if row else None

# ---------- Endpoints ----------

# 1) Bombas con estado actual (todas)
@router.get("/pumps/status")
async def pumps_status(pool: asyncpg.Pool = Depends(get_pool)) -> List[dict]:
    sql = "SELECT * FROM public.v_pumps_with_status"
    return await fetch(pool, sql)

# 1.b) Bomba específica por id
@router.get("/pumps/{pump_id}/status")
async def pump_status(
    pump_id: int,
    pool: asyncpg.Pool = Depends(get_pool),
) -> dict:
    sql = "SELECT * FROM public.v_pumps_with_status WHERE pump_id = $1"
    row = await fetchrow(pool, sql, pump_id)
    if not row:
        raise HTTPException(status_code=404, detail="Pump not found")
    return row

# 2) Tanques con config y último nivel (todos)
@router.get("/tanks/latest")
async def tanks_latest(pool: asyncpg.Pool = Depends(get_pool)) -> List[dict]:
    sql = "SELECT * FROM public.v_tanks_with_config"
    return await fetch(pool, sql)

# 2.b) Tanque específico
@router.get("/tanks/{tank_id}/latest")
async def tank_latest(
    tank_id: int,
    pool: asyncpg.Pool = Depends(get_pool),
) -> dict:
    sql = "SELECT * FROM public.v_tanks_with_config WHERE tank_id = $1"
    row = await fetchrow(pool, sql, tank_id)
    if not row:
        raise HTTPException(status_code=404, detail="Tank not found")
    return row

# 3) Serie temporal para gráficos (opcional)
@router.get("/tanks/{tank_id}/levels")
async def tank_levels_timeseries(
    tank_id: int,
    date_from: datetime = Query(..., alias="from"),
    date_to:   datetime = Query(..., alias="to"),
    pool: asyncpg.Pool = Depends(get_pool),
) -> List[dict]:
    if date_from >= date_to:
        raise HTTPException(status_code=400, detail="'from' must be before 'to'")
    sql = """
        SELECT tank_id, tank_name, level_pct, ts
        FROM public.v_tank_levels_timeseries
        WHERE tank_id = $1 AND ts >= $2 AND ts < $3
        ORDER BY ts ASC
    """
    return await fetch(pool, sql, tank_id, date_from, date_to)
