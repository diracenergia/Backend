# app/routes/locations.py
from fastapi import APIRouter, HTTPException, Query
from psycopg.rows import dict_row
from app.core.db import get_conn

router = APIRouter(prefix="/infra", tags=["infra-locations"])

@router.get("/locations")
def list_locations():
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        cur.execute("select id, code, name from public.locations order by name;")
        return cur.fetchall()

@router.get("/locations/{loc_id}/assets")
def location_assets(loc_id: int):
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            select al.asset_type, al.asset_id, n.name, n.code
            from public.asset_locations al
            left join public.v_asset_nodes n
              on n.type = al.asset_type and n.asset_id = al.asset_id
            where al.location_id = %s
            order by al.asset_type, n.name;
        """, (loc_id,))
        return cur.fetchall()

@router.get("/locations/{loc_id}/summary")
def location_summary(loc_id: int):
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        cur.execute("""
          select * from public.v_location_summary_30d where location_id=%s;
        """, (loc_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "location not found")
        return row
