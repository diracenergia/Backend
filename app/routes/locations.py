
from fastapi import APIRouter
from app.db import get_conn
from app.schemas import Location

router = APIRouter(prefix="/infra", tags=["infra"])

@router.get("/locations", response_model=list[Location])
def list_locations():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("select id,name,address,lat,lon,active,created_at from infra.locations order by id asc")
        rows = cur.fetchall()
        return rows
from fastapi import APIRouter
from app.db import get_conn
from app.schemas import Location

router = APIRouter(prefix="/infra", tags=["infra"])

@router.get("/locations", response_model=list[Location])
def list_locations():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("select id,name,address,lat,lon,active,created_at from infra.locations order by id asc")
        rows = cur.fetchall()
        return rows
