# app/routes/locations.py
from collections import defaultdict
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from psycopg.rows import dict_row

from app.core.db import get_conn
from app.schemas.infra import (
    Location, LocationWithStats, AssetGroup, AssetItem,
    LocationSummary, LocationTree, AssetType
)

router = APIRouter(prefix="/infra", tags=["infra-locations"])


# ------------------------------------------------------------
# Modelo de salida para el mapa plano asset → location
# ------------------------------------------------------------
class AssetLoc(BaseModel):
    asset_type: AssetType            # Literal['tank','pump','valve','manifold']
    asset_id: int
    location: Optional[Location] = None


# ------------------------------------------------------------
# GET /infra/asset-locations
# Mapa plano asset → location (incluye huérfanos con location=null)
# Filtros opcionales: ?type=tank&type=pump  y/o ?location_id=5
# ------------------------------------------------------------
@router.get("/asset-locations", response_model=List[AssetLoc])
def list_asset_locations(
    type: Optional[List[AssetType]] = Query(
        default=None,
        description="Filtro por tipo (repetir ?type=tank&type=pump)"
    ),
    location_id: Optional[int] = Query(
        default=None,
        description="Filtra por location_id. Si no se pasa, devuelve todos (incluye null)."
    )
):
    base_sql = """
    SELECT
      n.type       AS asset_type,
      n.asset_id,
      al.location_id,
      l.code       AS location_code,
      l.name       AS location_name
    FROM public.v_asset_nodes n
    LEFT JOIN public.asset_locations al
      ON al.asset_type = n.type AND al.asset_id = n.asset_id
    LEFT JOIN public.locations l
      ON l.id = al.location_id
    WHERE 1=1
    """
    params: list = []

    if type:
        base_sql += " AND n.type = ANY(%s)"
        params.append(type)

    if location_id is not None:
        base_sql += " AND al.location_id = %s"
        params.append(location_id)

    base_sql += " ORDER BY l.name NULLS LAST, n.type, n.asset_id;"

    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        cur.execute(base_sql, params or None)
        rows = cur.fetchall()

    out: list[AssetLoc] = []
    for r in rows:
        loc = None
        if r["location_id"] is not None:
            loc = Location(id=r["location_id"], code=r["location_code"], name=r["location_name"])
        out.append(AssetLoc(asset_type=r["asset_type"], asset_id=r["asset_id"], location=loc))
    return out


# ------------------------------------------------------------
# GET /infra/locations
# Lista de locations. Si with_stats=true, incluye conteos y alarmas.
# ------------------------------------------------------------
@router.get("/locations", response_model=List[Location])
def list_locations(with_stats: bool = Query(False)):
    """
    Lista de locations. Si with_stats=true, devuelve conteos y alarmas.
    """
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        if not with_stats:
            cur.execute("""
                SELECT id, code, name
                FROM public.locations
                ORDER BY name;
            """)
            return cur.fetchall()
        else:
            # devolvemos LocationWithStats pero FastAPI lo serializa igual
            cur.execute("""
                WITH counts AS (
                  SELECT
                    al.location_id,
                    count(*) AS assets_total,
                    count(*) FILTER (WHERE al.asset_type='tank')     AS tanks_count,
                    count(*) FILTER (WHERE al.asset_type='pump')     AS pumps_count,
                    count(*) FILTER (WHERE al.asset_type='valve')    AS valves_count,
                    count(*) FILTER (WHERE al.asset_type='manifold') AS manifolds_count
                  FROM public.asset_locations al
                  GROUP BY al.location_id
                ),
                alarms AS (
                  SELECT
                    al.location_id,
                    count(*) FILTER (WHERE a.is_active)                            AS alarms_active,
                    count(*) FILTER (WHERE a.is_active AND a.severity='critical')  AS alarms_critical_active
                  FROM public.alarms a
                  JOIN public.asset_locations al
                    ON al.asset_type=a.asset_type AND al.asset_id=a.asset_id
                  GROUP BY al.location_id
                )
                SELECT
                  l.id, l.code, l.name,
                  COALESCE(c.assets_total,0)        AS assets_total,
                  COALESCE(c.tanks_count,0)         AS tanks_count,
                  COALESCE(c.pumps_count,0)         AS pumps_count,
                  COALESCE(c.valves_count,0)        AS valves_count,
                  COALESCE(c.manifolds_count,0)     AS manifolds_count,
                  COALESCE(am.alarms_active,0)      AS alarms_active,
                  COALESCE(am.alarms_critical_active,0) AS alarms_critical_active
                FROM public.locations l
                LEFT JOIN counts c ON c.location_id = l.id
                LEFT JOIN alarms am ON am.location_id = l.id
                ORDER BY l.name;
            """)
            rows = cur.fetchall()
            # Para que el tipado no moleste al client, casteamos a LocationWithStats
            return [LocationWithStats(**r).dict() for r in rows]


# ------------------------------------------------------------
# GET /infra/locations/{loc_id}/assets
# Devuelve [{type: 'tank', items: [...]}, ...] para pintar grupos.
# ------------------------------------------------------------
@router.get(
    "/locations/{loc_id}/assets",
    response_model=List[AssetGroup]
)
def location_assets(
    loc_id: int,
    include: Optional[List[AssetType]] = Query(
        default=None,
        description="Filtra tipos: repetir ?include=tank&include=pump"
    )
):
    """
    Devuelve [{type: 'tank', items: [...]}, ...] para que el front pinte grupos.
    """
    valid: set[str] = {'tank','pump','valve','manifold'}
    inc = set(include) & valid if include else None

    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        # Verificamos que exista la location
        cur.execute("SELECT 1 FROM public.locations WHERE id=%s;", (loc_id,))
        if cur.fetchone() is None:
            raise HTTPException(404, "location not found")

        if inc:
            cur.execute("""
                SELECT al.asset_type, al.asset_id, COALESCE(n.name, CONCAT(al.asset_type,' ',al.asset_id)) AS name, n.code
                FROM public.asset_locations al
                LEFT JOIN public.v_asset_nodes n
                  ON n.type = al.asset_type AND n.asset_id = al.asset_id
                WHERE al.location_id = %s
                  AND al.asset_type = ANY(%s)
                ORDER BY al.asset_type, name;
            """, (loc_id, list(inc)))
        else:
            cur.execute("""
                SELECT al.asset_type, al.asset_id, COALESCE(n.name, CONCAT(al.asset_type,' ',al.asset_id)) AS name, n.code
                FROM public.asset_locations al
                LEFT JOIN public.v_asset_nodes n
                  ON n.type = al.asset_type AND n.asset_id = al.asset_id
                WHERE al.location_id = %s
                ORDER BY al.asset_type, name;
            """, (loc_id,))

        rows = cur.fetchall()
        groups: dict[str, list[AssetItem]] = defaultdict(list)
        for r in rows:
            groups[r["asset_type"]].append(AssetItem(id=r["asset_id"], name=r["name"], code=r["code"]).dict())

        # Orden estable por tipo
        return [AssetGroup(type=k, items=v).dict() for k, v in sorted(groups.items())]


# ------------------------------------------------------------
# GET /infra/locations/{loc_id}/summary
# Lee la vista v_location_summary_30d (LATERAL + last_seen histórico).
# ------------------------------------------------------------
@router.get(
    "/locations/{loc_id}/summary",
    response_model=LocationSummary
)
def location_summary(loc_id: int):
    """
    Lee la vista v_location_summary_30d (LATERAL + last_seen histórico).
    """
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        cur.execute("""
          SELECT * FROM public.v_location_summary_30d WHERE location_id=%s;
        """, (loc_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "location not found")
        return row


# ------------------------------------------------------------
# GET /infra/locations/{loc_id}/tree
# Paquete completo para el front: location + summary + assets agrupados.
# ------------------------------------------------------------
@router.get(
    "/locations/{loc_id}/tree",
    response_model=LocationTree
)
def location_tree(loc_id: int):
    """
    Paquete completo para el front: location + summary + assets agrupados.
    """
    with get_conn() as c, c.cursor(row_factory=dict_row) as cur:
        # summary + datos de location
        cur.execute("SELECT * FROM public.v_location_summary_30d WHERE location_id=%s;", (loc_id,))
        summary = cur.fetchone()
        if not summary:
            raise HTTPException(404, "location not found")

        # assets agrupados
        cur.execute("""
            SELECT al.asset_type, al.asset_id, COALESCE(n.name, CONCAT(al.asset_type,' ',al.asset_id)) AS name, n.code
            FROM public.asset_locations al
            LEFT JOIN public.v_asset_nodes n
              ON n.type = al.asset_type AND n.asset_id = al.asset_id
            WHERE al.location_id = %s
            ORDER BY al.asset_type, name;
        """, (loc_id,))
        rows = cur.fetchall()

        groups: dict[str, list[AssetItem]] = defaultdict(list)
        for r in rows:
            groups[r["asset_type"]].append(AssetItem(id=r["asset_id"], name=r["name"], code=r["code"]).dict())

        return LocationTree(
            location=Location(id=summary["location_id"], code=summary["location_code"], name=summary["location_name"]),
            summary=LocationSummary(**summary),
            assets=[AssetGroup(type=k, items=v) for k, v in sorted(groups.items())]
        )
