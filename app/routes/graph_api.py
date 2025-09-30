# app/routes/graph_api.py
from __future__ import annotations

from typing import List, Optional, Dict, Tuple, Any, Set

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from psycopg.rows import dict_row

from app.core.db import get_conn
from app.core.security import device_id_dep
from app.core.tenancy import require_org  # 👈 asegura GUC app.org_id por request

router = APIRouter()

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _get_layout_map() -> Dict[str, Tuple[float, float]]:
    """
    Lee posiciones guardadas en DB para la org actual (según app.org_id)
    y devuelve {node_uid: (x, y)}.
    """
    layout_map: Dict[str, Tuple[float, float]] = {}
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT node_uid, x, y
            FROM public.asset_layouts
            WHERE org_id = current_setting('app.org_id')::bigint
            """
        )
        for r in cur.fetchall() or []:
            layout_map[str(r["node_uid"])] = (float(r["x"]), float(r["y"]))
    return layout_map


def _node_uid(r: Dict[str, Any]) -> str:
    """
    ID único EXACTO, consistente con edges y con asset_layouts.node_uid:
      - si hay code:  "type:code"
      - si no:        "type_<asset_id>"
    """
    code = r.get("code")
    t = r.get("type")
    aid = r.get("asset_id")
    return f"{t}:{code}" if code else f"{t}_{aid}"


def _endpoint_key(t: Optional[str], asset_id: Any, code: Optional[str]) -> str:
    """Clave para lookup rápido de extremos de aristas."""
    t = (t or "node").lower()
    return f"{t}:{code}" if code else f"{t}_{asset_id}"


def _filter_edges_by_nodes(raw_edges: List[Dict[str, Any]], node_keys: Set[str]) -> List[Dict[str, Any]]:
    """
    Mantiene solo edges cuyos extremos existan en el conjunto de nodos cargados.
    Blinda ante vistas sin filtro por org o datos viejos.
    """
    kept: List[Dict[str, Any]] = []
    for e in raw_edges or []:
        src_key = _endpoint_key(e.get("from_type"), e.get("from_id"), e.get("from_code"))
        dst_key = _endpoint_key(e.get("to_type"),   e.get("to_id"),   e.get("to_code"))
        if src_key in node_keys and dst_key in node_keys and (e.get("is_active", True) is True):
            kept.append(e)
    return kept


# ---------------------------------------------------------------------
# Graph (combinado)
# ---------------------------------------------------------------------

@router.get("/graph")
def graph_all(
    _dev=Depends(device_id_dep),
    _org_id: int = Depends(require_org),  # 👈 setea app.org_id por request
):
    """
    Devuelve:
      {
        "nodes": [
          { "id": "type:code" | "type_<asset_id>", "type": "...", "name": "...", (x,y?) ... }
        ],
        "edges": ["SRC>DST", ...]
      }
    Inyecta (x,y) desde public.asset_layouts si existen para la org del token.
    Filtra NODOS y EDGES por org_id (vía locations) sin cambiar tu modelo.
    """
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            # Nodos SOLO de la org actual (via locations)
            cur.execute(
                """
                SELECT n.*
                FROM public.v_asset_nodes n
                JOIN public.locations l ON l.id = n.location_id
                WHERE l.org_id = current_setting('app.org_id')::bigint
                ORDER BY n.type, n.name;
                """
            )
            raw_nodes = cur.fetchall() or []

            # Edges SOLO dentro de la org (ambos extremos)
            cur.execute(
                """
                SELECT e.*
                FROM public.v_topology_edges e
                JOIN public.v_asset_nodes nf
                  ON nf.type = e.from_type AND nf.asset_id = e.from_id
                JOIN public.locations lf ON lf.id = nf.location_id
                JOIN public.v_asset_nodes nt
                  ON nt.type = e.to_type   AND nt.asset_id = e.to_id
                JOIN public.locations lt ON lt.id = nt.location_id
                WHERE e.is_active
                  AND lf.org_id = current_setting('app.org_id')::bigint
                  AND lt.org_id = current_setting('app.org_id')::bigint
                ORDER BY e.id;
                """
            )
            all_edges = cur.fetchall() or []

            # Por si viene algo “colado”, reforzamos por los nodos cargados:
            node_keys: Set[str] = set()
            for r in raw_nodes:
                node_keys.add(_node_uid(r))
            raw_edges = _filter_edges_by_nodes(all_edges, node_keys)

            # Config de tanques filtrada por org actual
            cur.execute(
                """
                SELECT tc.tank_id, tc.low_pct, tc.high_pct, tc.low_low_pct, tc.high_high_pct
                FROM public.tank_config tc
                JOIN public.tanks t     ON t.id = tc.tank_id
                JOIN public.locations l ON l.id = t.location_id
                WHERE l.org_id = current_setting('app.org_id')::bigint
                """
            )
            tank_config = {row["tank_id"]: row for row in (cur.fetchall() or [])}

            # Últimos niveles de tanques (filtrados por org)
            cur.execute(
                """
                SELECT v.tank_id, v.level_percent
                FROM public.v_tank_latest v
                JOIN public.tanks t     ON t.id = v.tank_id
                JOIN public.locations l ON l.id = t.location_id
                WHERE l.org_id = current_setting('app.org_id')::bigint
                """
            )
            tank_levels = {row["tank_id"]: row["level_percent"] for row in (cur.fetchall() or [])}

            # Último estado de bombas (filtrado por org)
            cur.execute(
                """
                SELECT v.pump_id, v.is_on
                FROM public.v_pump_latest v
                JOIN public.pumps p     ON p.id = v.pump_id
                JOIN public.locations l ON l.id = p.location_id
                WHERE l.org_id = current_setting('app.org_id')::bigint
                """
            )
            pump_statuses = {row["pump_id"]: row["is_on"] for row in (cur.fetchall() or [])}

        # Layouts guardados (por org)
        layout_map = _get_layout_map()

        # Construcción de nodos de salida
        nodes: List[Dict[str, Any]] = []
        for r in raw_nodes:
            nid = _node_uid(r)
            node: Dict[str, Any] = {"id": nid, "type": r["type"], "name": r["name"]}

            # payload por tipo
            if r["type"] == "tank":
                tank_id = r["asset_id"]
                cfg = tank_config.get(tank_id)
                if cfg:
                    node["low_pct"] = cfg["low_pct"]
                    node["high_pct"] = cfg["high_pct"]
                    node["low_low_pct"] = cfg["low_low_pct"]
                    node["high_high_pct"] = cfg["high_high_pct"]
                node["level"] = tank_levels.get(tank_id, None)

            elif r["type"] == "pump":
                node["status"] = pump_statuses.get(r["asset_id"], False)  # False=apagada
                if r.get("rated_kw") is not None:
                    node["kW"] = r.get("rated_kw")

            elif r["type"] == "valve":
                if r.get("valve_state") is not None:
                    node["state"] = r.get("valve_state")

            # Posiciones (si están guardadas)
            if nid in layout_map:
                x, y = layout_map[nid]
                node["x"], node["y"] = x, y

            nodes.append(node)

        # Edges → formateo compacto "SRC>DST"
        edges: List[str] = []
        for e in raw_edges:
            src = f'{e.get("from_type")}:{e.get("from_code")}' if e.get("from_code") else f'{e.get("from_type")}_{e.get("from_id")}'
            dst = f'{e.get("to_type")}:{e.get("to_code")}'     if e.get("to_code")   else f'{e.get("to_type")}_{e.get("to_id")}'
            edges.append(f"{src}>{dst}")

        return {"nodes": nodes, "edges": edges}

    except Exception as e:
        raise HTTPException(500, f"graph_all failed: {e}")


# ---------------------------------------------------------------------
# Graph (endpoints separados: opcional / compat)
# ---------------------------------------------------------------------

@router.get("/graph/nodes")
def graph_nodes(
    _dev=Depends(device_id_dep),
    _org_id: int = Depends(require_org),
):
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                  n.type,
                  n.asset_id AS id,
                  n.name, n.code,
                  n.level_ratio, n.capacity_liters,
                  n.pump_status, n.rated_kw, n.valve_state,
                  n.location_id, n.location_code, n.location_name
                FROM public.v_asset_nodes n
                JOIN public.locations l ON l.id = n.location_id
                WHERE l.org_id = current_setting('app.org_id')::bigint
                ORDER BY n.type, n.name;
                """
            )
            return cur.fetchall() or []
    except Exception as e:
        raise HTTPException(500, f"graph_nodes failed: {e}")


@router.get("/graph/edges")
def graph_edges(
    _dev=Depends(device_id_dep),
    _org_id: int = Depends(require_org),
):
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT e.id,
                       e.from_type, e.from_id, e.from_name, e.from_code,
                       e.to_type,   e.to_id,   e.to_name,   e.to_code,
                       e.pipe_diameter_mm, e.length_m, e.is_active
                FROM public.v_topology_edges e
                JOIN public.v_asset_nodes nf
                  ON nf.type = e.from_type AND nf.asset_id = e.from_id
                JOIN public.locations lf ON lf.id = nf.location_id
                JOIN public.v_asset_nodes nt
                  ON nt.type = e.to_type AND nt.asset_id = e.to_id
                JOIN public.locations lt ON lt.id = nt.location_id
                WHERE e.is_active
                  AND lf.org_id = current_setting('app.org_id')::bigint
                  AND lt.org_id = current_setting('app.org_id')::bigint
                ORDER BY e.id;
                """
            )
            return cur.fetchall() or []
    except Exception as e:
        raise HTTPException(500, f"graph_edges failed: {e}")


# ---------------------------------------------------------------------
# Layout Autosave (DB)
# ---------------------------------------------------------------------

class NodePos(BaseModel):
    id: str
    x: float
    y: float
    updated_by: Optional[str] = None  # opcional (ej. email o "ui-embed")


@router.post("/layout")
def save_layout(
    items: List[NodePos],
    _dev=Depends(device_id_dep),
    _org_id: int = Depends(require_org),
):
    """
    Guarda posiciones (UPSERT) en public.asset_layouts para la org del token.
    Body: [{ "id": "<node_uid>", "x": <num>, "y": <num>, "updated_by": "..." }, ...]
    """
    if not items:
        return {"ok": True, "saved": 0}

    try:
        with get_conn() as conn, conn.cursor() as cur:
            for it in items:
                cur.execute(
                    """
                    INSERT INTO public.asset_layouts (org_id, node_uid, x, y, updated_by)
                    VALUES (current_setting('app.org_id')::bigint, %s, %s, %s, %s)
                    ON CONFLICT (org_id, node_uid)
                    DO UPDATE SET x = EXCLUDED.x,
                                  y = EXCLUDED.y,
                                  updated_by = EXCLUDED.updated_by,
                                  updated_at = now()
                    """,
                    (it.id, it.x, it.y, it.updated_by or "ui-embed"),
                )
        return {"ok": True, "saved": len(items)}
    except Exception as e:
        raise HTTPException(500, f"save_layout failed: {e}")


@router.get("/layout")
def get_layout(
    _dev=Depends(device_id_dep),
    _org_id: int = Depends(require_org),
):
    """
    Devuelve todas las posiciones guardadas para la org del token (útil para debug).
    """
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT node_uid AS id, x, y, updated_by, updated_at
                FROM public.asset_layouts
                WHERE org_id = current_setting('app.org_id')::bigint
                ORDER BY updated_at DESC
                """
            )
            return cur.fetchall()
    except Exception as e:
        raise HTTPException(500, f"get_layout failed: {e}")
