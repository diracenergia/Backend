# app/routes/graph_api.py
from __future__ import annotations
from typing import List, Optional, Dict, Tuple, Any, Set

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from psycopg.rows import dict_row

from app.core.db import get_conn
from app.core.security import device_id_dep
from app.core.tenancy import require_org  # fija org por request

router = APIRouter()

# ----------------------------------
# Helpers
# ----------------------------------

def _set_org(cur, org_id: int) -> None:
    """
    Fuerza el GUC en ESTA conexión (pool-safe).
    is_local=True -> no “contagia” fuera de la transacción.
    """
    cur.execute("SELECT set_config('app.org_id', %s, true)", (str(int(org_id)),))

def _get_layout_map() -> Dict[str, Tuple[float, float]]:
    layout_map: Dict[str, Tuple[float, float]] = {}
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        # ⚠️ Si alguien llama esto sin haber seteado org en esta conexión,
        # usamos missing_ok y caemos a org_id NULL => no devuelve nada.
        cur.execute(
            """
            SELECT node_uid, x, y
            FROM public.asset_layouts
            WHERE org_id = current_setting('app.org_id', true)::bigint
            """
        )
        for r in cur.fetchall() or []:
            try:
                layout_map[str(r["node_uid"])] = (float(r["x"]), float(r["y"]))
            except Exception:
                pass
    return layout_map

def _node_uid(r: Dict[str, Any]) -> str:
    t = r.get("type") or "node"
    aid = r.get("asset_id")
    code = r.get("code")
    return f"{t}:{code}" if code else f"{t}_{aid}"

def _endpoint_key(t: Optional[str], asset_id: Any, code: Optional[str]) -> str:
    t = (t or "node").lower()
    return f"{t}:{code}" if code else f"{t}_{asset_id}"

def _filter_edges_by_nodes(raw_edges: List[Dict[str, Any]], node_keys: Set[str]) -> List[Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    for e in raw_edges or []:
        src = _endpoint_key(e.get("from_type"), e.get("from_id"), e.get("from_code"))
        dst = _endpoint_key(e.get("to_type"),   e.get("to_id"),   e.get("to_code"))
        if src in node_keys and dst in node_keys and (e.get("is_active", True) is True):
            kept.append(e)
    return kept

# ----------------------------------
# Graph combinado
# ----------------------------------

@router.get("/graph")
def graph_all(
    _dev=Depends(device_id_dep),
    _org_id: int = Depends(require_org),
):
    """
    Devuelve {"nodes": [...], "edges": [...]}
    - Setea app.org_id en la conexión (pool-safe).
    - Filtra nodos/edges por org vía locations.
    - Inyecta layout por org.
    """
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            # Fuerza el org_id en ESTA conexión
            _set_org(cur, _org_id)

            # NODOS de la org
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

            # EDGES dentro de la misma org (ambos extremos)
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

            node_keys: Set[str] = {_node_uid(r) for r in raw_nodes}
            raw_edges = _filter_edges_by_nodes(all_edges, node_keys)

            # TANQUES: config y latest por org
            cur.execute(
                """
                SELECT tc.tank_id, tc.low_pct, tc.high_pct, tc.low_low_pct, tc.high_high_pct
                  FROM public.tank_config tc
                  JOIN public.tanks t     ON t.id = tc.tank_id
                  JOIN public.locations l ON l.id = t.location_id
                 WHERE l.org_id = current_setting('app.org_id')::bigint
                """
            )
            tank_cfg_rows = cur.fetchall() or []
            tank_config = {r["tank_id"]: r for r in tank_cfg_rows if r.get("tank_id") is not None}

            cur.execute(
                """
                SELECT v.tank_id, v.level_percent
                  FROM public.v_tank_latest v
                  JOIN public.tanks t     ON t.id = v.tank_id
                  JOIN public.locations l ON l.id = t.location_id
                 WHERE l.org_id = current_setting('app.org_id')::bigint
                """
            )
            tank_levels = {r["tank_id"]: r.get("level_percent") for r in (cur.fetchall() or []) if r.get("tank_id") is not None}

            # BOMBAS: latest por org
            cur.execute(
                """
                SELECT v.pump_id, v.is_on
                  FROM public.v_pump_latest v
                  JOIN public.pumps p     ON p.id = v.pump_id
                  JOIN public.locations l ON l.id = p.location_id
                 WHERE l.org_id = current_setting('app.org_id')::bigint
                """
            )
            pump_statuses = {r["pump_id"]: r.get("is_on") for r in (cur.fetchall() or []) if r.get("pump_id") is not None}

        # Layouts (se cargan con su propia conexión; ya usan current_setting(..., true))
        layout_map = _get_layout_map()

        # Construcción de salida
        nodes: List[Dict[str, Any]] = []
        for r in raw_nodes:
            t = r.get("type")
            if not t:
                # nodo inválido, lo saltamos
                continue
            nid = _node_uid(r)
            node: Dict[str, Any] = {"id": nid, "type": t, "name": r.get("name")}

            if t == "tank":
                tid = r.get("asset_id")
                if tid in tank_config:
                    cfg = tank_config[tid]
                    node.update({
                        "low_pct": cfg.get("low_pct"),
                        "high_pct": cfg.get("high_pct"),
                        "low_low_pct": cfg.get("low_low_pct"),
                        "high_high_pct": cfg.get("high_high_pct"),
                    })
                node["level"] = tank_levels.get(tid)

            elif t == "pump":
                node["status"] = bool(pump_statuses.get(r.get("asset_id"), False))
                if r.get("rated_kw") is not None:
                    node["kW"] = r.get("rated_kw")

            elif t == "valve":
                if r.get("valve_state") is not None:
                    node["state"] = r.get("valve_state")

            # posiciones
            if nid in layout_map:
                x, y = layout_map[nid]
                node["x"], node["y"] = x, y

            nodes.append(node)

        edges: List[str] = []
        for e in raw_edges:
            src = f'{e.get("from_type")}:{e.get("from_code")}' if e.get("from_code") else f'{e.get("from_type")}_{e.get("from_id")}'
            dst = f'{e.get("to_type")}:{e.get("to_code")}'     if e.get("to_code")   else f'{e.get("to_type")}_{e.get("to_id")}'
            edges.append(f"{src}>{dst}")

        return {"nodes": nodes, "edges": edges}

    except HTTPException:
        raise
    except Exception as e:
        # Mensaje simple (para verlo desde PowerShell)
        raise HTTPException(500, f"graph_all failed: {e}")

# ----------------------------------
# Endpoints separados
# ----------------------------------

@router.get("/graph/nodes")
def graph_nodes(
    _dev=Depends(device_id_dep),
    _org_id: int = Depends(require_org),
):
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            _set_org(cur, _org_id)
            cur.execute(
                """
                SELECT n.type, n.asset_id AS id, n.name, n.code,
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
            _set_org(cur, _org_id)
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
                    ON nt.type = e.to_type   AND nt.asset_id = e.to_id
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

# ----------------------------------
# Layout (por org)
# ----------------------------------

class NodePos(BaseModel):
    id: str
    x: float
    y: float
    updated_by: Optional[str] = None

@router.post("/layout")
def save_layout(
    items: List[NodePos],
    _dev=Depends(device_id_dep),
    _org_id: int = Depends(require_org),
):
    if not items:
        return {"ok": True, "saved": 0}
    try:
        with get_conn() as conn, conn.cursor() as cur:
            _set_org(cur, _org_id)
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
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            _set_org(cur, _org_id)
            cur.execute(
                """
                SELECT node_uid AS id, x, y, updated_by, updated_at
                  FROM public.asset_layouts
                 WHERE org_id = current_setting('app.org_id')::bigint
                 ORDER BY updated_at DESC
                """
            )
            return cur.fetchall() or []
    except Exception as e:
        raise HTTPException(500, f"get_layout failed: {e}")
