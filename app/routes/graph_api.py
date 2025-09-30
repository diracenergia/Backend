# app/routes/graph_api.py
from __future__ import annotations

from typing import List, Optional, Dict, Tuple, Any, Set

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from psycopg.rows import dict_row

from app.core.db import get_conn
from app.core.security import device_id_dep
from app.core.tenancy import require_org  # asegura GUC app.org_id por request

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


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        return int(float(s)) if "." in s else int(s)
    except Exception:
        return None


def _node_uid(row: Dict[str, Any]) -> str:
    """
    UID consistente para nodos y layouts:
      - usa code si existe
      - si no, 'type_<id>'
    OJO: v_nodes_info expone 'id' (no 'asset_id').
    """
    code = row.get("code")
    t = (row.get("type") or "node").lower()
    # compat: algunos selects traen 'asset_id' (no debería con v_nodes_info)
    aid = row.get("id", row.get("asset_id"))
    return f"{t}:{code}" if (code not in (None, "")) else f"{t}_{aid}"


def _endpoint_key(t: Optional[str], asset_id: Any, code: Optional[str]) -> str:
    """Clave para lookup rápido de extremos de aristas."""
    t = (t or "node").lower()
    return f"{t}:{code}" if (code not in (None, "")) else f"{t}_{asset_id}"


def _filter_edges_by_nodes(raw_edges: List[Dict[str, Any]], node_keys: Set[str]) -> List[Dict[str, Any]]:
    """
    Mantiene solo edges cuyos extremos existan en el conjunto de nodos cargados.
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
    _org_id: int = Depends(require_org),
):
    """
    Devuelve:
      {
        "nodes": [
          { "id": "type:code" | "type_<id>", "type": "...", "name": "...", (x,y?) ... }
        ],
        "edges": ["SRC>DST", ...]
      }
    - Carga nodos desde v_nodes_info SOLO de la org actual.
    - Edges desde v_edges filtrados por org.
    - Payloads (tanques/bombas) restringidos a ids presentes en nodos.
    - Inyecta (x,y) desde public.asset_layouts por org.
    """
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            # 1) Nodos de la org actual (vista canónica con org_id)
            cur.execute(
                """
                SELECT type, id, name, org_id, code, location_code, location_name
                FROM public.v_nodes_info
                WHERE org_id = current_setting('app.org_id')::bigint
                ORDER BY type, name;
                """
            )
            raw_nodes = cur.fetchall() or []

            node_keys: Set[str] = set()
            tank_ids: Set[int] = set()
            pump_ids: Set[int] = set()

            for r in raw_nodes:
                nid = _node_uid(r)
                node_keys.add(nid)

                t = (r.get("type") or "").lower()
                aid = _safe_int(r.get("id"))
                if t == "tank" and aid is not None:
                    tank_ids.add(aid)
                elif t == "pump" and aid is not None:
                    pump_ids.add(aid)

            # 2) Edges por org (usa v_edges con org_id)
            cur.execute(
                """
                SELECT id,
                       from_type, from_id, from_name, from_code,
                       to_type,   to_id,   to_name,   to_code,
                       pipe_diameter_mm, length_m, is_active
                  FROM public.v_edges
                 WHERE org_id = current_setting('app.org_id')::bigint
                   AND is_active
                 ORDER BY id;
                """
            )
            all_edges = cur.fetchall() or []
            raw_edges = _filter_edges_by_nodes(all_edges, node_keys)

            # 3) Payloads: restringidos por los ids de nuestros nodos
            tank_config: Dict[int, Dict[str, Any]] = {}
            tank_levels: Dict[int, Any] = {}
            pump_statuses: Dict[int, Any] = {}

            if tank_ids:
                # Config de alarmas/niveles de tanques (por ids válidos de la org)
                cur.execute(
                    """
                    SELECT tc.tank_id, tc.low_pct, tc.high_pct, tc.low_low_pct, tc.high_high_pct
                      FROM public.tank_config tc
                      JOIN public.v_nodes n
                        ON n.type = 'tank' AND n.id = tc.tank_id
                     WHERE n.org_id = current_setting('app.org_id')::bigint
                    """
                )
                for r in cur.fetchall() or []:
                    tid = _safe_int(r.get("tank_id"))
                    if tid is not None and tid in tank_ids:
                        tank_config[tid] = r

                # Últimas lecturas de tanques (join contra nodos de la org)
                cur.execute(
                    """
                    SELECT v.tank_id, v.level_percent
                      FROM public.v_tank_latest v
                      JOIN public.v_nodes n
                        ON n.type = 'tank' AND n.id = v.tank_id
                     WHERE n.org_id = current_setting('app.org_id')::bigint
                    """
                )
                for r in cur.fetchall() or []:
                    tid = _safe_int(r.get("tank_id"))
                    if tid is not None and tid in tank_ids:
                        tank_levels[tid] = r.get("level_percent")

            if pump_ids:
                # Últimos estados de bombas (join contra nodos de la org)
                cur.execute(
                    """
                    SELECT v.pump_id, v.is_on
                      FROM public.v_pump_latest v
                      JOIN public.v_nodes n
                        ON n.type = 'pump' AND n.id = v.pump_id
                     WHERE n.org_id = current_setting('app.org_id')::bigint
                    """
                )
                for r in cur.fetchall() or []:
                    pid = _safe_int(r.get("pump_id"))
                    if pid is not None and pid in pump_ids:
                        pump_statuses[pid] = r.get("is_on")

        # 4) Layouts por org
        layout_map = _get_layout_map()

        # 5) Construcción de nodos de salida
        nodes: List[Dict[str, Any]] = []
        for r in raw_nodes:
            t = (r.get("type") or "").lower()
            nid = _node_uid(r)
            node: Dict[str, Any] = {"id": nid, "type": r.get("type"), "name": r.get("name")}

            # payload por tipo
            if t == "tank":
                tank_id = _safe_int(r.get("id"))
                if tank_id is not None:
                    cfg = tank_config.get(tank_id)
                    if cfg:
                        node["low_pct"] = cfg.get("low_pct")
                        node["high_pct"] = cfg.get("high_pct")
                        node["low_low_pct"] = cfg.get("low_low_pct")
                        node["high_high_pct"] = cfg.get("high_high_pct")
                    node["level"] = tank_levels.get(tank_id, None)

            elif t == "pump":
                pid = _safe_int(r.get("id"))
                if pid is not None:
                    node["status"] = bool(pump_statuses.get(pid, False))  # False=apagada

            # Posiciones (si están guardadas)
            if nid in layout_map:
                x, y = layout_map[nid]
                node["x"], node["y"] = x, y

            # Info de localización disponible en v_nodes_info
            if r.get("location_code") is not None:
                node["location_code"] = r.get("location_code")
            if r.get("location_name") is not None:
                node["location_name"] = r.get("location_name")

            nodes.append(node)

        # 6) Edges → "SRC>DST"
        edges: List[str] = []
        for e in raw_edges:
            src = f'{e.get("from_type")}:{e.get("from_code")}' if e.get("from_code") not in (None, "") else f'{e.get("from_type")}_{e.get("from_id")}'
            dst = f'{e.get("to_type")}:{e.get("to_code")}'     if e.get("to_code")   not in (None, "") else f'{e.get("to_type")}_{e.get("to_id")}'
            edges.append(f"{src}>{dst}")

        return {"nodes": nodes, "edges": edges}

    except HTTPException:
        raise
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
    """
    Nodos crudos SOLO de la org actual (v_nodes_info).
    """
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT type, id AS asset_id, name, code,
                       NULL::numeric AS level_ratio,
                       NULL::numeric AS capacity_liters,
                       NULL::boolean AS pump_status,
                       NULL::numeric AS rated_kw,
                       NULL::text    AS valve_state,
                       NULL::bigint  AS location_id,
                       location_code, location_name
                  FROM public.v_nodes_info
                 WHERE org_id = current_setting('app.org_id')::bigint
                 ORDER BY type, name
                """
            )
            return cur.fetchall() or []
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"graph_nodes failed: {e}")


@router.get("/graph/edges")
def graph_edges(
    _dev=Depends(device_id_dep),
    _org_id: int = Depends(require_org),
):
    """
    Edges activos SOLO dentro de la org actual (v_edges).
    """
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id,
                       from_type, from_id, from_name, from_code,
                       to_type,   to_id,   to_name,   to_code,
                       pipe_diameter_mm, length_m, is_active
                  FROM public.v_edges
                 WHERE org_id = current_setting('app.org_id')::bigint
                   AND is_active
                 ORDER BY id
                """
            )
            edges = cur.fetchall() or []
            # (node filter already guaranteed by org)
            return edges
    except HTTPException:
        raise
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
    except HTTPException:
        raise
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
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"get_layout failed: {e}")
