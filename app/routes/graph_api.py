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


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        # Evitar castear strings vacíos u otros
        s = str(v).strip()
        if s == "":
            return None
        return int(float(s)) if "." in s else int(s)
    except Exception:
        return None


def _load_nodes_filtered(cur) -> List[Dict[str, Any]]:
    """
    Carga nodos desde v_asset_nodes priorizando filtro por org_id.
    Si la vista no expone org_id, carga todo y dejamos que las uniones/joins
    y el post-filtrado hagan el resto.
    """
    try:
        cur.execute(
            """
            SELECT n.*
            FROM v_asset_nodes n
            WHERE n.org_id = current_setting('app.org_id')::bigint
            ORDER BY n.type, n.name;
            """
        )
        rows = cur.fetchall()
        return rows or []
    except Exception:
        # Vista sin org_id o sin permiso: caemos a SELECT plano
        cur.execute("SELECT * FROM v_asset_nodes ORDER BY type, name;")
        return cur.fetchall() or []


def _node_uid(row: Dict[str, Any]) -> str:
    """UID consistente para nodos y layouts: usa code si existe; si no, asset_id."""
    code = row.get("code")
    t = row.get("type") or "node"
    aid = row.get("asset_id")
    return f"{t}:{code}" if (code not in (None, "")) else f"{t}_{aid}"


def _endpoint_key(t: Optional[str], asset_id: Any, code: Optional[str]) -> str:
    """Clave para lookup rápido de extremos de aristas."""
    t = t or "node"
    return f"{t}:{code}" if (code not in (None, "")) else f"{t}_{asset_id}"


def _filter_edges_by_nodes(raw_edges: List[Dict[str, Any]], node_keys: Set[str]) -> List[Dict[str, Any]]:
    """
    Mantiene solo edges cuyos extremos existan en el conjunto de nodos cargados.
    Esto blinda contra vistas de edges sin filtro por org.
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
    _org_id: int = Depends(require_org),  # 👈 asegura el GUC para todas las consultas
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
    Además, blinda por org en Python: edges y payloads se limitan a IDs presentes en nodos.
    """
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            # Nodos (preferentemente filtrados por org_id desde SQL)
            raw_nodes = _load_nodes_filtered(cur)

            # Índices/sets por tipo para refinar payloads y edges
            node_keys: Set[str] = set()
            tank_ids: Set[int] = set()
            pump_ids: Set[int] = set()

            for r in raw_nodes:
                nid = _node_uid(r)
                node_keys.add(nid)

                t = (r.get("type") or "").lower()
                aid = _safe_int(r.get("asset_id"))
                if t == "tank" and aid is not None:
                    tank_ids.add(aid)
                elif t == "pump" and aid is not None:
                    pump_ids.add(aid)

            # Edges: versión con JOIN a v_asset_nodes (como tenías originalmente),
            # que además fuerza org via los nodos (aunque la vista no filtre sola).
            cur.execute(
                """
                SELECT e.*
                FROM v_topology_edges e
                JOIN v_asset_nodes nf
                  ON nf.type = e.from_type AND nf.asset_id = e.from_id
                JOIN v_asset_nodes nt
                  ON nt.type = e.to_type   AND nt.asset_id = e.to_id
                WHERE e.is_active
                ORDER BY e.id;
                """
            )
            all_edges = cur.fetchall() or []
            # Por si v_topology_edges no filtra correctamente, reforzamos:
            raw_edges = _filter_edges_by_nodes(all_edges, node_keys)

            # Payloads de tanques/bombas: usar JOIN contra v_asset_nodes (filtra por org)
            tank_config: Dict[int, Dict[str, Any]] = {}
            tank_levels: Dict[int, Any] = {}
            pump_statuses: Dict[int, Any] = {}

            if tank_ids:
                cur.execute(
                    """
                    SELECT tc.tank_id, tc.low_pct, tc.high_pct, tc.low_low_pct, tc.high_high_pct
                    FROM public.tank_config tc
                    JOIN v_asset_nodes n
                      ON n.type = 'tank' AND n.asset_id = tc.tank_id
                    """
                )
                rows = cur.fetchall() or []
                # Quedarnos solo con los que están en nuestros nodos (por si la vista no filtró org)
                for r in rows:
                    tid = _safe_int(r.get("tank_id"))
                    if tid is not None and tid in tank_ids:
                        tank_config[tid] = r

                cur.execute(
                    """
                    SELECT v.tank_id, v.level_percent
                    FROM v_tank_latest v
                    JOIN v_asset_nodes n
                      ON n.type = 'tank' AND n.asset_id = v.tank_id
                    """
                )
                rows = cur.fetchall() or []
                for r in rows:
                    tid = _safe_int(r.get("tank_id"))
                    if tid is not None and tid in tank_ids:
                        tank_levels[tid] = r.get("level_percent")

            if pump_ids:
                cur.execute(
                    """
                    SELECT v.pump_id, v.is_on
                    FROM v_pump_latest v
                    JOIN v_asset_nodes n
                      ON n.type = 'pump' AND n.asset_id = v.pump_id
                    """
                )
                rows = cur.fetchall() or []
                for r in rows:
                    pid = _safe_int(r.get("pump_id"))
                    if pid is not None and pid in pump_ids:
                        pump_statuses[pid] = r.get("is_on")

        # Layouts guardados (por org)
        layout_map = _get_layout_map()

        # Construcción de nodos de salida
        nodes: List[Dict[str, Any]] = []
        for r in raw_nodes:
            t = (r.get("type") or "").lower()
            nid = _node_uid(r)
            node: Dict[str, Any] = {"id": nid, "type": r.get("type"), "name": r.get("name")}

            # payload por tipo
            if t == "tank":
                tank_id = _safe_int(r.get("asset_id"))
                if tank_id is not None:
                    cfg = tank_config.get(tank_id)
                    if cfg:
                        node["low_pct"] = cfg.get("low_pct")
                        node["high_pct"] = cfg.get("high_pct")
                        node["low_low_pct"] = cfg.get("low_low_pct")
                        node["high_high_pct"] = cfg.get("high_high_pct")
                    node["level"] = tank_levels.get(tank_id, None)

            elif t == "pump":
                pid = _safe_int(r.get("asset_id"))
                if pid is not None:
                    node["status"] = bool(pump_statuses.get(pid, False))  # False=apagada
                rk = r.get("rated_kw")
                if rk is not None:
                    node["kW"] = rk

            elif t == "valve":
                vs = r.get("valve_state")
                if vs is not None:
                    node["state"] = vs

            # Posiciones (si están guardadas)
            if nid in layout_map:
                x, y = layout_map[nid]
                node["x"], node["y"] = x, y

            # Info de localización (si la vista la expone)
            for k in ("location_id", "location_code", "location_name"):
                if k in r and r[k] is not None:
                    node[k] = r[k]

            nodes.append(node)

        # Edges → formateo compacto "SRC>DST"
        edges: List[str] = []
        for e in raw_edges:
            src = f'{e.get("from_type")}:{e.get("from_code")}' if e.get("from_code") not in (None, "") else f'{e.get("from_type")}_{e.get("from_id")}'
            dst = f'{e.get("to_type")}:{e.get("to_code")}'     if e.get("to_code")   not in (None, "") else f'{e.get("to_type")}_{e.get("to_id")}'
            edges.append(f"{src}>{dst}")

        return {"nodes": nodes, "edges": edges}

    except HTTPException:
        raise
    except Exception as e:
        # No exponemos stack, pero sí un mensaje claro (te ayuda en PS con Invoke-WebRequest)
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
    Devuelve nodos “crudos” al estilo de la vista, pero garantizando que
    provengan de la org activa (mismo criterio que /graph).
    """
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            rows = _load_nodes_filtered(cur)
            out = []
            for r in rows:
                out.append({
                    "type": r.get("type"),
                    "id": r.get("asset_id"),
                    "name": r.get("name"),
                    "code": r.get("code"),
                    "level_ratio": r.get("level_ratio"),
                    "capacity_liters": r.get("capacity_liters"),
                    "pump_status": r.get("pump_status"),
                    "rated_kw": r.get("rated_kw"),
                    "valve_state": r.get("valve_state"),
                    "location_id": r.get("location_id"),
                    "location_code": r.get("location_code"),
                    "location_name": r.get("location_name"),
                })
            return out
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
    Devuelve edges activos, **filtrados** para que ambos extremos pertenezcan
    a los nodos de la org actual (blinda ante vistas sin filtro por org).
    """
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            nodes = _load_nodes_filtered(cur)
            node_keys = {_node_uid(r) for r in nodes}

            cur.execute(
                """
                SELECT e.id,
                       e.from_type, e.from_id, e.from_name, e.from_code,
                       e.to_type,   e.to_id,   e.to_name,   e.to_code,
                       e.pipe_diameter_mm, e.length_m, e.is_active
                FROM v_topology_edges e
                JOIN v_asset_nodes nf
                  ON nf.type = e.from_type AND nf.asset_id = e.from_id
                JOIN v_asset_nodes nt
                  ON nt.type = e.to_type   AND nt.asset_id = e.to_id
                WHERE e.is_active
                ORDER BY e.id;
                """
            )
            edges = cur.fetchall() or []
            edges = _filter_edges_by_nodes(edges, node_keys)
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
