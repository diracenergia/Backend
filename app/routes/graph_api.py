# app/routes/graph_api.py
from __future__ import annotations
from typing import List, Optional, Dict, Tuple, Any, Set

import logging
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from psycopg.rows import dict_row

from app.core.db import get_conn
from app.core.security import device_id_dep
from app.core.tenancy import require_org  # resuelve org del request (header/api-key/etc)

router = APIRouter()
log = logging.getLogger("rdls.infra")

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _set_org(cur, org_id: int) -> None:
    """
    Fija el GUC app.org_id **en ESTA conexión** (pool-safe).
    is_local=True evita que 'contagie' a otras conexiones.
    """
    cur.execute("SELECT set_config('app.org_id', %s, true)", (str(int(org_id)),))

def _safe_rollback(conn):
    try:
        conn.rollback()
    except Exception:
        pass

def _has_col(cur, relname: str, col: str, schema: str = "public") -> bool:
    """
    Chequea en information_schema si (schema.relname) tiene la columna.
    Evita disparar SELECTs que rompan por columnas inexistentes.
    """
    try:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            LIMIT 1
            """,
            (schema, relname, col),
        )
        return cur.fetchone() is not None
    except Exception:
        return False

def _node_uid(r: Dict[str, Any]) -> str:
    t = (r.get("type") or "node").lower()
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

def _load_nodes_filtered(conn, cur) -> List[Dict[str, Any]]:
    """
    Intenta filtrar v_asset_nodes por org sin romper la transacción:
    1) Si la vista tiene org_id -> filtro directo
    2) Si tiene location_id y existe locations.org_id -> join por locations
    3) Si no, fallback sin filtro (y más tarde blindamos edges/payload)
    Loguea la 'estrategia' elegida.
    """
    # 1) org_id en la vista
    try:
        if _has_col(cur, "v_asset_nodes", "org_id"):
            cur.execute(
                """
                SELECT n.*
                FROM v_asset_nodes n
                WHERE n.org_id = current_setting('app.org_id')::bigint
                ORDER BY n.type, n.name;
                """
            )
            rows = cur.fetchall() or []
            log.info("[infra] nodes via n.org_id (rows=%s)", len(rows))
            return rows
    except Exception as e:
        log.warning("[infra] nodes via n.org_id failed: %s", e)
        _safe_rollback(conn)

    # 2) join por locations (si ambas columnas existen)
    try:
        if _has_col(cur, "v_asset_nodes", "location_id") and _has_col(cur, "locations", "org_id"):
            cur.execute(
                """
                SELECT n.*
                FROM v_asset_nodes n
                JOIN public.locations l ON l.id = n.location_id
                WHERE l.org_id = current_setting('app.org_id')::bigint
                ORDER BY n.type, n.name;
                """
            )
            rows = cur.fetchall() or []
            log.info("[infra] nodes via JOIN locations (rows=%s)", len(rows))
            return rows
    except Exception as e:
        log.warning("[infra] nodes via JOIN locations failed: %s", e)
        _safe_rollback(conn)

    # 3) fallback sin filtro
    cur.execute("SELECT * FROM v_asset_nodes ORDER BY type, name;")
    rows = cur.fetchall() or []
    log.info("[infra] nodes fallback (NO FILTER) (rows=%s)", len(rows))
    return rows

def _get_layout_map(org_id: int) -> Dict[str, Tuple[float, float]]:
    """
    Lee posiciones guardadas PARA ESA ORG (usa org_id explícito).
    También setea el GUC local para consistencia.
    """
    layout_map: Dict[str, Tuple[float, float]] = {}
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        _set_org(cur, org_id)
        cur.execute(
            """
            SELECT node_uid, x, y
            FROM public.asset_layouts
            WHERE org_id = %s
            """,
            (int(org_id),),
        )
        for r in cur.fetchall() or []:
            try:
                layout_map[str(r["node_uid"])] = (float(r["x"]), float(r["y"]))
            except Exception:
                pass
    return layout_map

# ---------------------------------------------------------------------
# Debug: ver org efectiva
# ---------------------------------------------------------------------

@router.get("/__org_echo")
def __org_echo(_org_id: int = Depends(require_org)):
    return {"org_id": _org_id}

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
        "nodes": [{ "id": "type:code"| "type_<asset_id>", "type": "...", "name": "...", (x,y?) ... }],
        "edges": ["SRC>DST", ...]
      }
    Estrategia:
      - Setea app.org_id en la conexión (pool-safe).
      - Nodos: v_asset_nodes (varias estrategias de filtro, con logs).
      - Edges/payloads: no dependen de tablas base; se recortan por los nodos cargados.
      - Layouts: por org explícita.
    """
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            _set_org(cur, _org_id)
            log.info("[infra] /graph start org=%s", _org_id)

            # NODOS
            raw_nodes = _load_nodes_filtered(conn, cur)

            # Índices/sets de control
            node_keys: Set[str] = set()
            tank_ids: Set[int] = set()
            pump_ids: Set[int] = set()
            for r in raw_nodes:
                nid = _node_uid(r)
                node_keys.add(nid)
                t = (r.get("type") or "").lower()
                try:
                    aid = int(r.get("asset_id"))
                except Exception:
                    aid = None
                if t == "tank" and aid is not None:
                    tank_ids.add(aid)
                elif t == "pump" and aid is not None:
                    pump_ids.add(aid)

            log.info("[infra] nodes=%s tanks=%s pumps=%s", len(raw_nodes), len(tank_ids), len(pump_ids))

            # EDGES (sin join; blindo por node_keys)
            cur.execute(
                """
                SELECT e.*
                FROM v_topology_edges e
                WHERE e.is_active
                ORDER BY e.id;
                """
            )
            all_edges = cur.fetchall() or []
            raw_edges = _filter_edges_by_nodes(all_edges, node_keys)
            log.info("[infra] edges all=%s kept=%s", len(all_edges), len(raw_edges))

            # PAYLOADS: sólo para IDs presentes
            tank_config: Dict[int, Dict[str, Any]] = {}
            tank_levels: Dict[int, Any] = {}
            pump_statuses: Dict[int, Any] = {}

            if tank_ids:
                cur.execute(
                    """
                    SELECT tc.tank_id, tc.low_pct, tc.high_pct, tc.low_low_pct, tc.high_high_pct
                    FROM public.tank_config tc
                    WHERE tc.tank_id = ANY(%s);
                    """,
                    (list(tank_ids),),
                )
                for rr in cur.fetchall() or []:
                    tid = rr.get("tank_id")
                    if isinstance(tid, int) and tid in tank_ids:
                        tank_config[tid] = rr

                cur.execute(
                    """
                    SELECT v.tank_id, v.level_percent
                    FROM v_tank_latest v
                    WHERE v.tank_id = ANY(%s);
                    """,
                    (list(tank_ids),),
                )
                for rr in cur.fetchall() or []:
                    tid = rr.get("tank_id")
                    if isinstance(tid, int) and tid in tank_ids:
                        tank_levels[tid] = rr.get("level_percent")

                log.info("[infra] payload tanks cfg=%s levels=%s", len(tank_config), len(tank_levels))

            if pump_ids:
                cur.execute(
                    """
                    SELECT v.pump_id, v.is_on
                    FROM v_pump_latest v
                    WHERE v.pump_id = ANY(%s);
                    """,
                    (list(pump_ids),),
                )
                for rr in cur.fetchall() or []:
                    pid = rr.get("pump_id")
                    if isinstance(pid, int) and pid in pump_ids:
                        pump_statuses[pid] = rr.get("is_on")

                log.info("[infra] payload pumps latest=%s", len(pump_statuses))

        # Layouts por org explícita
        layout_map = _get_layout_map(_org_id)

        # Construcción de nodos
        nodes: List[Dict[str, Any]] = []
        for r in raw_nodes:
            t = r.get("type")
            if not t:
                continue
            nid = _node_uid(r)
            node: Dict[str, Any] = {"id": nid, "type": t, "name": r.get("name")}

            if t == "tank":
                tid = r.get("asset_id")
                if isinstance(tid, int) and tid in tank_config:
                    cfg = tank_config[tid]
                    node.update({
                        "low_pct": cfg.get("low_pct"),
                        "high_pct": cfg.get("high_pct"),
                        "low_low_pct": cfg.get("low_low_pct"),
                        "high_high_pct": cfg.get("high_high_pct"),
                    })
                node["level"] = tank_levels.get(tid)

            elif t == "pump":
                pid = r.get("asset_id")
                node["status"] = bool(pump_statuses.get(pid, False))
                rk = r.get("rated_kw")
                if rk is not None:
                    node["kW"] = rk

            elif t == "valve":
                vs = r.get("valve_state")
                if vs is not None:
                    node["state"] = vs

            if nid in layout_map:
                x, y = layout_map[nid]
                node["x"], node["y"] = x, y

            nodes.append(node)

        # Edges compactas
        edges: List[str] = []
        for e in raw_edges:
            src = f'{e.get("from_type")}:{e.get("from_code")}' if e.get("from_code") else f'{e.get("from_type")}_{e.get("from_id")}'
            dst = f'{e.get("to_type")}:{e.get("to_code")}'     if e.get("to_code")   else f'{e.get("to_type")}_{e.get("to_id")}'
            edges.append(f"{src}>{dst}")

        log.info("[infra] /graph done org=%s nodes=%s edges=%s", _org_id, len(nodes), len(edges))
        return {"nodes": nodes, "edges": edges}

    except HTTPException:
        raise
    except Exception as e:
        log.exception("[infra] /graph failed: %s", e)
        raise HTTPException(500, f"graph_all failed: {e}")

# ---------------------------------------------------------------------
# Graph (endpoints separados)
# ---------------------------------------------------------------------

@router.get("/graph/nodes")
def graph_nodes(
    _dev=Depends(device_id_dep),
    _org_id: int = Depends(require_org),
):
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            _set_org(cur, _org_id)
            rows = _load_nodes_filtered(conn, cur)
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
            log.info("[infra] /graph/nodes org=%s rows=%s", _org_id, len(out))
            return out
    except Exception as e:
        log.exception("[infra] /graph/nodes failed: %s", e)
        raise HTTPException(500, f"graph_nodes failed: {e}")

@router.get("/graph/edges")
def graph_edges(
    _dev=Depends(device_id_dep),
    _org_id: int = Depends(require_org),
):
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            _set_org(cur, _org_id)
            nodes = _load_nodes_filtered(conn, cur)
            node_keys = {_node_uid(r) for r in nodes}

            cur.execute(
                """
                SELECT e.id,
                       e.from_type, e.from_id, e.from_name, e.from_code,
                       e.to_type,   e.to_id,   e.to_name,   e.to_code,
                       e.pipe_diameter_mm, e.length_m, e.is_active
                FROM v_topology_edges e
                WHERE e.is_active
                ORDER BY e.id;
                """
            )
            edges = cur.fetchall() or []
            kept = _filter_edges_by_nodes(edges, node_keys)
            log.info("[infra] /graph/edges org=%s edges_all=%s edges_kept=%s", _org_id, len(edges), len(kept))
            return kept
    except Exception as e:
        log.exception("[infra] /graph/edges failed: %s", e)
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
    Guarda posiciones (UPSERT) por org explícita.
    Body: [{ "id": "<node_uid>", "x": <num>, "y": <num>, "updated_by": "..." }, ...]
    """
    if not items:
        return {"ok": True, "saved": 0}
    try:
        with get_conn() as conn, conn.cursor() as cur:
            _set_org(cur, _org_id)
            cnt = 0
            for it in items:
                cur.execute(
                    """
                    INSERT INTO public.asset_layouts (org_id, node_uid, x, y, updated_by)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (org_id, node_uid)
                    DO UPDATE SET x = EXCLUDED.x,
                                  y = EXCLUDED.y,
                                  updated_by = EXCLUDED.updated_by,
                                  updated_at = now()
                    """,
                    (int(_org_id), it.id, it.x, it.y, it.updated_by or "ui-embed"),
                )
                cnt += 1
        log.info("[infra] /layout POST org=%s saved=%s", _org_id, cnt)
        return {"ok": True, "saved": cnt}
    except Exception as e:
        log.exception("[infra] /layout POST failed: %s", e)
        raise HTTPException(500, f"save_layout failed: {e}")

@router.get("/layout")
def get_layout(
    _dev=Depends(device_id_dep),
    _org_id: int = Depends(require_org),
):
    """
    Devuelve todas las posiciones guardadas para la org del token.
    """
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            _set_org(cur, _org_id)
            cur.execute(
                """
                SELECT node_uid AS id, x, y, updated_by, updated_at
                FROM public.asset_layouts
                WHERE org_id = %s
                ORDER BY updated_at DESC
                """,
                (int(_org_id),),
            )
            rows = cur.fetchall() or []
        log.info("[infra] /layout GET org=%s rows=%s", _org_id, len(rows))
        return rows
    except Exception as e:
        log.exception("[infra] /layout GET failed: %s", e)
        raise HTTPException(500, f"get_layout failed: {e}")
