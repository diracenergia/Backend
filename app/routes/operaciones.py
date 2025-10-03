# app/routes/operaciones.py
from __future__ import annotations
from typing import Any, Dict, Optional

import os
from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg.rows import dict_row

from app.core.db import get_conn
from app.core.security import device_id_dep
from app.core.tenancy import require_org  # lee x-org-id o query param

router = APIRouter()

def _set_org(cur, org_id: int) -> None:
    # fijamos el GUC en ESTA conexión (importante si hay RLS/policies o vistas que usan current_setting)
    cur.execute("SELECT set_config('app.org_id', %s, true)", (str(int(org_id)),))

def _scalar(cur, sql: str, args: Optional[tuple] = None, default: Any = None):
    try:
        cur.execute(sql, args or ())
        row = cur.fetchone()
        if not row:
            return default
        # soporta dict_row y tuples
        if isinstance(row, dict):
            # toma la primera columna
            return next(iter(row.values()), default)
        return row[0]
    except Exception:
        return default

@router.get("/operaciones")
def operaciones(
    with_graph: bool = Query(False, description="Incluir grafo resumido"),
    _dev=Depends(device_id_dep),
    _org_id: int = Depends(require_org),
):
    """
    Endpoint unificado para la página de operaciones.
    - Devuelve tarjetas simples (totales, bombas on, nivel promedio).
    - Series vacías por ahora (para que el front cargue rápido).
    - Si algo falla, responde en 'degraded' con el error (cuando SHOW_ERRORS=1).
    """
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            _set_org(cur, _org_id)

            # === Tarjetas (robustas, sin romper si falta alguna vista) ===
            total_tanks = _scalar(cur, "SELECT count(*) FROM v_asset_nodes WHERE lower(type)='tank'", default=0)
            total_pumps = _scalar(cur, "SELECT count(*) FROM v_asset_nodes WHERE lower(type)='pump'", default=0)
            pumps_on    = _scalar(cur, "SELECT count(*) FROM v_pump_latest WHERE is_on IS TRUE", default=0)
            avg_level   = _scalar(cur, "SELECT avg(level_percent) FROM v_tank_latest", default=None)

            cards = {
                "total_tanks": int(total_tanks or 0),
                "total_pumps": int(total_pumps or 0),
                "pumps_on": int(pumps_on or 0),
                "avg_tank_level": float(avg_level) if avg_level is not None else None,
            }

            # === Series mínimas (placeholder) ===
            series = {
                "tank_level_24h": {"timestamps": [], "values": []},
                "pumps_on_24h": {"timestamps": [], "values": []},
            }

            out: Dict[str, Any] = {"ok": True, "org_id": _org_id, "cards": cards, "series": series}

            # === (Opcional) Grafo resumido: solo ids + edges compactas ===
            if with_graph:
                # Nodes: id, type, name
                cur.execute("""
                    SELECT
                        (CASE WHEN code IS NOT NULL AND code <> '' THEN lower(type)||':'||code
                              ELSE lower(type)||'_'||asset_id END) AS id,
                        type, name
                    FROM v_asset_nodes
                    ORDER BY type, name
                """)
                nodes = [{"id": r["id"], "type": r["type"], "name": r["name"]} for r in (cur.fetchall() or [])]

                # Edges: compactadas SRC>DST y solo activas
                cur.execute("""
                    SELECT
                        (CASE WHEN from_code IS NOT NULL AND from_code <> '' THEN lower(from_type)||':'||from_code
                              ELSE lower(from_type)||'_'||from_id END) AS src,
                        (CASE WHEN to_code IS NOT NULL AND to_code <> '' THEN lower(to_type)||':'||to_code
                              ELSE lower(to_type)||'_'||to_id END) AS dst
                    FROM v_topology_edges
                    WHERE is_active
                    ORDER BY id
                """)
                edges = [f'{r["src"]}>{r["dst"]}' for r in (cur.fetchall() or [])]

                out["graph"] = {"nodes": nodes, "edges": edges}

            return out

    except HTTPException:
        raise
    except Exception as e:
        # NO 500 en dev: si SHOW_ERRORS=1 devolvemos modo degradado con el error
        if os.getenv("SHOW_ERRORS", "0") == "1":
            return {"ok": False, "degraded": True, "error": str(e)}
        raise HTTPException(500, "operaciones failed")
