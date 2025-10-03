# app/routes/operaciones.py
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg.rows import dict_row
from typing import Dict, Any, List

from app.core.db import get_conn
from app.core.security import device_id_dep
from app.core.tenancy import require_org

router = APIRouter()

def _empty_series():
    return {"timestamps": [], "values": []}

@router.get("/operaciones")
def operaciones(
    with_graph: bool = Query(False),
    _dev = Depends(device_id_dep),
    _org_id: int = Depends(require_org),
):
    out: Dict[str, Any] = {
        "ok": True,
        "org_id": _org_id,
        "cards": {"total_tanks": 0, "total_pumps": 0, "pumps_on": 0, "avg_tank_level": None},
        "series": {
            "tank_level_24h": _empty_series(),
            "pumps_on_24h": _empty_series(),
        },
    }

    # ---------- CARDS ----------
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT
                  COALESCE(SUM((type='tank')::int),0)        AS total_tanks,
                  COALESCE(SUM((type='pump')::int),0)        AS total_pumps,
                  COALESCE(SUM((type='pump' AND pump_status)::int),0) AS pumps_on,
                  AVG(NULLIF(level_ratio,0))                 AS avg_tank_level
                FROM v_asset_nodes
            """)
            r = cur.fetchone() or {}
            out["cards"] = {
                "total_tanks": int(r.get("total_tanks") or 0),
                "total_pumps": int(r.get("total_pumps") or 0),
                "pumps_on":    int(r.get("pumps_on") or 0),
                "avg_tank_level": float(r.get("avg_tank_level")) if r.get("avg_tank_level") is not None else None,
            }
    except Exception as e:
        # No tiramos 500: devolvemos cards por defecto y anotamos el fallo
        out["cards_error"] = f"{e}"

    # ---------- SERIES (best-effort) ----------
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            # Bucket simple por hora, 24h (adaptá a tus vistas si ya las tenés)
            cur.execute("""
                WITH H AS (
                  SELECT date_trunc('hour', now() - i * interval '1 hour') AS ts
                  FROM generate_series(0,23) AS s(i)
                )
                SELECT H.ts, AVG(v.level_percent) AS lvl
                FROM H
                LEFT JOIN v_tank_latest v
                  ON v.updated_at >= H.ts
                 AND v.updated_at <  H.ts + interval '1 hour'
                GROUP BY 1
                ORDER BY 1
            """)
            rows = cur.fetchall() or []
            out["series"]["tank_level_24h"] = {
                "timestamps": [r["ts"].isoformat() for r in rows],
                "values":     [float(r["lvl"]) if r["lvl"] is not None else None for r in rows],
            }

            cur.execute("""
                WITH H AS (
                  SELECT date_trunc('hour', now() - i * interval '1 hour') AS ts
                  FROM generate_series(0,23) AS s(i)
                )
                SELECT H.ts, SUM(CASE WHEN v.is_on THEN 1 ELSE 0 END) AS pumps_on
                FROM H
                LEFT JOIN v_pump_latest v
                  ON v.updated_at >= H.ts
                 AND v.updated_at <  H.ts + interval '1 hour'
                GROUP BY 1
                ORDER BY 1
            """)
            rows = cur.fetchall() or []
            out["series"]["pumps_on_24h"] = {
                "timestamps": [r["ts"].isoformat() for r in rows],
                "values":     [int(r["pumps_on"] or 0) for r in rows],
            }

    except Exception as e:
        out["series_error"] = f"{e}"

    # ---------- GRAPH (opcional, best-effort) ----------
    if with_graph:
        try:
            # Reutilizamos tu endpoint infra internamente
            from app.routes.graph_api import graph_all
            out["graph"] = graph_all(_org_id=_org_id)  # mismo org del header
        except Exception as e:
            out["graph_error"] = f"{e}"

    return out
