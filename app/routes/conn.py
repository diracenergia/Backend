# app/routes/conn_simple.py
from datetime import datetime, timezone
import os
from fastapi import APIRouter, HTTPException, Header
from psycopg.rows import dict_row
from typing import Optional, Dict, Any, List

from app.core.db import get_conn

router = APIRouter(prefix="/conn", tags=["conn"])

WARN_SEC = int(os.getenv("WARN_SEC", "120"))   # 2 min
CRIT_SEC = int(os.getenv("CRIT_SEC", "300"))   # 5 min

def _tone(age: Optional[int]) -> str:
    if age is None:
        return "bad"
    return "ok" if age < WARN_SEC else ("warn" if age < CRIT_SEC else "bad")

@router.get("/simple")
def conn_simple(x_org_id: Optional[int] = Header(default=None, convert_underscores=False)) -> Dict[str, Any]:
    """
    Devuelve presencia basada en la frescura de lecturas (sin WS):
      - node_id: 'pump_<id>' | 'tank_<id>'
      - online: True/False
      - tone: 'ok'|'warn'|'bad'
      - age_sec, last_seen, source='reading'
    Si se pasa X-Org-Id, intenta filtrar por organización (opcional).
    """
    try:
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            if x_org_id:
                # Filtra solo activos de la org (vía assets)
                cur.execute("""
                  with ap as (
                    select p.asset_type, p.asset_id, p.last_ts
                    from public.v_asset_presence_simple p
                    join public.assets a
                      on a.kind = p.asset_type and a.native_id = p.asset_id
                    where a.org_id = %s
                  )
                  select * from ap;
                """, (x_org_id,))
            else:
                cur.execute("select asset_type, asset_id, last_ts from public.v_asset_presence_simple;")
            rows = cur.fetchall()

        now = datetime.now(timezone.utc)
        out: List[Dict[str, Any]] = []
        for r in rows:
            last_ts = r["last_ts"]
            age = None
            if last_ts:
                if last_ts.tzinfo is None:
                    last_ts = last_ts.replace(tzinfo=timezone.utc)
                age = max(0, int((now - last_ts).total_seconds()))
            node_id = f'{r["asset_type"]}_{r["asset_id"]}'
            out.append({
                "node_id": node_id,
                "asset_type": r["asset_type"],
                "asset_id": r["asset_id"],
                "last_seen": last_ts.isoformat() if last_ts else None,
                "age_sec": age,
                "online": age is not None and age < CRIT_SEC,
                "tone": _tone(age),
                "source": "reading"
            })
        return {"presence": out}
    except Exception as e:
        raise HTTPException(500, f"conn_simple failed: {e}")
