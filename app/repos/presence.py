# Marca presencia de un activo (o del sistema) actualizando last_seen_at en devices
# y dejando una huella en audit_events. Ajustá si querés atarlo a un device específico.

from typing import Optional
from datetime import datetime, timezone
from app.core.db import get_conn

def bump_presence(asset_type: str = "system", asset_id: Optional[int] = None, ts: Optional[float] = None) -> None:
    now = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else datetime.now(tz=timezone.utc)
    with get_conn() as conn, conn.cursor() as cur:
        # 1) audit (opcional pero útil para trazas)
        cur.execute("""
            INSERT INTO audit_events(ts, user, role, action, asset, domain, asset_type, asset_id, state)
            VALUES (%s, %s, %s, %s, %s, 'PRESENCE', %s, %s, %s)
        """, (now, 'presence', 'system', 'heartbeat', 'backend', asset_type, asset_id, 'online'))

        # 2) si tenés un device “virtual” para el backend, marcá su last_seen_at
        # (si no existe, podés saltearlo sin error)
        cur.execute("""
            UPDATE devices
               SET last_seen_at = %s
             WHERE name = 'backend'
        """, (now,))
        conn.commit()
