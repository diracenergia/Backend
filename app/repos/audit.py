from app.core.db import get_conn
from psycopg.types.json import Json

def insert_alarm_ack_event(user: str, asset_type: str, asset_id: int, code: str, severity: str, note: str | None):
    asset_label = f"TK-{asset_id}" if asset_type == "tank" else f"PU-{asset_id}"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO audit_events(
                ts,"user",role,action,asset,details,result,
                domain,asset_type,asset_id,code,severity,state
            )
            VALUES (
                now(), %s, 'operator', 'ALARM', %s,
                %s,
                'ok',
                'ALARM', %s, %s, %s, %s, 'ACKED'
            )
        """, (user, asset_label, Json({"note": note}), asset_type, asset_id, code, severity))
        conn.commit()

def list_audit(
    asset_type: str | None,
    asset_id: int | None,
    code: str | None,
    state: str | None,
    since,
    until,
    limit: int
):
    sql = """
      SELECT ts, "user", role, action, asset, details, result,
             domain, asset_type, asset_id, code, severity, state
      FROM audit_events
      WHERE 1=1
    """
    args: list = []
    if asset_type: sql += " AND asset_type=%s"; args.append(asset_type)
    if asset_id:   sql += " AND asset_id=%s";   args.append(asset_id)
    if code:       sql += " AND code=%s";       args.append(code)
    if state:      sql += " AND state=%s";      args.append(state)
    if since:      sql += " AND ts >= %s";      args.append(since)
    if until:      sql += " AND ts <  %s";      args.append(until)
    sql += " ORDER BY ts DESC LIMIT %s"; args.append(limit)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]
