from app.core.db import get_conn
from psycopg.types.json import Json
from typing import Optional

def enqueue_tank_command(tank_id: int, cmd: str, payload: dict | None, user: str) -> dict:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO tank_commands (tank_id, cmd, payload, requested_by)
            VALUES (%s, %s, %s, %s)
            RETURNING id, tank_id, cmd, status, payload, ts_created
        """, (tank_id, cmd, Json(payload) if payload else None, user))
        row = cur.fetchone()
        conn.commit()
    cols = ["id","tank_id","cmd","status","payload","ts_created"]
    return dict(zip(cols, row))

def list_tank_commands(tank_id: int, status: str | None, limit: int):
    with get_conn() as conn, conn.cursor() as cur:
        if status:
            cur.execute("""
                SELECT id, tank_id, cmd, payload, status, ts_created, ts_sent, ts_acked, error
                  FROM tank_commands
                 WHERE tank_id=%s AND status=%s
              ORDER BY ts_created ASC
                 LIMIT %s
            """, (tank_id, status, limit))
        else:
            cur.execute("""
                SELECT id, tank_id, cmd, payload, status, ts_created, ts_sent, ts_acked, error
                  FROM tank_commands
                 WHERE tank_id=%s
              ORDER BY ts_created DESC
                 LIMIT %s
            """, (tank_id, limit))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]

def get_command_status(jid: int, tank_id: int) -> Optional[str]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT status FROM tank_commands WHERE id=%s AND tank_id=%s", (jid, tank_id))
        r = cur.fetchone()
    return r[0] if r else None

def mark_sent(jid: int) -> dict:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE tank_commands SET status='sent', ts_sent=now(), error=NULL
            WHERE id=%s
            RETURNING id, tank_id, status, ts_sent
        """, (jid,))
        row = cur.fetchone()
        conn.commit()
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))

def mark_acked(jid: int) -> dict:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE tank_commands SET status='acked', ts_acked=now(), error=NULL
            WHERE id=%s
            RETURNING id, tank_id, status, ts_acked
        """, (jid,))
        row = cur.fetchone()
        conn.commit()
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))

def mark_other(jid: int, status: str, error: str | None) -> dict:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE tank_commands SET status=%s, error=%s
            WHERE id=%s
            RETURNING id, tank_id, status, error
        """, (status, error, jid))
        row = cur.fetchone()
        conn.commit()
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))
