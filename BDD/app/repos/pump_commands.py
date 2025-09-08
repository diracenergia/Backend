from app.core.db import get_conn
from psycopg.types.json import Json

def enqueue_pump_command(pump_id: int, cmd: str, payload: dict | None, user: str) -> dict:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pump_commands (pump_id, cmd, payload, requested_by)
            VALUES (%s, %s, %s, %s)
            RETURNING id, pump_id, cmd, status, payload, ts_created
        """, (pump_id, cmd, Json(payload) if payload else None, user))
        row = cur.fetchone(); conn.commit()
    cols = ["id","pump_id","cmd","status","payload","ts_created"]
    return dict(zip(cols, row))

def list_pump_commands(pump_id: int, status: str | None, limit: int):
    with get_conn() as conn, conn.cursor() as cur:
        if status:
            cur.execute("""
                SELECT id, pump_id, cmd, payload, status, ts_created, ts_sent, ts_acked, error
                FROM pump_commands
                WHERE pump_id=%s AND status=%s
                ORDER BY ts_created ASC
                LIMIT %s
            """, (pump_id, status, limit))
        else:
            cur.execute("""
                SELECT id, pump_id, cmd, payload, status, ts_created, ts_sent, ts_acked, error
                FROM pump_commands
                WHERE pump_id=%s
                ORDER BY ts_created DESC
                LIMIT %s
            """, (pump_id, limit))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]

def get_command_status(cmd_id: int, pump_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT status FROM pump_commands WHERE id=%s AND pump_id=%s", (cmd_id, pump_id))
        r = cur.fetchone()
    return r[0] if r else None

def mark_sent(cmd_id: int):  # idem tanques
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE pump_commands SET status='sent', ts_sent=now(), error=NULL
            WHERE id=%s
            RETURNING id, pump_id, status, ts_sent
        """, (cmd_id,))
        row = cur.fetchone(); conn.commit()
    cols = [d[0] for d in cur.description]; return dict(zip(cols, row))

def mark_acked(cmd_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE pump_commands SET status='acked', ts_acked=now(), error=NULL
            WHERE id=%s
            RETURNING id, pump_id, status, ts_acked
        """, (cmd_id,))
        row = cur.fetchone(); conn.commit()
    cols = [d[0] for d in cur.description]; return dict(zip(cols, row))

def mark_other(cmd_id: int, status: str, error: str | None):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE pump_commands SET status=%s, error=%s
            WHERE id=%s
            RETURNING id, pump_id, status, error
        """, (status, error, cmd_id))
        row = cur.fetchone(); conn.commit()
    cols = [d[0] for d in cur.description]; return dict(zip(cols, row))
