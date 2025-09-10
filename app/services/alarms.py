# app/services/alarms.py (o donde corresponda)
from app.services.alarm_events import publish_alarm_event

def raise_alarm(conn, *, asset_type: str, asset_id: int,
                code: str, severity: str, message: str = "",
                value: float | None = None, threshold: str | None = None,
                enable_telegram: bool | None = None) -> int:
    """
    Crea una alarma y, si corresponde, publica evento a Telegram vía listener.
    Retorna alarm_id.
    """
    telegram = True if enable_telegram is None else bool(enable_telegram)

    with conn.cursor() as cur:
        # INSERT con columnas opcionales telegram/tg_notified_at (si existen)
        cur.execute("""
            INSERT INTO public.alarms (asset_type, asset_id, code, severity, message, extra, telegram)
            VALUES (%s,%s,%s,%s,%s,
                    jsonb_build_object('value', %s, 'threshold', %s),
                    %s)
            RETURNING id, COALESCE(telegram, true) AS telegram;
        """, (asset_type, asset_id, code, severity, message, value, threshold, telegram))
        alarm_id, telegram = cur.fetchone()
    conn.commit()

    # Si está habilitado Telegram para esta alarma, publicamos el evento RAISED
    if telegram:
        payload = {
            "op":        "RAISED",
            "asset_type": asset_type,
            "asset_id":   asset_id,
            "code":       (code or "").upper(),
            "severity":   (severity or "").upper(),
            "threshold":  threshold,
            "value":      value,
            "message":    message or "",
            "alarm_id":   alarm_id,
        }
        publish_alarm_event(payload)

        # Marcar que ya notificamos (si tenés columna)
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE public.alarms SET tg_notified_at = now() WHERE id = %s;", (alarm_id,))
            conn.commit()
        except Exception as e:
            # Si no existe la columna, no pasa nada
            print(f"[alarms] tg_notified_at skip: {e}")

    return alarm_id


def clear_alarm(conn, *, alarm_id: int, message: str | None = None, value: float | None = None):
    """Limpia una alarma y, si corresponde, publica evento CLEARED."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE public.alarms
               SET ts_cleared = now(),
                   is_active  = false,
                   message    = COALESCE(%s, message)
             WHERE id = %s
         RETURNING asset_type, asset_id, code, severity,
                   COALESCE(telegram, true) AS telegram,
                   tg_notified_at;
        """, (message, alarm_id))
        row = cur.fetchone()
    conn.commit()

    if not row:
        return

    asset_type, asset_id, code, severity, telegram, tg_notified_at = row

    # Regla: avisar CLEARED si
    #  - telegram=True, o
    #  - previamente notificamos el RAISED (tg_notified_at no null)
    if telegram or tg_notified_at is not None:
        payload = {
            "op":        "CLEARED",
            "asset_type": asset_type,
            "asset_id":   asset_id,
            "code":       (code or "").upper(),
            "severity":   (severity or "").upper(),
            "threshold":  None,
            "value":      value,
            "message":    message or "",
            "alarm_id":   alarm_id,
        }
        publish_alarm_event(payload)
