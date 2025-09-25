# app/services/alarm_listener.py
import os, json, asyncio, logging
import psycopg
from psycopg.rows import dict_row
from .telegram import send

log = logging.getLogger("alarm_listener")

EMO_SEV = {"CRITICAL": "🔴", "WARNING": "🟠", "INFO": "ℹ️"}

def _label(asset_type: str, asset_id, name: str | None):
    nice = {"tank": "Tanque", "pump": "Bomba", "valve": "Válvula", "manifold": "Colectora"}.get(
        (asset_type or "").lower(), asset_type
    )
    return f"{nice} {name or asset_id}"

def _format(payload: dict, name: str | None) -> str:
    op   = str(payload.get("op") or payload.get("event") or "").upper()  # RAISED / CLEARED
    code = str(payload.get("code") or "").upper()
    sev  = str(payload.get("severity") or "INFO").upper()
    a_t  = str(payload.get("asset_type") or payload.get("type") or "")
    a_id = payload.get("asset_id") or payload.get("id")
    msg  = payload.get("message") or ""
    val  = payload.get("value") or payload.get("level_percent")
    thr  = payload.get("threshold") or payload.get("threshold_pct")

    # Normalizar op
    if op == "RAISE": op = "RAISED"
    if op == "CLEAR": op = "CLEARED"

    # Bombas ON/OFF (code RUNNING)
    if a_t == "pump" and code == "RUNNING":
        verb = "ON ▶️" if op == "RAISED" else "OFF ⏹️"
        return f"💡 {_label(a_t, a_id, name)} {verb}"

    # Tanques por umbral
    if a_t == "tank" and code:
        bits = []
        if thr is not None: bits.append(f"umbral {thr}")
        if val is not None: bits.append(f"nivel {val}%")
        tail = f" ({', '.join(bits)})" if bits else ""
        emoji = EMO_SEV.get(sev, "ℹ️")
        state = "ALERTA" if op == "RAISED" else "NORMAL"
        return f"{emoji} {_label(a_t, a_id, name)}: {code} {state}{tail}"

    # Genérico
    emoji = EMO_SEV.get(sev, "ℹ️")
    base = msg or code or op or "evento"
    return f"{emoji} {_label(a_t, a_id, name)}: {base}"

async def _get_name(aconn: psycopg.AsyncConnection, asset_type: str, asset_id):
    try:
        async with aconn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT name FROM public.v_asset_nodes WHERE type=%s AND asset_id=%s LIMIT 1;",
                (asset_type, asset_id),
            )
            row = await cur.fetchone()
            return row["name"] if row else None
    except Exception:
        return None

async def _mark_notified(aconn: psycopg.AsyncConnection, alarm_id):
    if not alarm_id:
        return
    try:
        async with aconn.cursor() as cur:
            await cur.execute(
                "UPDATE public.alarms SET tg_notified_at = now() WHERE id=%s;",
                (int(alarm_id),),
            )
    except Exception as e:
        log.warning("mark_notified failed: %s", e)

async def listen_alarm_events():
    # gating por envs
    if os.getenv("TELEGRAM_ENABLED", "").lower() not in ("1","true","yes","on"):
        log.info("Telegram disabled; listener no iniciado")
        return
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or os.getenv("PG_DSN")
    if not dsn:
        log.warning("DATABASE_URL no definido; listener no iniciado")
        return

    aconn = await psycopg.AsyncConnection.connect(dsn)
    await aconn.execute("LISTEN alarm_events;")
    log.info("LISTEN alarm_events listo")

    try:
        while True:
            note = await aconn.notifies.get()  # queue de psycopg3
            try:
                payload = json.loads(note.payload)
            except Exception:
                log.exception("payload invalido: %r", note.payload)
                continue

            name = await _get_name(aconn, payload.get("asset_type"), payload.get("asset_id"))
            text = _format(payload, name)

            # send() es sincrónico → ejecutarlo en pool de hilos
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, send, text)

            await _mark_notified(aconn, payload.get("alarm_id"))
    finally:
        await aconn.close()
