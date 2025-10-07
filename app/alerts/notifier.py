#!/usr/bin/env python3
# app/alerts/notifier.py
from __future__ import annotations

import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, List

import httpx
import psycopg
from psycopg.rows import dict_row


log = logging.getLogger("alerts")


# -------------------------------------------------------------
# Config desde entorno (con defaults razonables)
# -------------------------------------------------------------
def _get_env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.lower() in ("1", "true", "yes", "on")


POLL_INTERVAL = float(os.getenv("ALERTS_POLL_INTERVAL_SEC", "1.0"))
WARMUP_ON_STARTUP = _get_env_bool("ALERTS_WARMUP_ON_STARTUP", True)
NOISE_GUARD_SEC = int(os.getenv("ALERTS_NOISE_GUARD_SEC", "0"))

DATABASE_URL = os.environ.get("DATABASE_URL", "")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

WA_TOKEN = os.getenv("WHATSAPP_TOKEN")
WA_PHONE_ID = os.getenv("WHATSAPP_PHONE_ID")
WA_TO = os.getenv("WHATSAPP_TO")


# -------------------------------------------------------------
# Mensajería (Telegram + WhatsApp opcional)
# -------------------------------------------------------------
def _strip_html(text: str) -> str:
    import re
    return re.sub(r"<[^>]+>", "", text)


class Messenger:
    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    async def send(self, text: str) -> None:
        # Telegram
        if TG_TOKEN and TG_CHAT_ID:
            try:
                url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
                payload = {
                    "chat_id": TG_CHAT_ID,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                }
                r = await self.client.post(url, json=payload, timeout=15)
                r.raise_for_status()
            except Exception as e:
                log.exception("Telegram error: %s", e)

        # WhatsApp Business Cloud (opcional)
        if WA_TOKEN and WA_PHONE_ID and WA_TO:
            try:
                url = f"https://graph.facebook.com/v20.0/{WA_PHONE_ID}/messages"
                headers = {"Authorization": f"Bearer {WA_TOKEN}"}
                payload = {
                    "messaging_product": "whatsapp",
                    "to": WA_TO,
                    "type": "text",
                    "text": {"body": _strip_html(text)},
                }
                r = await self.client.post(url, headers=headers, json=payload, timeout=15)
                r.raise_for_status()
            except Exception as e:
                log.exception("WhatsApp error: %s", e)


# -------------------------------------------------------------
# Notificador principal
# -------------------------------------------------------------
class AlertsNotifier:
    def __init__(self):
        self._stop = asyncio.Event()
        self._tasks: List[asyncio.Task] = []
        self._http = httpx.AsyncClient()
        self._msgr = Messenger(self._http)

    async def start(self):
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL no configurada")

        log.info(
            "AlertsNotifier starting. poll=%.2fs warmup=%s noise_guard=%ss",
            POLL_INTERVAL, WARMUP_ON_STARTUP, NOISE_GUARD_SEC
        )
        if WARMUP_ON_STARTUP:
            await self._warmup()

        self._tasks = [
            asyncio.create_task(self._watch_tanks(), name="watch_tanks"),
            asyncio.create_task(self._watch_pumps(), name="watch_pumps"),
        ]

    async def stop(self):
        log.info("AlertsNotifier stopping...")
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._http.aclose()

    # -----------------------
    # Tanks
    # -----------------------
    async def _watch_tanks(self):
        sql = """
            SELECT
                tank_id,
                name,
                location_name,
                COALESCE(alarma, 'ok') AS alarma,
                online,
                level_pct,
                age_sec
            FROM public.v_tanks_with_config;
        """
        async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                while not self._stop.is_set():
                    try:
                        await cur.execute(sql)
                        rows = await cur.fetchall()
                        for r in rows:
                            key = f"tank:{r['tank_id']}:alarma"
                            new_hash = f"{r['alarma']}|{bool(r['online'])}"
                            changed, old_hash, last_changed_at = await self._upsert_state(conn, key, new_hash)
                            if changed and not self._should_muffle(last_changed_at):
                                msg = self._fmt_tank_msg(r, old_hash)
                                await self._msgr.send(msg)
                    except Exception:
                        log.exception("watch_tanks iteration error")
                    await asyncio.sleep(POLL_INTERVAL)

    def _fmt_tank_msg(self, r: dict, old_hash: Optional[str]) -> str:
        alarma = r["alarma"]
        online = "✅ online" if r["online"] else "❌ offline"
        level = f"{r['level_pct']}%" if r.get("level_pct") is not None else "--"
        age = f"hace {r['age_sec']}s" if r.get("age_sec") is not None else "sin lectura"
        prev = old_hash.split("|")[0] if old_hash else "(n/a)"
        emoji = "🚨" if alarma in ("critico", "alto", "bajo") else "ℹ️"
        return (
            f"{emoji} <b>Tanque</b> {r['name']} ({r['location_name']})\n"
            f"Alarma: <b>{alarma}</b> (antes: {prev})\n"
            f"Nivel: {level} — {age}\n"
            f"Estado: {online}"
        )

    # -----------------------
    # Pumps
    # -----------------------
    async def _watch_pumps(self):
        sql = """
            SELECT
                pump_id,
                name,
                location_name,
                state,
                online,
                event_ts,
                hb_ts
            FROM public.v_pumps_with_status;
        """
        async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                while not self._stop.is_set():
                    try:
                        await cur.execute(sql)
                        rows = await cur.fetchall()
                        for r in rows:
                            key = f"pump:{r['pump_id']}:state"
                            new_hash = f"{r['state']}|{bool(r['online'])}"
                            changed, old_hash, last_changed_at = await self._upsert_state(conn, key, new_hash)
                            if changed and not self._should_muffle(last_changed_at):
                                msg = self._fmt_pump_msg(r, old_hash)
                                await self._msgr.send(msg)
                    except Exception:
                        log.exception("watch_pumps iteration error")
                    await asyncio.sleep(POLL_INTERVAL)

    def _fmt_pump_msg(self, r: dict, old_hash: Optional[str]) -> str:
        state = r["state"]
        online = "✅ online" if r["online"] else "❌ offline"
        prev = old_hash.split("|")[0] if old_hash else "(n/a)"
        tmark = r.get("event_ts") or r.get("hb_ts")
        ts = tmark.strftime("%Y-%m-%d %H:%M:%S %Z") if isinstance(tmark, datetime) else ""
        emoji = "🟢" if state == "start" else ("🔴" if state == "stop" else "🔁")
        line_ts = f"\nMarca de tiempo: {ts}" if ts else ""
        return (
            f"{emoji} <b>Bomba</b> {r['name']} ({r['location_name']})\n"
            f"Estado: <b>{state}</b> (antes: {prev})\n"
            f"Conectividad: {online}{line_ts}"
        )

    # -----------------------
    # Estado (deduplicación)
    # -----------------------
    async def _upsert_state(self, conn: psycopg.AsyncConnection, key: str, new_hash: str):
        old_hash: Optional[str] = None
        last_changed_at: Optional[datetime] = None

        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT hash, last_changed_at FROM app.notify_state WHERE key = %s", (key,))
            row = await cur.fetchone()
            if row:
                old_hash = row["hash"]
                last_changed_at = row["last_changed_at"]
                if old_hash != new_hash:
                    await cur.execute(
                        """
                        UPDATE app.notify_state
                           SET prev_hash = %s,
                               hash = %s,
                               last_changed_at = now(),
                               last_seen_at = now()
                         WHERE key = %s
                        """,
                        (old_hash, new_hash, key),
                    )
                    await conn.commit()
                    changed = True
                else:
                    await cur.execute("UPDATE app.notify_state SET last_seen_at = now() WHERE key = %s", (key,))
                    await conn.commit()
                    changed = False
            else:
                # Primera vez: insertamos. Si WARMUP está activo, no avisamos.
                await cur.execute(
                    """
                    INSERT INTO app.notify_state(key, hash, prev_hash, last_changed_at, last_seen_at)
                    VALUES (%s, %s, NULL, now(), now())
                    """,
                    (key, new_hash),
                )
                await conn.commit()
                changed = not WARMUP_ON_STARTUP

        return changed, old_hash, last_changed_at

    def _should_muffle(self, last_changed_at: Optional[datetime]) -> bool:
        if NOISE_GUARD_SEC <= 0 or last_changed_at is None:
            return False
        now = datetime.now(timezone.utc)
        return (now - last_changed_at).total_seconds() < NOISE_GUARD_SEC

    # Warmup inicial: guarda estados sin enviar (si está activado)
    async def _warmup(self):
        log.info("Warmup de estados iniciales…")
        async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
            # Tanks
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT tank_id, COALESCE(alarma, 'ok') AS alarma, online
                    FROM public.v_tanks_with_config;
                    """
                )
                for r in await cur.fetchall():
                    key = f"tank:{r['tank_id']}:alarma"
                    new_hash = f"{r['alarma']}|{bool(r['online'])}"
                    await self._upsert_state(conn, key, new_hash)

            # Pumps
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT pump_id, state, online
                    FROM public.v_pumps_with_status;
                    """
                )
                for r in await cur.fetchall():
                    key = f"pump:{r['pump_id']}:state"
                    new_hash = f"{r['state']}|{bool(r['online'])}"
                    await self._upsert_state(conn, key, new_hash)
        log.info("Warmup listo.")


# -------------------------------------------------------------
# Modo standalone (opcional): ejecutar como servicio simple
# -------------------------------------------------------------
async def _run_forever():
    notifier = AlertsNotifier()
    try:
        await notifier.start()
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        await notifier.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
    try:
        asyncio.run(_run_forever())
    except KeyboardInterrupt:
        log.info("Interrumpido por el usuario")
