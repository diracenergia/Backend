# app/core/telegram.py
import os, httpx

def _enabled() -> bool:
    return os.getenv("TELEGRAM_ENABLED", "").strip().lower() in ("1","true","yes","on")

async def send_telegram(text: str, chat_id: str | None = None, parse_mode: str = "HTML"):
    if not _enabled():
        return {"ok": False, "reason": "disabled"}

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat  = chat_id or os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat:
        return {"ok": False, "reason": "missing token/chat"}

    async with httpx.AsyncClient(timeout=12) as cli:
        r = await cli.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
        )
        try:
            return r.json()
        except Exception:
            return {"ok": False, "status": r.status_code, "body": r.text}
