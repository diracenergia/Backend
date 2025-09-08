# app/services/telegram.py
import os, requests, json

def _enabled() -> bool:
    return os.getenv("TELEGRAM_ENABLED", "").lower() in ("1","true","yes","on")

def send(text: str, chat_id: str | None = None, parse_mode: str = "HTML"):
    """
    Envia mensaje a Telegram. Por defecto HTML (más robusto que Markdown).
    Loguea status para diagnóstico.
    """
    if not _enabled():
        print("[telegram] disabled (TELEGRAM_ENABLED != true)")
        return {"ok": False, "reason": "disabled"}

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat  = chat_id or os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat:
        print("[telegram] missing token/chat")
        return {"ok": False, "reason": "missing token/chat"}

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True},
            timeout=10,
        )
        if resp.status_code != 200:
            print(f"[telegram] HTTP {resp.status_code}: {resp.text}")
            return {"ok": False, "status": resp.status_code, "body": resp.text}
        data = resp.json()
        print("[telegram] sent ok:", json.dumps({"to": str(chat), "len": len(text)}, ensure_ascii=False))
        return data
    except Exception as e:
        print(f"[telegram] send error: {e}")
        return {"ok": False, "exception": repr(e)}
