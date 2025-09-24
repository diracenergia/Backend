# app/services/telegram.py
import os, requests, json

def _enabled() -> bool:
    return os.getenv("TELEGRAM_ENABLED", "").strip().lower() in ("1","true","yes","on")

def _token() -> str | None:
    return os.getenv("TELEGRAM_BOT_TOKEN")

def _chat() -> str | None:
    return os.getenv("TELEGRAM_CHAT_ID")

def _parse_mode() -> str:
    # HTML es más tolerante que Markdown en Telegram
    return os.getenv("TELEGRAM_PARSE_MODE", "HTML")

_MAX = 3800  # < 4096 por seguridad
def _chunks(s: str, n: int = _MAX):
    s = s or ""
    for i in range(0, len(s), n):
        yield s[i:i+n]

def send(text: str, chat_id: str | None = None, parse_mode: str | None = None):
    """
    Envía 1+ mensajes si el texto es largo. Loguea errores HTTP.
    """
    if not _enabled():
        print("[telegram] disabled (TELEGRAM_ENABLED != true)")
        return {"ok": False, "reason": "disabled"}

    token = _token()
    chat  = chat_id or _chat()
    if not token or not chat:
        print("[telegram] missing token/chat")
        return {"ok": False, "reason": "missing token/chat"}

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    mode = parse_mode or _parse_mode()

    last = {"ok": True}
    for part in _chunks(text):
        try:
            resp = requests.post(
                url,
                json={"chat_id": chat, "text": part, "parse_mode": mode, "disable_web_page_preview": True},
                timeout=12,
            )
            if resp.status_code != 200:
                print(f"[telegram] HTTP {resp.status_code}: {resp.text}")
                last = {"ok": False, "status": resp.status_code, "body": resp.text}
            else:
                data = resp.json()
                last = data
                print("[telegram] sent ok:", json.dumps({"to": str(chat), "len": len(part)}, ensure_ascii=False))
        except Exception as e:
            print(f"[telegram] send error: {e!r}")
            last = {"ok": False, "exception": repr(e)}
    return last

def healthcheck() -> dict:
    return {"enabled": _enabled(), "chat": bool(_chat()), "token": bool(_token())}
