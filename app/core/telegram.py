import httpx, json
from .config import BOT, CHAT, ENABLED

async def send_telegram(text: str, chat_id: str | int = CHAT):
    if not ENABLED:
        print("[tg] disabled: TELEGRAM_ENABLED != true")
        return {"ok": False, "reason": "disabled"}

    url = f"https://api.telegram.org/bot{BOT}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    try:
        async with httpx.AsyncClient(timeout=10) as cli:
            r = await cli.post(url, json=payload)
        if r.status_code != 200:
            print(f"[tg] HTTP {r.status_code}: {r.text}")
            return {"ok": False, "status": r.status_code, "body": r.text}
        data = r.json()
        print("[tg] sent ok:", json.dumps({"to": str(chat_id), "text": text[:60]}, ensure_ascii=False))
        return data
    except Exception as e:
        print("[tg] EXC:", repr(e))
        return {"ok": False, "exception": repr(e)}
