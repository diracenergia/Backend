# app/core/config.py
import os

# (Opcional) cargar .env si está instalado python-dotenv
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# Variables presentes o no; no rompen si faltan
BOT = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT = os.getenv("TELEGRAM_CHAT_ID")

# 🔴 Forzar desactivado
ENABLED = False

__all__ = ["BOT", "CHAT", "ENABLED"]
