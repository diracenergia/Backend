# app/core/config.py
import os

BOT = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
ENABLED = os.getenv("TELEGRAM_ENABLED", "false").lower() in ("1","true","yes","on") and BOT and CHAT
