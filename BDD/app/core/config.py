import os
from dotenv import load_dotenv
load_dotenv()
BOT = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT = os.environ["TELEGRAM_CHAT_ID"]
ENABLED = os.environ.get("TELEGRAM_ENABLED", "true").lower() == "true"
