import logging

import requests

log = logging.getLogger("telegram_client")

TELEGRAM_API_BASE = "https://api.telegram.org"


class TelegramClient:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = (bot_token or "").strip()
        self.chat_id = str(chat_id or "").strip()

        if not self.bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN not configured")
        if not self.chat_id:
            raise ValueError("TELEGRAM_CHAT_ID not configured")

    def send_message(self, text: str) -> dict:
        url = f"{TELEGRAM_API_BASE}/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
        }

        response = requests.post(url, json=payload, timeout=15)
        if not response.ok:
            log.error(
                "Telegram sendMessage error status=%s body=%s",
                response.status_code,
                response.text,
            )
            response.raise_for_status()

        data = response.json()
        if not data.get("ok", False):
            raise Exception(f"Telegram API returned ok=false description={data.get('description')}")

        log.info("Telegram message sent chat_id=%s", self.chat_id)
        return data
