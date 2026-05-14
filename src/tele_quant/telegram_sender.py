from __future__ import annotations

import logging
from typing import Any

import httpx

from tele_quant.headline_cleaner import apply_final_report_cleaner
from tele_quant.settings import Settings
from tele_quant.telegram_client import TelegramGateway
from tele_quant.textutil import chunk_message, mask_bot_token, sanitize_for_telegram

log = logging.getLogger(__name__)


class TelegramSender:
    def __init__(self, settings: Settings, gateway: TelegramGateway | None = None) -> None:
        self.settings = settings
        self.gateway = gateway

    async def send(self, text: str) -> None:
        text = apply_final_report_cleaner(text)
        text = sanitize_for_telegram(text)
        chunks = chunk_message(text)
        failed = 0
        for idx, chunk in enumerate(chunks, start=1):
            prefix = f"({idx}/{len(chunks)})\n" if len(chunks) > 1 else ""
            try:
                if self.settings.telegram_send_mode == "bot":
                    await self._send_bot(prefix + chunk)
                else:
                    if self.gateway is None:
                        async with TelegramGateway(self.settings) as gateway:
                            await gateway.send_text_user(prefix + chunk)
                    else:
                        await self.gateway.send_text_user(prefix + chunk)
            except Exception as exc:
                failed += 1
                log.warning("[sender] chunk %d/%d 전송 실패: %s", idx, len(chunks), exc)
        if failed > 0 and failed == len(chunks):
            log.error(
                "[sender] 전체 %d 청크 모두 전송 실패 — 수신자가 메시지를 받지 못했습니다",
                len(chunks),
            )
        elif failed > 0:
            log.warning(
                "[sender] %d/%d 청크 전송 실패 (나머지 %d개는 전송됨)",
                failed,
                len(chunks),
                len(chunks) - failed,
            )

    async def _send_bot(self, text: str) -> None:
        if not self.settings.telegram_bot_token or not self.settings.telegram_bot_target_chat_id:
            raise ValueError(
                "bot 모드는 TELEGRAM_BOT_TOKEN / TELEGRAM_BOT_TARGET_CHAT_ID가 필요합니다."
            )
        url = f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/sendMessage"
        payload: dict[str, Any] = {
            "chat_id": self.settings.telegram_bot_target_chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(url, json=payload)
                if resp.status_code == 400:
                    body = mask_bot_token(resp.text[:500])
                    log.warning("[bot] 400 Bad Request — response: %s", body)
                resp.raise_for_status()
                data = resp.json()
            if not data.get("ok"):
                raise RuntimeError(f"Telegram bot send failed: {mask_bot_token(str(data))}")
        except httpx.HTTPStatusError as exc:
            masked = mask_bot_token(str(exc))
            raise RuntimeError(f"Telegram bot HTTP error: {masked}") from exc
        except httpx.RequestError as exc:
            masked = mask_bot_token(str(exc))
            raise RuntimeError(f"Telegram bot request error: {masked}") from exc

    async def get_bot_updates(self) -> list[dict[str, Any]]:
        if not self.settings.telegram_bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN이 필요합니다.")
        url = f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/getUpdates"
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json()
        except httpx.HTTPStatusError as exc:
            masked = mask_bot_token(str(exc))
            raise RuntimeError(f"getUpdates HTTP error: {masked}") from exc
        return data.get("result", [])
