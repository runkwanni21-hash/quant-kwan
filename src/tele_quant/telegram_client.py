from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from rich.prompt import Prompt
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import Channel, Chat, Message, User

from tele_quant.models import RawItem, utc_now
from tele_quant.settings import Settings
from tele_quant.textutil import clean_text, truncate

log = logging.getLogger(__name__)


class TelegramGateway:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        if settings.telegram_api_id is None or not settings.telegram_api_hash:
            raise ValueError("TELEGRAM_API_ID / TELEGRAM_API_HASH가 필요합니다.")
        self.client = TelegramClient(
            str(settings.telegram_session_path),
            settings.telegram_api_id,
            settings.telegram_api_hash,
        )

    async def connect(self) -> TelegramClient:
        if self.client.is_connected():
            return self.client
        await self.client.connect()
        if not await self.client.is_user_authorized():
            phone = self.settings.telegram_phone or Prompt.ask("텔레그램 전화번호(+82...)")
            await self.client.start(phone=phone)
        return self.client

    async def disconnect(self) -> None:
        await self.client.disconnect()

    async def __aenter__(self) -> TelegramGateway:
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.disconnect()

    async def list_dialogs(
        self, limit: int = 300, only_channels: bool = True
    ) -> list[dict[str, Any]]:
        await self.connect()
        rows: list[dict[str, Any]] = []
        async for dialog in self.client.iter_dialogs(limit=limit):
            entity = dialog.entity
            if only_channels and not isinstance(entity, Channel):
                continue
            username = getattr(entity, "username", None)
            rows.append(
                {
                    "id": getattr(entity, "id", None),
                    "title": getattr(entity, "title", None) or getattr(entity, "first_name", ""),
                    "username": username or "",
                    "type": type(entity).__name__,
                    "unread": dialog.unread_count,
                }
            )
        return rows

    async def fetch_recent_messages(self, hours: float | None = None) -> list[RawItem]:
        await self.connect()
        lookback_hours = hours if hours is not None else self.settings.fetch_lookback_hours
        since = utc_now() - timedelta(hours=lookback_hours)
        entities = await self._source_entities()
        all_items: list[RawItem] = []

        for entity in entities:
            source_name = self._entity_name(entity)
            try:
                items = await self._fetch_entity_messages(entity, since)
                all_items.extend(items)
                log.info("[telegram] %s: %d items", source_name, len(items))
            except FloodWaitError as exc:
                wait = int(getattr(exc, "seconds", 60))
                log.warning("[telegram] FloodWait %ss: %s", wait, source_name)
                await asyncio.sleep(min(wait, 120))
            except Exception as exc:
                log.exception("[telegram] fetch failed for %s: %s", source_name, exc)
        return all_items

    async def send_text_user(self, text: str, target: str | None = None) -> None:
        await self.connect()
        target_chat = target or self.settings.telegram_target_chat or "me"
        await self.client.send_message(target_chat, text, link_preview=False)

    def _is_excluded_entity(self, entity: Any) -> bool:
        """Return True if this entity should be excluded from collection."""
        excluded = {c.lower().lstrip("@") for c in self.settings.exclude_chats}

        # Also exclude the bot's own output channel
        bot_target = (self.settings.telegram_bot_target_chat_id or "").lower().lstrip("@")

        title = (getattr(entity, "title", None) or "").lower()
        username = (getattr(entity, "username", None) or "").lower()
        eid = str(getattr(entity, "id", ""))

        for excl in excluded:
            if excl and excl in (title, username, eid):
                return True
        return bool(bot_target and bot_target in (username, eid))

    async def _source_entities(self) -> list[Any]:
        if self.settings.telegram_include_all_channels:
            entities: list[Any] = []
            async for dialog in self.client.iter_dialogs(limit=None):
                entity = dialog.entity
                if isinstance(entity, User):
                    continue
                if isinstance(entity, Channel):
                    if self._is_excluded_entity(entity):
                        name = getattr(entity, "title", None) or getattr(entity, "username", "?")
                        log.info("[telegram] excluding self/output channel: %s", name)
                        continue
                    entities.append(entity)
            log.info("[telegram] all-channels mode: %d entities found", len(entities))
            return entities

        entities = []
        for raw in self.settings.source_chats:
            try:
                entity = await self.client.get_entity(raw)
                if self._is_excluded_entity(entity):
                    log.info("[telegram] skipping excluded source chat: %s", raw)
                    continue
                entities.append(entity)
            except Exception as exc:
                log.warning("[telegram] source not found: %s (%s)", raw, exc)
        return entities

    async def _fetch_entity_messages(self, entity: Any, since: datetime) -> list[RawItem]:
        items: list[RawItem] = []
        source_name = self._entity_name(entity)
        username = getattr(entity, "username", None)
        chat_id = getattr(entity, "id", source_name)

        async for msg in self.client.iter_messages(
            entity, limit=self.settings.max_messages_per_chat
        ):
            if not isinstance(msg, Message):
                continue
            if not msg.date:
                continue
            msg_dt = msg.date.astimezone(UTC) if msg.date.tzinfo else msg.date.replace(tzinfo=UTC)
            if msg_dt < since:
                break

            text = clean_text(msg.message or "")
            if len(text) < self.settings.min_text_chars:
                continue

            # Self-generated message filter (prevents our own output from being re-collected)
            if self.settings.drop_self_generated_messages and any(
                marker in text for marker in self.settings.self_markers
            ):
                log.debug("[telegram] dropping self-generated msg from %s", source_name)
                continue

            url = f"https://t.me/{username}/{msg.id}" if username else None
            title = truncate(text.splitlines()[0], 120)
            items.append(
                RawItem(
                    source_type="telegram",
                    source_name=source_name,
                    external_id=f"{chat_id}:{msg.id}",
                    published_at=msg_dt,
                    title=title,
                    text=text,
                    url=url,
                    meta={
                        "chat_id": chat_id,
                        "message_id": msg.id,
                        "username": username,
                    },
                )
            )
        return items

    def _entity_name(self, entity: Any) -> str:
        if isinstance(entity, Channel | Chat):
            return (
                getattr(entity, "title", None)
                or getattr(entity, "username", None)
                or str(entity.id)
            )
        if isinstance(entity, User):
            return " ".join(part for part in [entity.first_name, entity.last_name] if part) or str(
                entity.id
            )
        return str(getattr(entity, "id", entity))
