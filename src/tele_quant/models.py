from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

SourceType = Literal["telegram", "naver_report", "market_snapshot"]


def utc_now() -> datetime:
    return datetime.now(UTC)


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except ValueError:
        return None


@dataclass(slots=True)
class RawItem:
    source_type: SourceType
    source_name: str
    external_id: str
    published_at: datetime
    text: str
    title: str = ""
    url: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def display_title(self) -> str:
        if self.title.strip():
            return self.title.strip()
        first_line = self.text.strip().splitlines()[0] if self.text.strip() else "Untitled"
        return first_line[:100]

    @property
    def compact_text(self) -> str:
        title = self.display_title
        body = self.text.strip()
        if title and not body.startswith(title):
            return f"{title}\n{body}"
        return body


@dataclass(slots=True)
class RunStats:
    telegram_items: int = 0
    report_items: int = 0
    inserted_items: int = 0
    candidate_items: int = 0
    kept_items: int = 0
    duplicate_items: int = 0
    sent: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "telegram_items": self.telegram_items,
            "report_items": self.report_items,
            "inserted_items": self.inserted_items,
            "candidate_items": self.candidate_items,
            "kept_items": self.kept_items,
            "duplicate_items": self.duplicate_items,
            "sent": self.sent,
        }


@dataclass(slots=True)
class RunReport:
    id: int
    created_at: datetime
    digest: str
    analysis: str | None
    period_hours: float
    mode: str
    stats: dict[str, Any]
