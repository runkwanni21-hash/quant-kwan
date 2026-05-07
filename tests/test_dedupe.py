from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tele_quant.dedupe import Deduper
from tele_quant.models import RawItem
from tele_quant.settings import Settings


def item(text: str, external_id: str) -> RawItem:
    return RawItem(
        source_type="telegram",
        source_name="test",
        external_id=external_id,
        published_at=datetime(2026, 1, 1, tzinfo=UTC),
        text=text,
    )


@pytest.mark.asyncio
async def test_dedupe_exact_and_fuzzy() -> None:
    settings = Settings(
        telegram_api_id=1,
        telegram_api_hash="x",
        telegram_source_chats="a",
        embedding_dedupe=False,
    )
    deduper = Deduper(settings)
    out = await deduper.dedupe(
        [
            item("삼성전자 HBM 공급 확대 기대", "1"),
            item("삼성전자 HBM 공급 확대 기대", "2"),
            item("삼성전자 HBM 공급 확대 기대감", "3"),
            item("미국 10년물 금리 상승", "4"),
        ]
    )
    assert len(out) == 2
