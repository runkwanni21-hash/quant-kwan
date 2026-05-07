from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

from tele_quant.analysis.extractor import fast_extract_candidates
from tele_quant.models import RawItem
from tele_quant.settings import Settings

_ALIASES_PATH = str(Path(__file__).parent.parent / "config" / "ticker_aliases.yml")


def _item(text: str, source: str = "test") -> RawItem:
    return RawItem(
        source_type="telegram",
        source_name=source,
        external_id=f"{source}:{abs(hash(text))}",
        published_at=datetime.now(UTC),
        text=text,
    )


def _settings(analysis_max_symbols: int = 15) -> Settings:
    return Settings(
        ticker_aliases_path=_ALIASES_PATH,
        analysis_max_symbols=analysis_max_symbols,
        telegram_api_id=12345,
        telegram_api_hash="fakehash",
    )


def test_fast_extract_samsung_skhynix_nvda():
    """fast_extract_candidates returns major KR + US stocks without LLM."""
    items = [
        _item("삼성전자 주가 강세, AI 서버 메모리 수요 증가 기대"),
        _item("SK하이닉스 HBM3E 납품 확대로 실적 상승"),
        _item("엔비디아 실적 발표 예정, 주가 급등"),
    ]
    result = fast_extract_candidates(items, _settings())
    symbols = {c.symbol for c in result}
    assert "005930.KS" in symbols, "삼성전자 not found"
    assert "000660.KS" in symbols, "SK하이닉스 not found"
    assert "NVDA" in symbols, "NVDA not found"


def test_fast_extract_does_not_call_llm():
    """fast_extract_candidates must never invoke OllamaClient.generate_text."""
    items = [_item("삼성전자 주가 급등, SK하이닉스 실적 호조")]

    with patch(
        "tele_quant.ollama_client.OllamaClient.generate_text",
        new_callable=AsyncMock,
    ) as mock_llm:
        result = fast_extract_candidates(items, _settings())
        mock_llm.assert_not_called()

    assert len(result) > 0


def test_fast_extract_empty_input():
    """Empty item list returns empty result."""
    assert fast_extract_candidates([], _settings()) == []


def test_fast_extract_no_known_stocks():
    """Text with no stock mentions yields empty result."""
    items = [_item("오늘 날씨가 맑고 기온은 25도입니다")]
    result = fast_extract_candidates(items, _settings())
    assert len(result) == 0


def test_fast_extract_crypto_excluded():
    """CRYPTO market symbols must not appear in fast_extract results."""
    items = [_item("비트코인 급등으로 주가 상승, 삼성전자도 실적 호조")]
    result = fast_extract_candidates(items, _settings())
    symbols = {c.symbol for c in result}
    assert "BTC-USD" not in symbols
    assert "005930.KS" in symbols


def test_fast_extract_max_symbols_respected():
    """Result length must not exceed analysis_max_symbols."""
    items = [
        _item(
            "삼성전자 SK하이닉스 현대차 기아 셀트리온 엔비디아 테슬라 메타 "
            "삼성SDI LG에너지솔루션 알테오젠 HD현대중공업 두산에너빌리티 주가 실적"
        )
    ]
    result = fast_extract_candidates(items, _settings(5))
    assert len(result) <= 5


def test_fast_extract_returns_stock_candidates():
    """Results should be StockCandidate instances with expected fields."""
    from tele_quant.analysis.models import StockCandidate

    items = [_item("삼성전자 실적 발표, 주가 급등")]
    result = fast_extract_candidates(items, _settings())
    assert len(result) > 0
    assert all(isinstance(c, StockCandidate) for c in result)
    samsung = next((c for c in result if c.symbol == "005930.KS"), None)
    assert samsung is not None
    assert samsung.mentions >= 1
    assert samsung.market == "KR"
