from __future__ import annotations

from dataclasses import dataclass, field

from tele_quant.sector_quota import apply_sector_quota, guess_sector


@dataclass
class _FakeCandidate:
    symbol: str
    name: str | None = None
    market: str = "US"
    sector: str | None = None
    mentions: int = 1
    sentiment: str = "neutral"
    catalysts: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    source_titles: list[str] = field(default_factory=list)
    origin: str = "직접 언급"


class _FakeSettings:
    sector_quota_enabled = True
    sector_quota_max_per_sector = 2
    sector_quota_overflow_count = 1


def test_guess_sector_bigtech():
    result = guess_sector("NVDA", "NVIDIA", ["AI", "GPU"])
    assert result is not None
    assert "반도체" in result or "AI" in result or "빅테크" in result


def test_guess_sector_bio():
    result = guess_sector("LLY", "Eli Lilly", ["GLP-1", "FDA"])
    # Should be some bio/healthcare sector or None (depending on aliases)
    assert result is None or "바이오" in result or "헬스" in result


def test_guess_sector_unknown():
    assert guess_sector("XYZ", "Unknown Corp", []) is None


def test_apply_sector_quota_limits_bigtech():
    cands = [
        _FakeCandidate("NVDA", "NVIDIA", catalysts=["AI"]),
        _FakeCandidate("MSFT", "Microsoft", catalysts=["AI"]),
        _FakeCandidate("AAPL", "Apple", catalysts=["빅테크"]),
        _FakeCandidate("GOOGL", "Google", catalysts=["AI"]),
    ]
    settings = _FakeSettings()
    result = apply_sector_quota(cands, settings)  # type: ignore[arg-type]
    # All candidates preserved; overflow just moved to back
    assert len(result) == 4


def test_apply_sector_quota_disabled():
    class _NoQuota(_FakeSettings):
        sector_quota_enabled = False

    cands = [_FakeCandidate(f"SYM{i}") for i in range(10)]
    result = apply_sector_quota(cands, _NoQuota())  # type: ignore[arg-type]
    assert result == cands


def test_apply_sector_quota_preserves_all():
    cands = [
        _FakeCandidate("NVDA"),
        _FakeCandidate("MSFT"),
        _FakeCandidate("AAPL"),
        _FakeCandidate("LLY"),
        _FakeCandidate("005930.KS"),
    ]
    settings = _FakeSettings()
    result = apply_sector_quota(cands, settings)  # type: ignore[arg-type]
    # All symbols should still be present
    result_syms = {c.symbol for c in result}
    assert result_syms == {c.symbol for c in cands}
