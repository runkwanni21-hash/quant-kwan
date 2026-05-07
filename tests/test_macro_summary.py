from __future__ import annotations

from dataclasses import dataclass, field

from tele_quant.deterministic_report import _build_macro_rate_summary


@dataclass
class _FakeCluster:
    headline: str
    summary_hint: str
    tickers: list[str] = field(default_factory=list)
    source_count: int = 1
    polarity: str = "neutral"


@dataclass
class _FakePack:
    macro: list[_FakeCluster]
    positive_stock: list[_FakeCluster] = field(default_factory=list)
    negative_stock: list[_FakeCluster] = field(default_factory=list)
    total_count: int = 0
    dropped_count: int = 0


def test_rate_keyword_produces_explanation():
    pack = _FakePack(macro=[_FakeCluster(headline="FOMC 금리 결정 예정", summary_hint="")])
    items = _build_macro_rate_summary(pack)
    rate_items = [x for x in items if x.startswith("금리")]
    assert rate_items, "금리 키워드 → 금리 설명 포함 필요"
    assert "성장주" in rate_items[0] or "밸류에이션" in rate_items[0]


def test_fx_keyword_produces_explanation():
    pack = _FakePack(macro=[_FakeCluster(headline="원화 약세 달러 강세", summary_hint="")])
    items = _build_macro_rate_summary(pack)
    fx_items = [x for x in items if x.startswith("환율")]
    assert fx_items, "환율 키워드 → 환율 설명 포함 필요"
    assert "수급" in fx_items[0] or "수출" in fx_items[0]


def test_oil_keyword_produces_explanation():
    pack = _FakePack(macro=[_FakeCluster(headline="WTI 유가 상승", summary_hint="")])
    items = _build_macro_rate_summary(pack)
    oil_items = [x for x in items if x.startswith("유가")]
    assert oil_items, "유가 키워드 → 유가 설명 포함 필요"
    assert "항공" in oil_items[0] or "에너지" in oil_items[0]


def test_empty_macro_no_items():
    pack = _FakePack(macro=[])
    items = _build_macro_rate_summary(pack)
    assert items == []


def test_employment_keyword_produces_explanation():
    pack = _FakePack(macro=[_FakeCluster(headline="고용지표 발표", summary_hint="")])
    items = _build_macro_rate_summary(pack)
    emp_items = [x for x in items if x.startswith("고용")]
    assert emp_items, "고용 키워드 → 고용 설명 포함 필요"
    assert "금리" in emp_items[0]
