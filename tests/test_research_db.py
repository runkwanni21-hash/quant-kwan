from __future__ import annotations

import tempfile
from pathlib import Path

from tele_quant.research_db import (
    ResearchLeadLagPair,
    find_related_targets,
    find_sources_for_target,
    load_research_pairs,
    normalize_research_symbol,
    summarize_research_context,
    to_yfinance_symbol,
)

# ------- Symbol normalization -------


def test_normalize_kr_ks():
    assert normalize_research_symbol("005930.KS") == "KR:005930"


def test_normalize_kr_kq():
    assert normalize_research_symbol("000660.KQ") == "KR:000660"


def test_normalize_us_plain():
    assert normalize_research_symbol("NVDA") == "US:NVDA"


def test_normalize_already_normalized():
    assert normalize_research_symbol("KR:005930") == "KR:005930"
    assert normalize_research_symbol("US:AAPL") == "US:AAPL"


def test_to_yfinance_kr():
    assert to_yfinance_symbol("KR:005930") == "005930.KS"


def test_to_yfinance_us():
    assert to_yfinance_symbol("US:NVDA") == "NVDA"


def test_to_yfinance_passthrough():
    assert to_yfinance_symbol("AAPL") == "AAPL"


# ------- CSV loading with temp files -------

_CSV_HEADER = (
    "source_security_id,source_symbol,source_name,source_market,source_sector,"
    "target_security_id,target_symbol,target_name,target_market,target_sector,"
    "market_direction,follow_relation_type,lag_days,event_count,overlap_count,"
    "hit_rate,baseline_hit_rate,lift,avg_target_follow_return,baseline_avg_target_return,"
    "excess_avg_target_return,subwindow_stability_flag,outlier_event_count,"
    "ranking_score,reliability_bucket,caution_reason\n"
)


def _make_row(
    src_sym="NVDA",
    src_market="US",
    src_name="NVIDIA",
    tgt_sym="AMD",
    tgt_market="US",
    tgt_name="AMD Inc",
    direction="US->US",
    relation="UP_LEADS_UP",
    lag=1,
    lift=2.0,
    excess=0.05,
    stability="STABLE",
    outliers=0,
    ranking_score=20.0,
    reliability_bucket="promising_research_candidate",
    caution_reason="",
):
    return (
        f"US:{src_sym},{src_sym},{src_name},{src_market},Tech,"
        f"US:{tgt_sym},{tgt_sym},{tgt_name},{tgt_market},Tech,"
        f"{direction},{relation},{lag},50,50,"
        f"0.8,0.4,{lift},0.02,0.001,{excess},{stability},{outliers},"
        f"{ranking_score},{reliability_bucket},{caution_reason}\n"
    )


def _build_package(base_dir: Path, rows: list[str], caution_rows: list[str] | None = None):
    """Create a fake research_package_* directory with CSV files."""
    pkg = base_dir / "research_package_20260101T000000Z" / "artifacts" / "csv"
    pkg.mkdir(parents=True)
    for fname in [
        "top_strict_stable_no_outlier_pairs.csv",
        "top_us_to_us_pairs.csv",
        "top_us_to_kr_pairs.csv",
        "top_kr_to_us_pairs.csv",
        "top_kr_to_kr_pairs.csv",
    ]:
        (pkg / fname).write_text(_CSV_HEADER + "".join(rows), encoding="utf-8")
    if caution_rows is not None:
        (pkg / "pairs_requiring_caution.csv").write_text(
            _CSV_HEADER + "".join(caution_rows), encoding="utf-8"
        )
    return base_dir


class _FakeSettings:
    research_db_enabled = True
    research_db_path = ""
    research_package_path = ""
    research_leadlag_enabled = True
    research_top_pairs_limit = 200
    research_min_reliability = "promising_research_candidate"
    research_allow_caution = False


def test_load_research_pairs_basic():
    with tempfile.TemporaryDirectory() as tmp:
        _build_package(Path(tmp), [_make_row()])
        s = _FakeSettings()
        s.research_db_path = tmp
        pairs = load_research_pairs(s)
    assert len(pairs) >= 1
    assert pairs[0].source_ticker == "NVDA"
    assert pairs[0].target_ticker == "AMD"
    assert pairs[0].relation == "UP_LEADS_UP"


def test_load_research_pairs_dedup():
    """Same pair appearing in multiple CSVs should not be duplicated."""
    row = _make_row()
    with tempfile.TemporaryDirectory() as tmp:
        _build_package(Path(tmp), [row])
        s = _FakeSettings()
        s.research_db_path = tmp
        pairs = load_research_pairs(s)
    nvda_amd = [p for p in pairs if p.source_ticker == "NVDA" and p.target_ticker == "AMD"]
    assert len(nvda_amd) == 1


def test_load_research_pairs_caution_excluded_by_default():
    caution_row = _make_row(
        src_sym="RISK",
        tgt_sym="DANGER",
        reliability_bucket="caution_outlier_or_unstable",
        caution_reason="outlier",
    )
    with tempfile.TemporaryDirectory() as tmp:
        _build_package(Path(tmp), [], caution_rows=[caution_row])
        s = _FakeSettings()
        s.research_db_path = tmp
        s.research_allow_caution = False
        pairs = load_research_pairs(s)
    assert not any(p.source_ticker == "RISK" for p in pairs)


def test_load_research_pairs_caution_included_when_allowed():
    caution_row = _make_row(
        src_sym="RISK",
        tgt_sym="DANGER",
        reliability_bucket="caution_outlier_or_unstable",
        caution_reason="outlier",
    )
    with tempfile.TemporaryDirectory() as tmp:
        _build_package(Path(tmp), [], caution_rows=[caution_row])
        s = _FakeSettings()
        s.research_db_path = tmp
        s.research_allow_caution = True
        pairs = load_research_pairs(s)
    caution = [p for p in pairs if p.source_ticker == "RISK"]
    assert len(caution) == 1
    assert caution[0].is_caution is True


def test_load_research_pairs_disabled():
    s = _FakeSettings()
    s.research_db_enabled = False
    pairs = load_research_pairs(s)
    assert pairs == []


def test_load_research_pairs_missing_path():
    s = _FakeSettings()
    s.research_db_path = "/nonexistent/path"
    s.research_package_path = "/also/nonexistent"
    pairs = load_research_pairs(s)
    assert pairs == []


# ------- find_related_targets / find_sources_for_target -------


def _sample_pairs():
    return [
        ResearchLeadLagPair(
            source_market="US",
            source_ticker="NVDA",
            source_name="NVIDIA",
            target_market="US",
            target_ticker="AMD",
            target_name="AMD Inc",
            relation="UP_LEADS_UP",
            lag=1,
            lift=2.0,
            excess=0.05,
            stability="STABLE",
            outliers=0,
            reliability_bucket="promising_research_candidate",
            direction="US->US",
            ranking_score=25.0,
            hit_rate=0.8,
            event_count=50,
        ),
        ResearchLeadLagPair(
            source_market="US",
            source_ticker="NVDA",
            source_name="NVIDIA",
            target_market="KR",
            target_ticker="000660.KS",
            target_name="SK하이닉스",
            relation="UP_LEADS_UP",
            lag=2,
            lift=1.8,
            excess=0.04,
            stability="STABLE",
            outliers=0,
            reliability_bucket="promising_research_candidate",
            direction="US->KR",
            ranking_score=20.0,
            hit_rate=0.7,
            event_count=40,
        ),
        ResearchLeadLagPair(
            source_market="US",
            source_ticker="SPY",
            source_name="S&P500 ETF",
            target_market="US",
            target_ticker="NVDA",
            target_name="NVIDIA",
            relation="UP_LEADS_UP",
            lag=1,
            lift=1.5,
            excess=0.02,
            stability="STABLE",
            outliers=0,
            reliability_bucket="promising_research_candidate",
            direction="US->US",
            ranking_score=15.0,
            hit_rate=0.65,
            event_count=100,
        ),
    ]


def test_find_related_targets_source_nvda():
    pairs = _sample_pairs()
    targets = find_related_targets(pairs, "NVDA")
    syms = [p.target_ticker for p in targets]
    assert "AMD" in syms
    assert "000660.KS" in syms


def test_find_related_targets_relation_filter():
    pairs = _sample_pairs()
    targets = find_related_targets(pairs, "NVDA", relation="DOWN_LEADS_DOWN")
    assert targets == []


def test_find_related_targets_direction_filter():
    pairs = _sample_pairs()
    targets = find_related_targets(pairs, "NVDA", direction="US->KR")
    assert all(p.direction == "US->KR" for p in targets)
    assert any(p.target_ticker == "000660.KS" for p in targets)


def test_find_sources_for_target():
    pairs = _sample_pairs()
    sources = find_sources_for_target(pairs, "NVDA")
    assert any(p.source_ticker == "SPY" for p in sources)


def test_find_sources_empty():
    pairs = _sample_pairs()
    sources = find_sources_for_target(pairs, "UNKNOWN")
    assert sources == []


def test_summarize_research_context_has_content():
    pairs = _sample_pairs()
    summary = summarize_research_context(pairs, "NVDA")
    assert "연구DB" in summary
    assert "통계 후보" in summary


def test_summarize_research_context_empty_for_unknown():
    pairs = _sample_pairs()
    summary = summarize_research_context(pairs, "UNKNOWN_XYZ")
    assert summary == ""
