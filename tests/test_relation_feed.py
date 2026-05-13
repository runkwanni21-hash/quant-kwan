from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from tele_quant.relation_feed import (
    LeadLagCandidateRow,
    MoverRow,
    RelationFeedData,
    RelationFeedSummary,
    build_relation_feed_section,
    get_relation_boost,
    load_relation_feed,
)

_FEED_DIR = Path("/home/kwanni/projects/quant_spillover/shared_relation_feed")

_FORBIDDEN_PATTERNS = re.compile(
    r"ACTION_READY|LIVE_READY|\bBUY\b|\bSELL\b|\bORDER\b"
    r"|확정 수익|반드시 상승|무조건 매수",
    re.IGNORECASE,
)
_MACRO_ONLY_FORBIDDEN = re.compile(r"롱 관심|숏/매도|관심 진입|손절|목표/매도 관찰")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(**kwargs):
    """Minimal settings-like object."""

    class FakeSettings:
        relation_feed_enabled = True
        relation_feed_dir = str(_FEED_DIR)
        relation_feed_max_age_hours = 72.0
        relation_feed_min_confidence = "medium"
        relation_feed_max_movers = 8
        relation_feed_max_targets_per_mover = 3

        def __init__(self, **kw):
            for k, v in kw.items():
                setattr(self, k, v)

    return FakeSettings(**kwargs)


def _make_feed_with_rows() -> RelationFeedData:
    feed = RelationFeedData(
        summary=RelationFeedSummary(
            generated_at="2026-05-07T08:00:00+09:00",
            asof_date="2026-05-04",
            mover_rows=2,
            leadlag_rows=2,
            status="ok",
            warnings=["TEST_WARNING"],
        ),
        movers=[
            MoverRow(
                asof_date="2026-05-04",
                market="US",
                symbol="SNDK",
                name="Sandisk",
                sector="",
                close=100.0,
                prev_close=65.0,
                return_pct=54.6,
                volume=None,
                volume_ratio_20d=None,
                move_type="UP",
            ),
            MoverRow(
                asof_date="2026-05-04",
                market="US",
                symbol="RIVN",
                name="Rivian",
                sector="",
                close=10.0,
                prev_close=11.0,
                return_pct=-9.2,
                volume=None,
                volume_ratio_20d=None,
                move_type="DOWN",
            ),
        ],
        leadlag=[
            LeadLagCandidateRow(
                asof_date="2026-05-04",
                source_market="US",
                source_symbol="SNDK",
                source_name="Sandisk",
                source_sector="",
                source_move_type="UP",
                source_return_pct=54.6,
                target_market="US",
                target_symbol="MU",
                target_name="Micron",
                target_sector="",
                relation_type="UP_LEADS_UP",
                lag_days=1,
                event_count=20,
                hit_count=12,
                conditional_prob=0.625,
                lift=1.8,
                confidence="medium",
                direction="beneficiary",
                note="beneficiary candidate",
            ),
        ],
    )
    return feed


# ---------------------------------------------------------------------------
# Tests: summary JSON read
# ---------------------------------------------------------------------------


def test_summary_json_read():
    """실제 피드 파일에서 summary를 읽는다."""
    if not (_FEED_DIR / "latest_relation_summary.json").exists():
        pytest.skip("shared_relation_feed 없음")

    settings = _make_settings()
    feed = load_relation_feed(settings)

    assert feed.available
    assert feed.summary is not None
    assert feed.summary.asof_date != ""
    assert feed.summary.mover_rows >= 0


# ---------------------------------------------------------------------------
# Tests: movers CSV read
# ---------------------------------------------------------------------------


def test_movers_csv_read():
    if not (_FEED_DIR / "latest_movers.csv").exists():
        pytest.skip("shared_relation_feed 없음")

    settings = _make_settings()
    feed = load_relation_feed(settings)

    assert len(feed.movers) > 0
    for m in feed.movers:
        assert m.move_type in ("UP", "DOWN")
        assert m.symbol != ""


# ---------------------------------------------------------------------------
# Tests: leadlag CSV read
# ---------------------------------------------------------------------------


def test_leadlag_csv_read():
    if not (_FEED_DIR / "latest_leadlag_candidates.csv").exists():
        pytest.skip("shared_relation_feed 없음")

    settings = _make_settings()
    feed = load_relation_feed(settings)

    # confidence=low가 기본 숨김(min_confidence=medium)이므로 low rows가 없어야 함
    for row in feed.leadlag:
        assert row.confidence != "low", "low confidence row가 포함됨"


# ---------------------------------------------------------------------------
# Tests: confidence=low 숨김
# ---------------------------------------------------------------------------


def test_confidence_low_hidden(tmp_path: Path):
    """confidence=low rows가 기본 필터링됨."""
    feed_dir = tmp_path / "feed"
    feed_dir.mkdir()

    # Write summary
    (feed_dir / "latest_relation_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-05-07T08:00:00+09:00",
                "asof_date": "2026-05-04",
                "status": "ok",
                "warnings": [],
            }
        )
    )

    # Write leadlag CSV with low + medium rows
    (feed_dir / "latest_movers.csv").write_text(
        "asof_date,market,symbol,name,sector,close,prev_close,return_pct,volume,volume_ratio_20d,move_type\n"
    )
    (feed_dir / "latest_leadlag_candidates.csv").write_text(
        "asof_date,source_market,source_symbol,source_name,source_sector,"
        "source_move_type,source_return_pct,target_market,target_symbol,target_name,"
        "target_sector,relation_type,lag_days,event_count,hit_count,conditional_prob,"
        "lift,confidence,direction,note\n"
        "2026-05-04,US,ZSL,ZSL Corp,ETF,UP,6.0,US,EP,Empire,,"
        "UP_LEADS_UP,20,44,8,0.19,1.75,low,beneficiary,test\n"
        "2026-05-04,US,ZSL,ZSL Corp,ETF,UP,6.0,US,USBC,USBC Corp,,"
        "UP_LEADS_UP,20,55,10,0.17,1.59,medium,beneficiary,test\n"
    )

    settings = _make_settings(relation_feed_dir=str(feed_dir))
    feed = load_relation_feed(settings)

    assert len(feed.leadlag) == 1
    assert feed.leadlag[0].confidence == "medium"


# ---------------------------------------------------------------------------
# Tests: graceful fallback when files absent
# ---------------------------------------------------------------------------


def test_graceful_fallback_no_files(tmp_path: Path):
    """파일 없으면 graceful fallback, 예외 없음."""
    settings = _make_settings(relation_feed_dir=str(tmp_path / "nonexistent"))
    feed = load_relation_feed(settings)

    assert not feed.available
    assert len(feed.load_warnings) > 0
    assert not feed.movers
    assert not feed.leadlag


# ---------------------------------------------------------------------------
# Tests: stale feed warning
# ---------------------------------------------------------------------------


def test_stale_feed_warning(tmp_path: Path):
    """max_age_hours 초과하면 is_stale=True, warning 표시."""
    feed_dir = tmp_path / "feed"
    feed_dir.mkdir()

    (feed_dir / "latest_relation_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2020-01-01T00:00:00+00:00",  # 아주 오래전
                "asof_date": "2020-01-01",
                "status": "ok",
                "warnings": [],
            }
        )
    )
    (feed_dir / "latest_movers.csv").write_text(
        "asof_date,market,symbol,name,sector,close,prev_close,return_pct,volume,volume_ratio_20d,move_type\n"
    )
    (feed_dir / "latest_leadlag_candidates.csv").write_text(
        "asof_date,source_market,source_symbol,source_name,source_sector,"
        "source_move_type,source_return_pct,target_market,target_symbol,target_name,"
        "target_sector,relation_type,lag_days,event_count,hit_count,conditional_prob,"
        "lift,confidence,direction,note\n"
    )

    settings = _make_settings(relation_feed_dir=str(feed_dir), relation_feed_max_age_hours=72.0)
    feed = load_relation_feed(settings)

    assert feed.available  # stale해도 로드는 됨
    assert feed.is_stale
    stale_warns = [w for w in feed.load_warnings if "오래됨" in w]
    assert len(stale_warns) > 0


# ---------------------------------------------------------------------------
# Tests: report section generation
# ---------------------------------------------------------------------------


def test_report_section_generation():
    """섹션 문자열이 생성되고 필수 요소를 포함."""
    feed = _make_feed_with_rows()
    section = build_relation_feed_section(feed)

    assert "후행 관찰 후보" in section
    assert "SNDK" in section
    assert "통계적 관찰 목록" in section


def test_report_section_shows_target():
    """target 종목이 섹션에 표시됨."""
    feed = _make_feed_with_rows()
    section = build_relation_feed_section(feed)
    assert "MU" in section
    assert "Micron" in section


def test_report_section_empty_feed():
    """feed가 없으면 warning 섹션만 반환 또는 빈 문자열."""
    feed = RelationFeedData()
    feed.load_warnings.append("relation feed 없음")
    section = build_relation_feed_section(feed)
    assert "후행 관찰 후보" in section or section == ""


# ---------------------------------------------------------------------------
# Tests: 금지 표현 없음
# ---------------------------------------------------------------------------


def test_no_forbidden_expressions():
    """금지 표현이 섹션에 없음."""
    feed = _make_feed_with_rows()
    section = build_relation_feed_section(feed)
    assert not _FORBIDDEN_PATTERNS.search(section), f"금지 표현 발견: {section}"


def test_no_forbidden_expressions_macro_only():
    """macro_only 섹션에 매수/매도 지시 표현 없음."""
    feed = _make_feed_with_rows()
    section = build_relation_feed_section(feed, macro_only=True)
    assert not _MACRO_ONLY_FORBIDDEN.search(section), f"macro_only 금지 표현 발견: {section}"


# ---------------------------------------------------------------------------
# Tests: target 중복 제거
# ---------------------------------------------------------------------------


def test_target_deduplication():
    """같은 target이 중복 표시되지 않음."""
    feed = RelationFeedData(
        summary=RelationFeedSummary(
            generated_at="2026-05-07T08:00:00+09:00",
            asof_date="2026-05-04",
            status="ok",
        ),
        movers=[
            MoverRow("2026-05-04", "US", "SNDK", "Sandisk", "", 100.0, 65.0, 54.6, None, None, "UP")
        ],
        leadlag=[
            LeadLagCandidateRow(
                "2026-05-04",
                "US",
                "SNDK",
                "Sandisk",
                "",
                "UP",
                54.6,
                "US",
                "MU",
                "Micron",
                "",
                "UP_LEADS_UP",
                1,
                20,
                12,
                0.625,
                1.8,
                "medium",
                "beneficiary",
                "",
            ),
            LeadLagCandidateRow(
                "2026-05-04",
                "US",
                "SNDK",
                "Sandisk",
                "",
                "UP",
                54.6,
                "US",
                "MU",
                "Micron",
                "",
                "UP_LEADS_UP",  # 중복
                20,
                20,
                12,
                0.500,
                1.5,
                "medium",
                "beneficiary",
                "",
            ),
        ],
    )
    section = build_relation_feed_section(feed)
    # MU가 두 번 나오면 안 됨
    assert section.count("MU") <= 2  # 한 번은 target에, 한 번은 이름에


# ---------------------------------------------------------------------------
# Tests: score boost
# ---------------------------------------------------------------------------


def test_relation_boost_medium():
    """medium confidence boost=1."""
    feed = _make_feed_with_rows()
    boost, note = get_relation_boost(feed, "MU", has_telegram_evidence=True, technical_ok=True)
    assert boost == 1.0
    assert "SNDK" in note or "Sandisk" in note


def test_relation_boost_no_telegram():
    """telegram evidence 없으면 boost=0."""
    feed = _make_feed_with_rows()
    boost, _note = get_relation_boost(feed, "MU", has_telegram_evidence=False, technical_ok=True)
    assert boost == 0.0


def test_relation_boost_no_technical():
    """technical ok 아니면 boost=0."""
    feed = _make_feed_with_rows()
    boost, _note = get_relation_boost(feed, "MU", has_telegram_evidence=True, technical_ok=False)
    assert boost == 0.0


def test_relation_boost_not_in_feed():
    """feed에 없는 심볼은 boost=0."""
    feed = _make_feed_with_rows()
    boost, _note = get_relation_boost(feed, "AAPL", has_telegram_evidence=True, technical_ok=True)
    assert boost == 0.0


def test_relation_boost_none_feed():
    """feed=None이면 boost=0."""
    boost, _note = get_relation_boost(None, "MU", has_telegram_evidence=True, technical_ok=True)
    assert boost == 0.0


# ---------------------------------------------------------------------------
# Tests: stale feed 40시간 섹션 숨김
# ---------------------------------------------------------------------------


def test_stale_feed_section_hidden():
    """is_stale=True이면 build_relation_feed_section이 빈 문자열 반환."""
    feed = _make_feed_with_rows()
    feed.is_stale = True
    feed.feed_age_hours = 41.0
    section = build_relation_feed_section(feed)
    assert section == ""
    assert "⚡ 과거 급등" not in section
    assert "보성파워텍" not in section
    assert "에스티큐브" not in section


def test_stale_feed_section_no_heavy_content():
    """stale feed 시 후행 관찰 후보 상세가 나오지 않는다."""
    feed = _make_feed_with_rows()
    feed.is_stale = True
    feed.feed_age_hours = 137.0
    section = build_relation_feed_section(feed)
    # 제목 자체가 없어야 함
    assert "⚡" not in section
    assert "후행 관찰 후보" not in section


def test_fresh_feed_section_shown():
    """is_stale=False이면 섹션이 정상 표시된다."""
    feed = _make_feed_with_rows()
    feed.is_stale = False
    feed.feed_age_hours = 10.0
    section = build_relation_feed_section(feed)
    assert section != ""
    assert "후행 관찰 후보" in section
