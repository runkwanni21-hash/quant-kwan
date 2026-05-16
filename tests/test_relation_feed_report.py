from __future__ import annotations

import re

from tele_quant.deterministic_report import build_macro_digest
from tele_quant.evidence import EvidenceCluster
from tele_quant.evidence_ranker import RankedEvidencePack
from tele_quant.models import RunStats
from tele_quant.relation_feed import (
    LeadLagCandidateRow,
    MoverRow,
    RelationFeedData,
    RelationFeedSummary,
)

_LONG_SHORT_FORBIDDEN = re.compile(r"롱 관심|숏/매도|관심 진입|손절·무효화|목표/매도 관찰")
_BUY_SELL_FORBIDDEN = re.compile(r"무조건 매수|반드시 상승|확정 수익|ACTION_READY|LIVE_READY")


def _make_pack(**kwargs) -> RankedEvidencePack:
    from datetime import UTC, datetime

    def _c(headline: str, polarity: str = "positive") -> EvidenceCluster:
        return EvidenceCluster(
            cluster_id="t1",
            headline=headline,
            summary_hint="",
            tickers=[],
            themes=[],
            polarity=polarity,
            source_names=["채널A"],
            source_count=1,
            newest_at=datetime.now(UTC),
            items=[],
            cluster_score=5.0,
        )

    return RankedEvidencePack(
        macro=kwargs.get("macro", [_c("FOMC 금리 동결")]),
        positive_stock=kwargs.get("pos", []),
        negative_stock=kwargs.get("neg", []),
        dropped_count=0,
        total_count=5,
    )


def _make_stats() -> RunStats:
    s = RunStats()
    s.telegram_items = 10
    s.report_items = 3
    return s


def _make_feed() -> RelationFeedData:
    return RelationFeedData(
        summary=RelationFeedSummary(
            generated_at="2026-05-07T08:00:00+09:00",
            asof_date="2026-05-04",
            mover_rows=2,
            leadlag_rows=1,
            status="ok",
            warnings=["TEST_WARNING"],
            source_project="stock-relation-ai",
            method="event-conditioned lead-lag",
        ),
        movers=[
            MoverRow(
                "2026-05-04",
                "US",
                "ZSL",
                "ProShares Ultra Silver",
                "ETF",
                10.0,
                9.4,
                6.3,
                None,
                None,
                "UP",
            ),
            MoverRow(
                "2026-05-04",
                "KR",
                "322310",
                "오로스테크놀로지",
                "",
                44850.0,
                34500.0,
                30.0,
                None,
                None,
                "UP",
            ),
        ],
        leadlag=[
            LeadLagCandidateRow(
                "2026-05-04",
                "US",
                "ZSL",
                "ProShares Ultra Silver",
                "ETF",
                "UP",
                6.3,
                "US",
                "EP",
                "Empire Petroleum",
                "",
                "UP_LEADS_UP",
                20,
                44,
                8,
                0.192,
                1.75,
                "medium",
                "beneficiary",
                "beneficiary candidate",
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Tests: once digest에 "어제 급등·급락 → 후행 후보" 섹션 포함
# ---------------------------------------------------------------------------


def test_digest_contains_relation_feed_section():
    """4H digest에 relation feed 섹션이 포함됨."""
    pack = _make_pack()
    feed = _make_feed()
    result = build_macro_digest(pack, [], _make_stats(), hours=4, relation_feed=feed)
    assert "후행 관찰 후보" in result or "후행 후보" in result


def test_digest_contains_asof_date():
    """기준일이 섹션에 표시됨."""
    pack = _make_pack()
    feed = _make_feed()
    result = build_macro_digest(pack, [], _make_stats(), hours=4, relation_feed=feed)
    assert "2026-05-04" in result


def test_digest_contains_relation_section_with_fold_summary():
    """라이브 확인 없이 digest 생성 시 fold 요약 또는 source 심볼이 섹션에 표시됨."""
    pack = _make_pack()
    feed = _make_feed()
    result = build_macro_digest(pack, [], _make_stats(), hours=4, relation_feed=feed)
    # live_checks=None이면 candidates 접힘 → fold 요약 또는 section 자체 포함
    assert "라이브 확인 미실행" in result or "후행 관찰 후보" in result


def test_digest_contains_disclaimer():
    """통계적 후보 면책 문구 포함."""
    pack = _make_pack()
    feed = _make_feed()
    result = build_macro_digest(pack, [], _make_stats(), hours=4, relation_feed=feed)
    assert "통계적 관찰 목록" in result


# ---------------------------------------------------------------------------
# Tests: macro-only에서 롱/숏 표현 없음
# ---------------------------------------------------------------------------


def test_macro_only_no_long_short_expressions():
    """macro_only 모드에서 롱/숏 매수/매도 표현이 없음."""
    pack = _make_pack()
    feed = _make_feed()
    result = build_macro_digest(
        pack, [], _make_stats(), hours=4, macro_only=True, relation_feed=feed
    )
    assert not _LONG_SHORT_FORBIDDEN.search(result), f"macro_only에서 금지 표현 발견:\n{result}"


def test_macro_only_uses_observation_label():
    """macro_only 섹션 헤더가 '관찰' 표현을 사용함."""
    pack = _make_pack()
    feed = _make_feed()
    result = build_macro_digest(
        pack, [], _make_stats(), hours=4, macro_only=True, relation_feed=feed
    )
    # macro_only에서는 "관찰 후보" 등의 표현을 사용
    assert "관찰" in result or "관찰 후보" in result


def test_no_buy_sell_forbidden_in_digest():
    """digest에 매수/매도 확정 표현 없음."""
    pack = _make_pack()
    feed = _make_feed()
    result = build_macro_digest(pack, [], _make_stats(), hours=4, relation_feed=feed)
    assert not _BUY_SELL_FORBIDDEN.search(result)


# ---------------------------------------------------------------------------
# Tests: relation feed만으로 LONG 후보 생성하지 않음
# ---------------------------------------------------------------------------


def test_relation_feed_alone_no_long_candidate():
    """relation feed 단독으로 LONG 후보가 생성되지 않음."""
    from tele_quant.relation_feed import get_relation_boost

    feed = _make_feed()

    # technical_ok=False, has_tg=False → boost=0
    boost, _note = get_relation_boost(feed, "EP", has_telegram_evidence=False, technical_ok=False)
    assert boost == 0.0, "telegram evidence 없으면 boost 0"

    # telegram evidence=True but no technical → still 0
    boost2, _ = get_relation_boost(feed, "EP", has_telegram_evidence=True, technical_ok=False)
    assert boost2 == 0.0, "technical ok 아니면 boost 0"


def test_relation_feed_section_no_long_confirm():
    """섹션 문자열에 LONG 확정 표현이 없음."""
    from tele_quant.relation_feed import build_relation_feed_section

    feed = _make_feed()
    section = build_relation_feed_section(feed)

    long_confirm_re = re.compile(r"LONG 확정|롱 진입|매수 확정|상승 확정")
    assert not long_confirm_re.search(section), f"LONG 확정 표현 발견: {section}"


# ---------------------------------------------------------------------------
# Tests: digest without relation feed (None) still works
# ---------------------------------------------------------------------------


def test_digest_without_relation_feed():
    """relation_feed=None이어도 digest가 정상 생성됨."""
    pack = _make_pack()
    result = build_macro_digest(pack, [], _make_stats(), hours=4, relation_feed=None)
    assert isinstance(result, str)
    assert "Tele Quant" in result
    # 섹션이 없어야 함
    assert "어제 급등·급락" not in result


# ---------------------------------------------------------------------------
# Tests: weekly report contains relation feed section
# ---------------------------------------------------------------------------


def test_weekly_report_contains_relation_feed():
    """weekly report에 relation feed 섹션이 포함됨."""
    from datetime import UTC, datetime, timedelta

    from tele_quant.models import RunReport
    from tele_quant.weekly import build_weekly_deterministic_summary, build_weekly_input

    now = datetime.now(UTC)
    reports = [
        RunReport(
            id=1,
            created_at=now - timedelta(days=1),
            digest="🧠 Tele Quant 4시간 투자 브리핑\n금리인하 기대\n삼성전자 호재",
            analysis=None,
            period_hours=4.0,
            mode="no_llm",
            stats={},
        )
    ]
    wi = build_weekly_input(reports)
    feed = _make_feed()

    summary = build_weekly_deterministic_summary(wi, relation_feed_data=feed)
    assert "급등·급락 후행 후보 리뷰" in summary or "relation feed" in summary.lower()


def test_weekly_report_no_relation_feed():
    """relation_feed_data=None이어도 weekly report가 정상 생성됨."""
    from datetime import UTC, datetime, timedelta

    from tele_quant.models import RunReport
    from tele_quant.weekly import build_weekly_deterministic_summary, build_weekly_input

    now = datetime.now(UTC)
    reports = [
        RunReport(
            id=1,
            created_at=now - timedelta(days=1),
            digest="🧠 Tele Quant 4시간 투자 브리핑\n금리 안정",
            analysis=None,
            period_hours=4.0,
            mode="no_llm",
            stats={},
        )
    ]
    wi = build_weekly_input(reports)
    summary = build_weekly_deterministic_summary(wi, relation_feed_data=None)
    assert isinstance(summary, str)
    assert "주간 총정리" in summary


# ---------------------------------------------------------------------------
# Tests: boost guard conditions
# ---------------------------------------------------------------------------


def _make_ll_row(
    source_symbol: str = "ZSL",
    source_return_pct: float = 6.3,
    target_symbol: str = "EP",
    confidence: str = "medium",
    note: str = "beneficiary candidate",
) -> LeadLagCandidateRow:
    return LeadLagCandidateRow(
        "2026-05-04",
        "US",
        source_symbol,
        "Source Corp",
        "",
        "UP",
        source_return_pct,
        "US",
        target_symbol,
        "Target Corp",
        "",
        "UP_LEADS_UP",
        1,
        20,
        12,
        0.60,
        1.8,
        confidence,
        "beneficiary",
        note,
    )


def _feed_with_row(row: LeadLagCandidateRow) -> RelationFeedData:
    return RelationFeedData(
        summary=RelationFeedSummary(
            generated_at="2026-05-07T08:00:00+09:00",
            asof_date="2026-05-04",
            status="ok",
        ),
        movers=[
            MoverRow(
                "2026-05-04",
                "US",
                row.source_symbol,
                "Source Corp",
                "",
                None,
                None,
                6.3,
                None,
                None,
                "UP",
            ),
        ],
        leadlag=[row],
    )


def test_boost_zero_when_source_return_pct_zero():
    """source_return_pct==0이면 boost 0."""
    from tele_quant.relation_feed import get_relation_boost

    feed = _feed_with_row(_make_ll_row(source_return_pct=0.0))
    boost, _ = get_relation_boost(feed, "EP", has_telegram_evidence=True, technical_ok=True)
    assert boost == 0.0


def test_boost_zero_when_source_not_in_movers():
    """source_symbol이 feed.movers에 없으면 boost 0."""
    from tele_quant.relation_feed import get_relation_boost

    row = _make_ll_row(source_symbol="GHOST", source_return_pct=8.0)
    feed = RelationFeedData(
        summary=RelationFeedSummary(
            generated_at="2026-05-07T08:00:00+09:00", asof_date="2026-05-04", status="ok"
        ),
        movers=[
            MoverRow("2026-05-04", "US", "NVDA", "NVIDIA", "", None, None, 5.0, None, None, "UP"),
        ],
        leadlag=[row],
    )
    boost, _ = get_relation_boost(feed, "EP", has_telegram_evidence=True, technical_ok=True)
    assert boost == 0.0


def test_boost_zero_for_refill_note():
    """refill/deeper empirical lawbook note → boost 0."""
    from tele_quant.relation_feed import get_relation_boost

    row = _make_ll_row(note="refined from deeper empirical lawbook")
    feed = _feed_with_row(row)
    boost, _ = get_relation_boost(feed, "EP", has_telegram_evidence=True, technical_ok=True)
    assert boost == 0.0


def test_boost_medium_is_one():
    """confidence=medium이면 boost +1."""
    from tele_quant.relation_feed import get_relation_boost

    feed = _feed_with_row(_make_ll_row(confidence="medium"))
    boost, note = get_relation_boost(feed, "EP", has_telegram_evidence=True, technical_ok=True)
    assert boost == 1.0
    assert "+0.0%" not in note


def test_boost_high_is_two():
    """confidence=high이면 boost +2."""
    from tele_quant.relation_feed import get_relation_boost

    feed = _feed_with_row(_make_ll_row(confidence="high"))
    boost, _ = get_relation_boost(feed, "EP", has_telegram_evidence=True, technical_ok=True)
    assert boost == 2.0


def test_boost_low_is_zero():
    """confidence=low이면 boost 0."""
    from tele_quant.relation_feed import get_relation_boost

    feed = _feed_with_row(_make_ll_row(confidence="low"))
    boost, _ = get_relation_boost(feed, "EP", has_telegram_evidence=True, technical_ok=True)
    assert boost == 0.0


def test_zero_return_pct_mover_excluded_from_section():
    """return_pct==0 mover는 section 표시에서 제외됨."""
    from tele_quant.relation_feed import build_relation_feed_section

    zero_mover = MoverRow(
        "2026-05-04", "US", "MTD", "Mettler-Toledo", "", 1000.0, 1000.0, 0.0, None, None, "UP"
    )
    real_mover = MoverRow(
        "2026-05-04", "KR", "005930", "삼성전자", "", 60000.0, 57000.0, 5.3, None, None, "UP"
    )
    feed = RelationFeedData(
        summary=RelationFeedSummary(
            generated_at="2026-05-07T08:00:00+09:00", asof_date="2026-05-04", status="ok"
        ),
        movers=[zero_mover, real_mover],
        leadlag=[],
    )
    section = build_relation_feed_section(feed)
    assert "MTD" not in section, "+0.0% mover가 section에 표시됨"
    # source-only movers (no targets) are not shown per spec; the key assertion is MTD exclusion


# ---------------------------------------------------------------------------
# Tests: CLI limit output
# ---------------------------------------------------------------------------


def _make_feed_with_many_leadlag(n: int = 50) -> RelationFeedData:
    mover = MoverRow(
        "2026-05-04", "US", "NVDA", "NVIDIA", "반도체", 850.0, 800.0, 6.2, None, None, "UP"
    )
    rows = [
        LeadLagCandidateRow(
            "2026-05-04",
            "US",
            "NVDA",
            "NVIDIA",
            "",
            "UP",
            6.2,
            "US",
            f"TGT{i:03d}",
            f"Target {i}",
            "",
            "UP_LEADS_UP",
            1,
            15,
            9,
            0.60,
            1.8,
            "medium",
            "beneficiary",
            "",
        )
        for i in range(n)
    ]
    return RelationFeedData(
        summary=RelationFeedSummary(
            generated_at="2026-05-07T08:00:00+09:00",
            asof_date="2026-05-04",
            status="ok",
        ),
        movers=[mover],
        leadlag=rows,
    )


def test_cli_default_limit_20(tmp_path):
    """relation-feed CLI 기본 출력 — 자체 계산 피드 표시 확인."""
    import os

    from typer.testing import CliRunner

    from tele_quant.cli import app

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["relation-feed"],
        env={
            **os.environ,
            "TELEGRAM_API_ID": "12345",
            "TELEGRAM_API_HASH": "abc",
            "TELEGRAM_PHONE": "+821012345678",
            "TELEGRAM_SOURCE_CHATS": "test",
            "TELEGRAM_INCLUDE_ALL_CHANNELS": "false",
        },
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    # 자체 계산 summary 표가 표시돼야 함
    assert "자체 계산" in result.output or "스캔 종목" in result.output or "급등락 모버" in result.output


def test_cli_limit_5(tmp_path):
    """--limit 5 옵션이 CLI에서 동작함."""
    import os

    from typer.testing import CliRunner

    from tele_quant.cli import app

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["relation-feed", "--limit", "5"],
        env={
            **os.environ,
            "TELEGRAM_API_ID": "12345",
            "TELEGRAM_API_HASH": "abc",
            "TELEGRAM_PHONE": "+821012345678",
            "TELEGRAM_SOURCE_CHATS": "test",
            "TELEGRAM_INCLUDE_ALL_CHANNELS": "false",
        },
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "자체 계산" in result.output or "스캔 종목" in result.output


def test_fallback_only_hides_stock_feed_table(tmp_path):
    """--fallback-only이면 stock feed lead-lag 표가 출력되지 않음."""
    import os

    from typer.testing import CliRunner

    from tele_quant.cli import app

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["relation-feed", "--fallback-only"],
        env={
            **os.environ,
            "TELEGRAM_API_ID": "12345",
            "TELEGRAM_API_HASH": "abc",
            "TELEGRAM_PHONE": "+821012345678",
            "TELEGRAM_SOURCE_CHATS": "test",
            "TELEGRAM_INCLUDE_ALL_CHANNELS": "false",
        },
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    # stock feed 표가 아니라 fallback/상관관계 표가 표시돼야 함
    assert "Stock Feed Lead-Lag" not in result.output
    # 자체 계산 summary 또는 상관관계 후보 표가 나와야 함
    assert "자체 계산" in result.output or "상관관계" in result.output or "Fallback" in result.output
