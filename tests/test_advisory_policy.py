"""tests/test_advisory_policy.py — AdvisoryPolicy 테스트."""

from __future__ import annotations

from unittest.mock import MagicMock

from tele_quant.advisory_policy import (
    DISCLAIMER,
    AdvisoryItem,
    AdvisorySeverity,
    classify_severity,
    dedupe_items,
    filter_4h_items,
    filter_urgent_items,
    format_advisory_item,
    should_include_in_4h,
    should_send_immediately,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _settings(
    advisory_only_mode: bool = True,
    urgent_alert_min_score: float = 90.0,
    advisory_min_score: float = 70.0,
) -> MagicMock:
    s = MagicMock()
    s.advisory_only_mode = advisory_only_mode
    s.urgent_alert_min_score = urgent_alert_min_score
    s.advisory_min_score = advisory_min_score
    return s


def _make_item(
    score: float = 75.0,
    direct_evidence: bool = False,
    title: str = "테스트 종목",
    symbol: str = "005930.KS",
) -> AdvisoryItem:
    return AdvisoryItem(
        source="test",
        market="KR",
        symbol=symbol,
        title=title,
        severity=classify_severity(score, direct_evidence),
        score=score,
        reason="테스트 근거",
        action="진입 검토 후보",
        dedupe_key=AdvisoryItem.make_dedupe_key("test", symbol, title),
        direct_evidence=direct_evidence,
    )


# ── should_send_immediately ────────────────────────────────────────────────────

class TestShouldSendImmediately:
    def test_urgent_with_evidence_sends(self) -> None:
        """score >= 90 + direct_evidence → 즉시 발송"""
        item = _make_item(score=92.0, direct_evidence=True)
        assert should_send_immediately(item, _settings()) is True

    def test_high_score_no_evidence_does_not_send(self) -> None:
        """score >= 90이어도 direct_evidence 없으면 즉시 발송 안 함"""
        item = _make_item(score=91.0, direct_evidence=False)
        assert should_send_immediately(item, _settings()) is False

    def test_low_score_with_evidence_does_not_send(self) -> None:
        """direct_evidence 있어도 score < 90이면 즉시 발송 안 함"""
        item = _make_item(score=85.0, direct_evidence=True)
        assert should_send_immediately(item, _settings()) is False

    def test_advisory_only_mode_false_ignores_evidence(self) -> None:
        """advisory_only_mode=False이면 URGENT 심각도만 체크"""
        item = _make_item(score=91.0, direct_evidence=False)
        item.severity = AdvisorySeverity.URGENT
        # advisory_only_mode=False → 심각도 URGENT이면 즉시 발송
        assert should_send_immediately(item, _settings(advisory_only_mode=False)) is True

    def test_custom_threshold(self) -> None:
        """urgent_alert_min_score 커스텀 설정"""
        item = _make_item(score=95.0, direct_evidence=True)
        # threshold=96 이면 95점은 통과 못함
        assert should_send_immediately(item, _settings(urgent_alert_min_score=96.0)) is False
        # threshold=94 이면 95점은 통과
        assert should_send_immediately(item, _settings(urgent_alert_min_score=94.0)) is True


# ── should_include_in_4h ─────────────────────────────────────────────────────

class TestShouldIncludeIn4h:
    def test_score_above_min_included(self) -> None:
        """score >= 70 → 4H 브리핑 포함"""
        item = _make_item(score=75.0)
        assert should_include_in_4h(item, _settings()) is True

    def test_score_below_min_excluded(self) -> None:
        """score < 70 → 4H 브리핑 미포함"""
        item = _make_item(score=65.0)
        assert should_include_in_4h(item, _settings()) is False

    def test_urgent_item_excluded_from_4h(self) -> None:
        """즉시 발송 대상(URGENT)은 4H 브리핑 미포함 (이미 즉시 발송됨)"""
        item = _make_item(score=92.0, direct_evidence=True)
        assert should_include_in_4h(item, _settings()) is False

    def test_boundary_score_70(self) -> None:
        """score = 70 → 포함"""
        item = _make_item(score=70.0)
        assert should_include_in_4h(item, _settings()) is True

    def test_boundary_score_69(self) -> None:
        """score = 69.9 → 미포함"""
        item = _make_item(score=69.9)
        assert should_include_in_4h(item, _settings()) is False


# ── classify_severity ─────────────────────────────────────────────────────────

class TestClassifySeverity:
    def test_urgent(self) -> None:
        assert classify_severity(90.0, direct_evidence=True) == AdvisorySeverity.URGENT

    def test_action(self) -> None:
        assert classify_severity(82.0) == AdvisorySeverity.ACTION

    def test_watch(self) -> None:
        assert classify_severity(72.0) == AdvisorySeverity.WATCH

    def test_info(self) -> None:
        assert classify_severity(60.0) == AdvisorySeverity.INFO

    def test_90_without_evidence_is_action(self) -> None:
        """score=90이어도 direct_evidence 없으면 ACTION"""
        assert classify_severity(90.0, direct_evidence=False) == AdvisorySeverity.ACTION


# ── dedupe_items ──────────────────────────────────────────────────────────────

class TestDedupeItems:
    def test_keeps_highest_score(self) -> None:
        """같은 dedupe_key는 score 높은 것만 유지"""
        key = "abc123"
        low = _make_item(score=72.0)
        low.dedupe_key = key
        high = _make_item(score=85.0)
        high.dedupe_key = key

        result = dedupe_items([low, high])
        assert len(result) == 1
        assert result[0].score == 85.0

    def test_different_keys_kept(self) -> None:
        """다른 dedupe_key는 모두 유지"""
        a = _make_item(score=75.0)
        b = _make_item(score=80.0, symbol="000660.KS")
        result = dedupe_items([a, b])
        assert len(result) == 2


# ── filter_4h_items ───────────────────────────────────────────────────────────

class TestFilter4hItems:
    def test_filters_below_min_score(self) -> None:
        items = [_make_item(score=s) for s in [60.0, 70.0, 80.0, 95.0]]
        # 95점 + direct_evidence=False → 즉시 발송 안 됨, 4H 포함
        # 70점 이상 모두 포함
        result = filter_4h_items(items, _settings())
        scores = [it.score for it in result]
        assert 60.0 not in scores
        assert 70.0 in scores
        assert 80.0 in scores

    def test_sorted_by_score_desc(self) -> None:
        items = [_make_item(score=s) for s in [70.0, 85.0, 75.0]]
        result = filter_4h_items(items, _settings())
        assert result[0].score == 85.0


# ── advisory_item validation ──────────────────────────────────────────────────

class TestAdvisoryItemValidation:
    def test_forbidden_phrase_in_title_replaced(self) -> None:
        """금지 표현이 제목에 있으면 자동 치환됨"""
        item = _make_item(title="매수 권장 삼성전자")
        assert "매수 권장" not in item.title
        assert "[리서치 보조]" in item.title

    def test_normal_title_untouched(self) -> None:
        """정상 제목은 변경 없음"""
        item = _make_item(title="삼성전자 진입 검토 후보")
        assert item.title == "삼성전자 진입 검토 후보"

    def test_dedupe_key_generation(self) -> None:
        """dedupe_key는 결정론적으로 생성됨"""
        k1 = AdvisoryItem.make_dedupe_key("surge", "005930.KS", "삼성전자 급등")
        k2 = AdvisoryItem.make_dedupe_key("surge", "005930.KS", "삼성전자 급등")
        assert k1 == k2

    def test_dedupe_key_different_for_different_inputs(self) -> None:
        k1 = AdvisoryItem.make_dedupe_key("surge", "005930.KS", "삼성전자 급등")
        k2 = AdvisoryItem.make_dedupe_key("surge", "000660.KS", "SK하이닉스 급등")
        assert k1 != k2


# ── format_advisory_item ──────────────────────────────────────────────────────

class TestFormatAdvisoryItem:
    def test_includes_score(self) -> None:
        item = _make_item(score=78.0)
        text = format_advisory_item(item, index=1)
        assert "78점" in text

    def test_includes_symbol(self) -> None:
        item = _make_item(symbol="005930.KS")
        text = format_advisory_item(item)
        assert "005930" in text  # .KS 제거됨

    def test_includes_reason(self) -> None:
        item = _make_item()
        text = format_advisory_item(item)
        assert "테스트 근거" in text

    def test_chasing_risk_flag(self) -> None:
        item = _make_item()
        item.chasing_risk = True
        text = format_advisory_item(item)
        assert "추격주의" in text


# ── disclaimer ────────────────────────────────────────────────────────────────

def test_disclaimer_contains_required_phrase() -> None:
    """면책 문구에 '공개 정보 기반 리서치 보조' 포함 필수"""
    assert "공개 정보 기반 리서치 보조" in DISCLAIMER
    assert "투자 판단 책임" in DISCLAIMER


# ── filter_urgent_items ───────────────────────────────────────────────────────

def test_filter_urgent_items_only_high_score_with_evidence() -> None:
    items = [
        _make_item(score=92.0, direct_evidence=True),
        _make_item(score=85.0, direct_evidence=False),
        _make_item(score=70.0, direct_evidence=True),
    ]
    result = filter_urgent_items(items, _settings())
    assert len(result) == 1
    assert result[0].score == 92.0
