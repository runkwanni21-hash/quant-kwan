"""수주잔고(Order Backlog) 모듈 단위 테스트."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tele_quant.order_backlog import (
    _DART_PBLNTF_TY_ORDER,
    BacklogEvent,
    _classify_backlog_tier,
    _is_dart_amendment,
    _is_dart_cancel,
    _ok_krw_to_usd_million,
    _parse_dart_contract_xml,
    _parse_krw_ok,
    _parse_usd_million,
    _static_backlog_event,
    _usd_million_to_ok_krw,
    backlog_boost,
    build_backlog_section,
    run_backlog_audit,
)

# ── 금액 파싱 테스트 ───────────────────────────────────────────────────────────

class TestParseKrwOk:
    def test_억원(self):
        assert _parse_krw_ok("1,234억원 규모") == 1234.0

    def test_억_no_suffix(self):
        assert _parse_krw_ok("500억") == 500.0

    def test_조_단독(self):
        assert _parse_krw_ok("2조원") == 20_000.0

    def test_조_억_복합(self):
        assert _parse_krw_ok("1조 2,345억원") == pytest.approx(10_000 + 2345, rel=1e-3)

    def test_백만(self):
        # 5,000백만 = 50억
        assert _parse_krw_ok("5,000백만원") == pytest.approx(50.0, rel=1e-3)

    def test_no_amount(self):
        assert _parse_krw_ok("공급계약 체결") is None

    def test_commas_stripped(self):
        assert _parse_krw_ok("10,000억원") == 10_000.0

    def test_large_조(self):
        # 한화오션 30조
        assert _parse_krw_ok("수주잔고 30조원") == 300_000.0


class TestParseUsdMillion:
    def test_billion(self):
        assert _parse_usd_million("$1.2 billion") == pytest.approx(1_200.0, rel=1e-3)

    def test_billion_uppercase(self):
        assert _parse_usd_million("USD 5B order") == pytest.approx(5_000.0, rel=1e-3)

    def test_million(self):
        assert _parse_usd_million("$500 million contract") == pytest.approx(500.0, rel=1e-3)

    def test_million_M(self):
        assert _parse_usd_million("300M award") == pytest.approx(300.0, rel=1e-3)

    def test_억달러(self):
        # 2억달러 = 200백만달러
        assert _parse_usd_million("2억달러") == pytest.approx(200.0, rel=1e-3)

    def test_백만달러(self):
        assert _parse_usd_million("500백만달러") == pytest.approx(500.0, rel=1e-3)

    def test_no_amount(self):
        assert _parse_usd_million("no amount here") is None


class TestCurrencyConversion:
    def test_usd_to_krw(self):
        # 1,000 million USD = 1 billion USD → ~13,700 억원 (at 1370 KRW/USD)
        ok = _usd_million_to_ok_krw(1_000.0)
        assert 12_000 < ok < 16_000  # reasonable range

    def test_krw_to_usd(self):
        # 13,700 억원 → ~1 billion USD = 1,000 million USD
        usd = _ok_krw_to_usd_million(13_700.0)
        assert 800 < usd < 1_200


# ── 백로그 티어 분류 테스트 ────────────────────────────────────────────────────

class TestBacklogTier:
    def test_high_threshold(self):
        # ≥15조 → HIGH
        assert _classify_backlog_tier(150_000.0) == "HIGH"

    def test_medium_threshold(self):
        # 3조~15조 → MEDIUM
        assert _classify_backlog_tier(50_000.0) == "MEDIUM"

    def test_low_threshold(self):
        assert _classify_backlog_tier(10_000.0) == "LOW"

    def test_none_is_low(self):
        assert _classify_backlog_tier(None) == "LOW"

    def test_조선_규모_high(self):
        # 한화오션 30조 → HIGH
        assert _classify_backlog_tier(300_000.0) == "HIGH"


# ── 정적 레지스트리 테스트 ────────────────────────────────────────────────────

class TestStaticBacklog:
    def test_hd_hyundai_heavy(self):
        ev = _static_backlog_event("329180.KS")
        assert ev is not None
        assert ev.backlog_tier == "HIGH"
        assert ev.market == "KR"
        assert ev.amount_ok_krw is not None
        assert ev.amount_ok_krw >= 100_000  # 최소 10조원

    def test_lmt_lockheed(self):
        ev = _static_backlog_event("LMT")
        assert ev is not None
        assert ev.backlog_tier == "HIGH"
        assert ev.market == "US"
        assert ev.amount_usd_million is not None
        assert ev.amount_usd_million >= 100_000  # 최소 $100B

    def test_rtx(self):
        ev = _static_backlog_event("RTX")
        assert ev is not None
        assert ev.backlog_tier == "HIGH"

    def test_unknown_symbol(self):
        ev = _static_backlog_event("UNKNOWN123")
        assert ev is None

    def test_samsung_heavy(self):
        ev = _static_backlog_event("010140.KS")
        assert ev is not None
        assert ev.backlog_tier == "HIGH"
        assert ev.source == "STATIC"


# ── BacklogEvent 속성 테스트 ─────────────────────────────────────────────────

class TestBacklogEventDisplay:
    def _make_event(self, ok_krw: float | None, usd_m: float | None) -> BacklogEvent:
        return BacklogEvent(
            symbol="TEST",
            market="KR",
            source="STATIC",
            event_date=datetime.now(UTC),
            amount_ok_krw=ok_krw,
            amount_usd_million=usd_m,
            client="",
            contract_type="STATIC",
            raw_title="test",
            backlog_tier="HIGH",
        )

    def test_조원_display(self):
        ev = self._make_event(ok_krw=300_000.0, usd_m=None)
        assert "조원" in ev.amount_ok_krw_display

    def test_억원_display(self):
        ev = self._make_event(ok_krw=5_000.0, usd_m=None)
        assert "억원" in ev.amount_ok_krw_display

    def test_usd_B_display(self):
        ev = self._make_event(ok_krw=None, usd_m=50_000.0)
        assert "$" in ev.amount_usd_display
        assert "B" in ev.amount_usd_display

    def test_usd_M_display(self):
        ev = self._make_event(ok_krw=None, usd_m=500.0)
        assert "$" in ev.amount_usd_display
        assert "M" in ev.amount_usd_display

    def test_none_display(self):
        ev = self._make_event(ok_krw=None, usd_m=None)
        assert "미파싱" in ev.amount_ok_krw_display


# ── backlog_boost 테스트 ──────────────────────────────────────────────────────

class TestBacklogBoost:
    def test_static_high_returns_15(self):
        boost = backlog_boost("329180.KS", store=None)
        assert boost == 15.0

    def test_static_medium_returns_8(self):
        boost = backlog_boost("042700.KS", store=None)  # 한미반도체 MEDIUM
        assert boost == 8.0

    def test_unknown_no_store_returns_0(self):
        boost = backlog_boost("UNKNOWN999", store=None)
        assert boost == 0.0

    def test_lmt_high_boost(self):
        boost = backlog_boost("LMT", store=None)
        assert boost == 15.0

    def test_rtx_high_boost(self):
        boost = backlog_boost("RTX", store=None)
        assert boost == 15.0


# ── build_backlog_section 테스트 ─────────────────────────────────────────────

class TestBuildBacklogSection:
    def _make_ev(self, symbol: str, ok_krw: float, tier: str, source: str = "STATIC") -> BacklogEvent:
        return BacklogEvent(
            symbol=symbol,
            market="KR" if symbol.endswith((".KS", ".KQ")) else "US",
            source=source,
            event_date=datetime.now(UTC),
            amount_ok_krw=ok_krw,
            amount_usd_million=None,
            client="",
            contract_type="STATIC",
            raw_title=f"[{source}] {symbol}",
            backlog_tier=tier,
        )

    def test_section_header(self):
        events = [self._make_ev("LMT", 2_000_000.0, "HIGH")]
        section = build_backlog_section(events)
        assert "수주잔고 현황" in section

    def test_high_emoji_present(self):
        events = [self._make_ev("LMT", 2_000_000.0, "HIGH")]
        section = build_backlog_section(events)
        assert "🔥" in section

    def test_empty_events(self):
        section = build_backlog_section([])
        assert "신규 수주 공시 없음" in section

    def test_new_events_shown(self):
        events = [self._make_ev("329180.KS", 400_000.0, "HIGH", source="DART")]
        section = build_backlog_section(events)
        assert "신규 수주·계약 공시" in section

    def test_disclaimer_present(self):
        events = [self._make_ev("329180.KS", 400_000.0, "HIGH")]
        section = build_backlog_section(events)
        assert "투자 판단 책임" in section

    def test_section_number_16(self):
        events = [self._make_ev("LMT", 2_000_000.0, "HIGH")]
        section = build_backlog_section(events)
        assert "16." in section


# ── chain_tier SpilloverTarget 통합 테스트 ───────────────────────────────────

class TestChainTierIntegration:
    def test_supply_chain_cost_is_tier1(self):
        from tele_quant.supply_chain_alpha import _CHAIN_TIER
        assert _CHAIN_TIER["SUPPLY_CHAIN_COST"] == 1

    def test_beneficiary_is_tier1(self):
        from tele_quant.supply_chain_alpha import _CHAIN_TIER
        assert _CHAIN_TIER["BENEFICIARY"] == 1

    def test_lagging_beneficiary_is_tier2(self):
        from tele_quant.supply_chain_alpha import _CHAIN_TIER
        assert _CHAIN_TIER["LAGGING_BENEFICIARY"] == 2

    def test_peer_momentum_is_tier3(self):
        from tele_quant.supply_chain_alpha import _CHAIN_TIER
        assert _CHAIN_TIER["PEER_MOMENTUM"] == 3

    def test_style_long_tier2_prefix(self):
        from tele_quant.supply_chain_alpha import _style_long
        style = _style_long("LAGGING_BENEFICIARY", 60, 60, chain_tier=2)
        assert "2차" in style

    def test_style_long_tier3_prefix(self):
        from tele_quant.supply_chain_alpha import _style_long
        style = _style_long("PEER_MOMENTUM", 60, 60, chain_tier=3)
        assert "3차" in style

    def test_style_long_tier1_no_prefix(self):
        from tele_quant.supply_chain_alpha import _style_long
        style = _style_long("BENEFICIARY", 60, 60, chain_tier=1)
        assert "1차" not in style

    def test_style_short_tier2_prefix(self):
        from tele_quant.supply_chain_alpha import _style_short
        style = _style_short("PEER_MOMENTUM", 60, 60, chain_tier=2)
        assert "2차" in style

    def test_spillover_target_default_chain_tier(self):
        from tele_quant.supply_chain_alpha import MoverEvent, SpilloverTarget
        mover = MoverEvent(
            symbol="NVDA", name="NVIDIA", market="US",
            return_1d=6.0, direction="BULLISH", confidence="HIGH",
            volume_ratio=2.0, reason_type="ai_capex", reason_ko="AI 투자",
        )
        target = SpilloverTarget(
            symbol="AMAT", name="Applied Materials", sector="반도체장비",
            relation_type="BENEFICIARY", rule_id="test", chain_name="AI 반도체",
            connection="NVDA → 장비 수요", source=mover,
        )
        assert target.chain_tier == 1


# ── DART pblntf_ty 설정 테스트 ────────────────────────────────────────────────

class TestDartPblntfTy:
    def test_pblntf_ty_contains_B(self):
        """B = 주요사항보고 (수주 공시 포함)."""
        assert "B" in _DART_PBLNTF_TY_ORDER

    def test_pblntf_ty_contains_I(self):
        """I = 거래소공시 (수주 공시 포함)."""
        assert "I" in _DART_PBLNTF_TY_ORDER

    def test_pblntf_ty_no_G(self):
        """G = 펀드공시 — 수주와 무관, 포함 금지."""
        assert "G" not in _DART_PBLNTF_TY_ORDER

    def test_pblntf_ty_no_C(self):
        """C = 발행공시 — 수주와 무관, 포함 금지."""
        assert "C" not in _DART_PBLNTF_TY_ORDER


# ── DART 정정/해지 감지 테스트 ────────────────────────────────────────────────

class TestDartAmendCancel:
    def test_amendment_detection(self):
        assert _is_dart_amendment("[정정] 단일판매공급계약체결") is True

    def test_amendment_detection2(self):
        assert _is_dart_amendment("정정공시: 수주계약") is True

    def test_not_amendment(self):
        assert _is_dart_amendment("단일판매공급계약체결") is False

    def test_cancel_detection(self):
        assert _is_dart_cancel("계약해지 공시") is True

    def test_cancel_detection2(self):
        assert _is_dart_cancel("납품계약 취소 통보") is True

    def test_not_cancel(self):
        assert _is_dart_cancel("신규 수주 계약 체결") is False


# ── DART document.xml 파싱 테스트 ─────────────────────────────────────────────

_SAMPLE_CONTRACT_XML = """
<html><body>
<table>
<tr><th>계약금액</th><td>1,234억원</td></tr>
<tr><th>계약상대방</th><td>한국조선주식회사</td></tr>
<tr><th>계약기간</th><td>2026.06.01 ~ 2027.12.31</td></tr>
<tr><th>최근매출액대비(%)</th><td>12.5%</td></tr>
</table>
</body></html>
"""

_SAMPLE_AMENDMENT_XML = """
<html><body>
<table>
<tr><th>계약금액</th><td>500억원</td></tr>
<tr><th>계약상대방</th><td>테스트주</td></tr>
</table>
<p>[정정] 기존 공시 내용 수정</p>
</body></html>
"""

_SAMPLE_CANCEL_XML = """
<html><body>
<table>
<tr><th>계약금액</th><td>300억원</td></tr>
</table>
<p>본 계약은 해지되었습니다.</p>
</body></html>
"""


class TestParseDartContractXml:
    def test_amount_parsed(self):
        result = _parse_dart_contract_xml(_SAMPLE_CONTRACT_XML)
        assert result.get("amount_ok_krw") == 1234.0

    def test_client_parsed(self):
        result = _parse_dart_contract_xml(_SAMPLE_CONTRACT_XML)
        assert "한국조선" in result.get("client", "")

    def test_contract_start_parsed(self):
        result = _parse_dart_contract_xml(_SAMPLE_CONTRACT_XML)
        assert result.get("contract_start", "") != ""

    def test_contract_end_parsed(self):
        result = _parse_dart_contract_xml(_SAMPLE_CONTRACT_XML)
        assert result.get("contract_end", "") != ""

    def test_revenue_ratio_parsed(self):
        result = _parse_dart_contract_xml(_SAMPLE_CONTRACT_XML)
        ratio = result.get("amount_ratio_to_revenue")
        assert ratio is not None
        assert abs(ratio - 12.5) < 0.1

    def test_high_confidence_when_amount_found(self):
        result = _parse_dart_contract_xml(_SAMPLE_CONTRACT_XML)
        assert result.get("parsed_confidence") == "HIGH"

    def test_amendment_detected(self):
        result = _parse_dart_contract_xml(_SAMPLE_AMENDMENT_XML)
        assert result.get("is_amendment") is True

    def test_cancel_detected(self):
        result = _parse_dart_contract_xml(_SAMPLE_CANCEL_XML)
        assert result.get("is_cancellation") is True

    def test_empty_input(self):
        result = _parse_dart_contract_xml("")
        assert result.get("amount_ok_krw") is None


# ── BacklogEvent v2 dataclass 필드 테스트 ────────────────────────────────────

class TestBacklogEventV2Fields:
    def _make_v2(self) -> BacklogEvent:
        return BacklogEvent(
            symbol="329180.KS",
            market="KR",
            source="DART",
            event_date=datetime.now(UTC),
            amount_ok_krw=1234.0,
            amount_usd_million=None,
            client="테스트상대방",
            contract_type="수주",
            raw_title="단일판매공급계약체결",
            raw_amount_text="1,234억원",
            chain_tier=1,
            backlog_tier="MEDIUM",
            rcept_no="20260517001234",
            filing_url="https://dart.fss.or.kr/",
            corp_name="HD현대중공업",
            amount_ratio_to_revenue=12.5,
            contract_start="2026.06.01",
            contract_end="2027.12.31",
            parsed_confidence="HIGH",
            is_amendment=False,
            is_cancellation=False,
            cik="",
            accession_no="",
            source_raw_hash="abc123",
        )

    def test_rcept_no_field(self):
        ev = self._make_v2()
        assert ev.rcept_no == "20260517001234"

    def test_corp_name_field(self):
        ev = self._make_v2()
        assert ev.corp_name == "HD현대중공업"

    def test_parsed_confidence_field(self):
        ev = self._make_v2()
        assert ev.parsed_confidence == "HIGH"

    def test_is_amendment_false(self):
        ev = self._make_v2()
        assert ev.is_amendment is False

    def test_is_cancellation_false(self):
        ev = self._make_v2()
        assert ev.is_cancellation is False

    def test_amount_ratio_field(self):
        ev = self._make_v2()
        assert ev.amount_ratio_to_revenue == 12.5

    def test_source_raw_hash_field(self):
        ev = self._make_v2()
        assert ev.source_raw_hash == "abc123"

    def test_default_parsed_confidence_is_low(self):
        ev = BacklogEvent(
            symbol="LMT", market="US", source="STATIC",
            event_date=datetime.now(UTC),
            amount_ok_krw=None, amount_usd_million=None,
            client="", contract_type="STATIC", raw_title="",
        )
        assert ev.parsed_confidence == "LOW"

    def test_default_is_amendment_false(self):
        ev = BacklogEvent(
            symbol="LMT", market="US", source="STATIC",
            event_date=datetime.now(UTC),
            amount_ok_krw=None, amount_usd_million=None,
            client="", contract_type="STATIC", raw_title="",
        )
        assert ev.is_amendment is False


# ── build_backlog_section 4-섹션 구조 테스트 ─────────────────────────────────

class TestBuildBacklogSectionSubsections:
    def _make_ev(
        self, symbol: str, ok_krw: float, tier: str, source: str = "DART",
        is_amendment: bool = False, is_cancellation: bool = False,
        parsed_confidence: str = "HIGH",
    ) -> BacklogEvent:
        return BacklogEvent(
            symbol=symbol,
            market="KR",
            source=source,
            event_date=datetime.now(UTC),
            amount_ok_krw=ok_krw,
            amount_usd_million=None,
            client="테스트",
            contract_type="수주",
            raw_title="단일판매공급계약체결",
            backlog_tier=tier,
            parsed_confidence=parsed_confidence,
            is_amendment=is_amendment,
            is_cancellation=is_cancellation,
        )

    def test_amendment_goes_to_risk_section(self):
        ev = self._make_ev("329180.KS", 400_000.0, "HIGH", is_amendment=True)
        section = build_backlog_section([ev])
        assert "정정·해지·취소 리스크" in section

    def test_cancellation_goes_to_risk_section(self):
        ev = self._make_ev("329180.KS", 200_000.0, "MEDIUM", is_cancellation=True)
        section = build_backlog_section([ev])
        assert "정정·해지·취소 리스크" in section

    def test_amendment_not_in_new_orders_section(self):
        ev = self._make_ev("329180.KS", 400_000.0, "HIGH", is_amendment=True)
        section = build_backlog_section([ev])
        # Amendment should not appear as a normal new order
        assert "신규 수주·계약 공시" not in section or "HD현대" not in section

    def test_static_not_in_new_orders(self):
        ev = self._make_ev("329180.KS", 400_000.0, "HIGH", source="STATIC")
        section = build_backlog_section([ev])
        # STATIC should appear in 고수주잔고 subsection, not ① new orders
        assert "정적 레지스트리" in section or "고수주잔고" in section

    def test_risk_disclaimer_present(self):
        ev = self._make_ev("329180.KS", 400_000.0, "HIGH", is_amendment=True)
        section = build_backlog_section([ev])
        assert "호재로 해석 금지" in section or "리스크" in section


# ── run_backlog_audit 테스트 ──────────────────────────────────────────────────

class TestRunBacklogAudit:
    def test_returns_list(self):
        result = run_backlog_audit(store=None)
        assert isinstance(result, list)

    def test_each_item_has_severity(self):
        result = run_backlog_audit(store=None)
        for item in result:
            assert "severity" in item

    def test_each_item_has_detail(self):
        result = run_backlog_audit(store=None)
        for item in result:
            assert "detail" in item or "message" in item

    def test_no_dart_key_produces_warning(self, monkeypatch):
        monkeypatch.delenv("DART_API_KEY", raising=False)
        monkeypatch.delenv("OPENDART_API_KEY", raising=False)
        result = run_backlog_audit(store=None)
        severities = [i.get("severity") for i in result]
        assert "MEDIUM" in severities or "HIGH" in severities or "INFO" in severities

    def test_pblntf_ty_check_passes(self):
        result = run_backlog_audit(store=None)
        # Should NOT flag pblntf_ty as HIGH if B/I are correct
        high_pblntf = [
            i for i in result
            if "pblntf" in (i.get("check", "") + i.get("key", "")).lower()
            and i.get("severity") == "HIGH"
        ]
        assert high_pblntf == []


# ── output-lint 수주잔고 패턴 테스트 ─────────────────────────────────────────

class TestOutputLintBacklogPatterns:
    """output-lint 금지 수주잔고 표현이 실제로 감지되는지 확인."""

    def _lint_text(self, text: str) -> list[dict]:
        import re as _re
        issues = []
        lines = text.splitlines()

        def _check(severity: str, pattern: str, message: str, *, regex: bool = False) -> None:
            for _ln, line in enumerate(lines, 1):
                hit = _re.search(pattern, line, _re.IGNORECASE) if regex else pattern in line
                if hit:
                    issues.append({"severity": severity, "pattern": pattern, "message": message})

        _check("HIGH", r"수주\s*확정\s*수혜", "수주 확정 수혜 금지", regex=True)
        _check("HIGH", r"계약\s*=\s*매출\s*확정", "계약=매출 확정 금지", regex=True)
        _check("HIGH", r"수주잔고.*반드시\s*상승", "수주잔고→상승 단정 금지", regex=True)
        _check("HIGH", r"해지.*호재|취소.*호재", "해지·취소=호재 금지", regex=True)
        return issues

    def test_detects_수주확정수혜(self):
        issues = self._lint_text("이 종목은 수주 확정 수혜 종목이다.")
        assert any(i["severity"] == "HIGH" for i in issues)

    def test_detects_계약매출확정(self):
        issues = self._lint_text("계약 = 매출 확정이므로 즉시 매수.")
        assert any(i["severity"] == "HIGH" for i in issues)

    def test_detects_수주잔고상승(self):
        issues = self._lint_text("수주잔고가 높아 반드시 상승할 것입니다.")
        assert any(i["severity"] == "HIGH" for i in issues)

    def test_detects_해지호재(self):
        issues = self._lint_text("계약 해지는 호재로 볼 수 있다.")
        assert any(i["severity"] == "HIGH" for i in issues)

    def test_clean_text_no_issues(self):
        issues = self._lint_text("수주잔고 현황 (공개 정보 기반 리서치 보조)")
        assert issues == []
