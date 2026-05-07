"""헤드라인 품질 개선 테스트: 제목 접두어·출처 꼬리·브로커 접두어 정제."""

from __future__ import annotations

import pytest

from tele_quant.headline_cleaner import clean_source_header

# ── 제목 접두어 제거 ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,expected_contains,expected_absent",
    [
        (
            "제목 : 온 세미, AI·데이터센터 성장 기대 - 로스 외 *연합인포맥스*",
            ["온 세미", "AI", "데이터센터"],
            ["제목 :", "제목:", "연합인포맥스", "로스 외"],
        ),
        (
            "제목: SK하이닉스 HBM 수요 확대",
            ["SK하이닉스", "HBM"],
            ["제목:"],
        ),
        (
            "제목 :팔란티어 실적 상회",
            ["팔란티어", "실적 상회"],
            ["제목"],
        ),
    ],
)
def test_title_prefix_removed(raw, expected_contains, expected_absent):
    result = clean_source_header(raw)
    for token in expected_contains:
        assert token in result, f"expected '{token}' in result: {result!r}"
    for token in expected_absent:
        assert token not in result, f"expected '{token}' absent in result: {result!r}"


# ── 출처 꼬리 제거 ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,expected_contains,expected_absent",
    [
        (
            "온 세미, AI·데이터센터 성장 기대 - 로스 외 *연합인포맥스*",
            ["온 세미", "AI"],
            ["연합인포맥스", "로스 외"],
        ),
        (
            "NVDA 호재 - Reuters",
            ["NVDA", "호재"],
            ["Reuters"],
        ),
        (
            "삼성전자 실적 *연합인포맥스*",
            ["삼성전자", "실적"],
            ["연합인포맥스"],
        ),
        (
            "SK하이닉스 수요 확대 - Bloomberg",
            ["SK하이닉스", "수요"],
            ["Bloomberg"],
        ),
    ],
)
def test_source_suffix_removed(raw, expected_contains, expected_absent):
    result = clean_source_header(raw)
    for token in expected_contains:
        assert token in result, f"expected '{token}' in result: {result!r}"
    for token in expected_absent:
        assert token not in result, f"expected '{token}' absent in result: {result!r}"


# ── 브로커 접두어 제거 및 포맷 변환 ───────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,expected_contains,expected_absent",
    [
        (
            "모건스탠리) Nebius 1Q26 실적 프리뷰. 중립 / 목표가 $126",
            ["Nebius", "실적 프리뷰"],
            ["모건스탠리"],
        ),
        (
            "JP모건) 이튼(ETN) 실적 첫 인상",
            ["이튼(ETN)", "실적"],
            ["JP모건"],
        ),
        (
            "Citi) 팔란티어; 보다 광범위한 어닝 서프라이즈",
            ["팔란티어", "어닝 서프라이즈"],
            ["Citi"],
        ),
        (
            "Wedbush) AMD; 강한 CPU 수요",
            ["AMD", "CPU 수요"],
            ["Wedbush"],
        ),
        (
            "Goldman Sachs) NVDA 목표가 상향",
            ["NVDA", "목표가"],
            ["Goldman"],
        ),
        (
            "BofA) MSFT 클라우드 성장 견조",
            ["MSFT", "클라우드"],
            ["BofA"],
        ),
    ],
)
def test_broker_paren_prefix_removed(raw, expected_contains, expected_absent):
    result = clean_source_header(raw)
    for token in expected_contains:
        assert token in result, f"expected '{token}' in result: {result!r}"
    for token in expected_absent:
        assert token not in result, f"expected '{token}' absent in result: {result!r}"


def test_semicolon_becomes_colon():
    """'Citi) 팔란티어; 서프라이즈' → 'Subject: rest' (세미콜론 → 콜론)."""
    result = clean_source_header("Citi) 팔란티어; 어닝 서프라이즈")
    assert "팔란티어:" in result or ("팔란티어" in result and "어닝 서프라이즈" in result)
    assert "Citi" not in result


def test_english_subject_colon_inserted():
    """'Wedbush) Nebius 실적 프리뷰' → 'Nebius: 실적 프리뷰' (영문 대문자 주어 → 콜론)."""
    result = clean_source_header("Wedbush) Nebius 실적 프리뷰")
    assert "Nebius:" in result or "Nebius" in result
    assert "Wedbush" not in result


def test_korean_ticker_subject_colon_inserted():
    """'JP모건) 이튼(ETN) 실적' → 'subject 포함, 브로커 제거'."""
    result = clean_source_header("JP모건) 이튼(ETN) 실적 추정")
    assert "이튼(ETN)" in result
    assert "JP모건" not in result


# ── 복합 케이스 ────────────────────────────────────────────────────────────────


def test_title_prefix_plus_source_suffix_combined():
    """제목 접두어 + 출처 꼬리 동시 제거."""
    raw = "제목 : 온 세미, AI·데이터센터 성장 기대 - 로스 외 *연합인포맥스*"
    result = clean_source_header(raw)
    assert "제목" not in result
    assert "연합인포맥스" not in result
    assert "온 세미" in result
    assert "데이터센터" in result


def test_clean_content_unchanged():
    """클린한 헤드라인은 변경되지 않아야 한다."""
    raw = "NVDA 호재: 데이터센터 수요 확대로 실적 상회"
    result = clean_source_header(raw)
    assert "NVDA" in result
    assert "데이터센터" in result


def test_final_report_no_제목_prefix():
    """_one_sentence()를 통한 report 경로에서도 '제목 :' 없어야 한다."""
    from tele_quant.deterministic_report import _one_sentence

    result = _one_sentence("제목 : 온 세미, AI·데이터센터 성장 기대 - 로스 외 *연합인포맥스*")
    assert "제목" not in result, f"'제목' still in result: {result!r}"
    assert "연합인포맥스" not in result, f"'연합인포맥스' still in result: {result!r}"
    assert "온 세미" in result or "AI" in result, f"content lost: {result!r}"


def test_final_report_no_broker_prefix():
    """_one_sentence()를 통한 report 경로에서도 브로커명 없어야 한다."""
    from tele_quant.deterministic_report import _one_sentence

    result = _one_sentence("모건스탠리) Nebius 1Q26 실적 프리뷰")
    assert "모건스탠리" not in result, f"broker name still in result: {result!r}"
    assert "Nebius" in result, f"content lost: {result!r}"
