from __future__ import annotations

from pathlib import Path

from tele_quant.analysis.aliases import AliasBook, AliasSymbol, ThemeAlias, load_alias_config

YAML_PATH = Path(__file__).parent.parent / "config" / "ticker_aliases.yml"


def _book() -> AliasBook:
    return load_alias_config(YAML_PATH)


# ── Symbol matching ───────────────────────────────────────────────────────────


def test_samsung_preferred_share_vs_common():
    """삼성전자우 and 삼성전자 must resolve to separate symbols."""
    book = _book()
    result = book.match_symbols("삼성전자우와 삼성전자 모두 상승")
    symbols = {m.symbol for m in result}
    assert "005935.KS" in symbols, "삼성전자우(005935.KS) not found"
    assert "005930.KS" in symbols, "삼성전자(005930.KS) not found"


def test_samsung_preferred_not_double_counted():
    """Characters consumed by 삼성전자우 must not also match 삼성전자."""
    book = _book()
    result = book.match_symbols("삼성전자우 실적 발표")
    symbols = {m.symbol for m in result}
    # 삼성전자우 consumed — 삼성전자 must NOT appear unless a separate occurrence exists
    assert "005935.KS" in symbols
    assert "005930.KS" not in symbols, "삼성전자 should not match inside 삼성전자우"


def test_lg_alone_no_stock_match():
    """Bare 'LG' (2-char all-caps) requires context and should not match without it."""
    book = _book()
    # Text deliberately has no stock-related vocabulary.
    result = book.match_symbols("LG가 새로운 광고를 냈다")
    symbols = {m.symbol for m in result}
    # None of the LG-family stocks should appear
    lg_symbols = {"373220.KS", "051910.KS", "066570.KS", "051900.KS", "032640.KS"}
    assert not (symbols & lg_symbols), f"LG alone matched: {symbols & lg_symbols}"


def test_lg_energy_matches():
    """LG에너지솔루션 (long alias) must match without context."""
    book = _book()
    result = book.match_symbols("LG에너지솔루션 수주 발표")
    symbols = {m.symbol for m in result}
    assert "373220.KS" in symbols, "LG에너지솔루션(373220.KS) not found"


def test_nvda_with_dollar_sign():
    """$NVDA should provide enough context to match the short ticker."""
    book = _book()
    result = book.match_symbols("$NVDA 실적이 예상치를 상회했다")
    symbols = {m.symbol for m in result}
    assert "NVDA" in symbols, "$NVDA should match"


def test_nvda_alone_no_match():
    """NVDA without stock context should not be matched (4-char all-caps rule)."""
    book = _book()
    result = book.match_symbols("NVDA 패키지를 import하여 GPU 가속 처리를 수행한다")
    symbols = {m.symbol for m in result}
    assert "NVDA" not in symbols, "NVDA without stock context should not match"


def test_korean_name_nvda_matches():
    """엔비디아 (Korean alias) must match NVDA without any additional context."""
    book = _book()
    result = book.match_symbols("엔비디아 실적 발표로 AI 섹터 강세")
    symbols = {m.symbol for m in result}
    assert "NVDA" in symbols, "엔비디아 should resolve to NVDA"


def test_multi_stock_and_hbm_theme():
    """A message about HBM should hit Samsung + SK Hynix + HBM theme."""
    book = _book()
    text = "삼성전자와 SK하이닉스 모두 HBM3E 수주 소식에 주가 급등"
    symbols = {m.symbol for m in book.match_symbols(text)}
    themes = book.match_themes(text)
    assert "005930.KS" in symbols
    assert "000660.KS" in symbols
    assert "HBM" in themes


def test_ai_is_theme_not_stock():
    """'AI' alone should not match as any stock symbol."""
    book = _book()
    result = book.match_symbols("AI 수요 증가로 반도체 섹터 강세")
    symbols = {m.symbol for m in result}
    # AI is a macro keyword → no direct stock match
    assert "NVDA" not in symbols, "AI alone should not map to NVDA"


def test_theme_matching_robot():
    """로봇 keyword should surface the 로봇 theme."""
    book = _book()
    themes = book.match_themes("산업용로봇 시장 성장 기대감 상승")
    assert "로봇" in themes


def test_theme_matching_2nd_battery():
    """배터리 keyword should surface the 2차전지 theme."""
    book = _book()
    themes = book.match_themes("전기차 배터리 소재 양극재 수요 급증")
    assert "2차전지" in themes


def test_crypto_excluded_from_symbol_matches_for_stocks():
    """CRYPTO market symbols must be in match_symbols but flagged as CRYPTO market."""
    book = _book()
    result = book.match_symbols("비트코인 급등에 코인 관련주 상승")
    markets = {m.market for m in result}
    # CRYPTO entries present but can be filtered downstream
    assert "CRYPTO" in markets


def test_longest_alias_wins_disambiguation():
    """포스코퓨처엠 should not be split into 포스코 + something else."""
    book = _book()
    result = book.match_symbols("포스코퓨처엠 양극재 사업 호재")
    symbols = {m.symbol for m in result}
    assert "003670.KS" in symbols, "포스코퓨처엠(003670.KS) not found"
    # 포스코홀딩스 (005490.KS) should NOT match — no separate 포스코 occurrence
    assert "005490.KS" not in symbols, "포스코홀딩스 incorrectly matched inside 포스코퓨처엠"


def test_book_loads_from_yaml():
    """AliasBook should load without error and contain expected symbols."""
    book = _book()
    symbols = {s.symbol for s in book.all_symbols}
    assert "005930.KS" in symbols
    assert "NVDA" in symbols
    assert "BTC-USD" in symbols


def test_inline_book_no_yaml():
    """AliasBook can be built directly without YAML for unit testing."""
    sym = AliasSymbol(
        symbol="TEST",
        name="TestCo",
        market="US",
        aliases=["TestCo", "테스트코"],
    )
    theme = ThemeAlias(name="테스트테마", aliases=["테스트테마"])
    book = AliasBook([sym], [theme])
    result = book.match_symbols("테스트코 주가 상승")
    assert result[0].symbol == "TEST"
    assert "테스트테마" not in book.match_themes("무관한 텍스트")
    assert "테스트테마" in book.match_themes("테스트테마 관련주 급등")


# ── New alias tests ───────────────────────────────────────────────────────────


def test_merck_gardasil_maps_to_mrk():
    """Gardasil (Merck brand) and 'Merck' should resolve to MRK."""
    book = _book()
    result = book.match_symbols("Gardasil 관련 Merck 실적 발표로 주가 상승")
    symbols = {m.symbol for m in result}
    assert "MRK" in symbols, "Merck/Gardasil should map to MRK"


def test_mrk_ticker_requires_context():
    """Bare 'MRK' without stock vocabulary should not match."""
    book = _book()
    result = book.match_symbols("MRK 데이터를 분석하는 파이썬 라이브러리")
    symbols = {m.symbol for m in result}
    assert "MRK" not in symbols, "MRK without stock context should not match"


def test_dusan_standalone_no_match():
    """Bare '두산' alone must not match any specific stock via a standalone alias."""
    book = _book()
    result = book.match_symbols("두산 관련 소식이 있다")
    for m in result:
        assert "두산" not in m.matched_aliases or len("두산") > 2, (
            f"{m.symbol} was matched via bare '두산' alias"
        )


def test_doosan_energability_matches():
    """두산에너빌리티 should match 034020.KS."""
    book = _book()
    result = book.match_symbols("두산에너빌리티 원전 사업 수주 기대 주가 상승")
    symbols = {m.symbol for m in result}
    assert "034020.KS" in symbols, "두산에너빌리티 → 034020.KS"


def test_doosan_robotics_matches():
    """두산로보틱스 should match 454910.KS."""
    book = _book()
    result = book.match_symbols("두산로보틱스 로봇 수주 발표 실적 기대")
    symbols = {m.symbol for m in result}
    assert "454910.KS" in symbols, "두산로보틱스 → 454910.KS"


def test_korean_air_maps_to_003490():
    """대한항공 should map to 003490.KS."""
    book = _book()
    result = book.match_symbols("대한항공 실적 발표 주가 강세")
    symbols = {m.symbol for m in result}
    assert "003490.KS" in symbols, "대한항공 → 003490.KS"


def test_lly_with_context():
    """일라이릴리 (Korean name) should match LLY without requiring context."""
    book = _book()
    result = book.match_symbols("일라이릴리 비만치료제 수요 급증")
    symbols = {m.symbol for m in result}
    assert "LLY" in symbols, "일라이릴리 should map to LLY"
