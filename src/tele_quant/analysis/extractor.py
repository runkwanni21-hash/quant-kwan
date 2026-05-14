from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

from tele_quant.analysis.aliases import MatchedSymbol, load_alias_config
from tele_quant.analysis.models import StockCandidate
from tele_quant.models import RawItem
from tele_quant.textutil import truncate

if TYPE_CHECKING:
    from tele_quant.analysis.aliases import AliasBook
    from tele_quant.ollama_client import OllamaClient
    from tele_quant.settings import Settings

log = logging.getLogger(__name__)

# ── Legacy hardcoded dicts (kept for backward-compat / test usage) ────────────

KOREAN_STOCKS: dict[str, str] = {
    "삼성전자": "005930.KS",
    "SK하이닉스": "000660.KS",
    "하이닉스": "000660.KS",
    "NAVER": "035420.KS",
    "네이버": "035420.KS",
    "카카오": "035720.KS",
    "현대차": "005380.KS",
    "현대자동차": "005380.KS",
    "기아": "000270.KS",
    "기아차": "000270.KS",
    "LG에너지솔루션": "373220.KS",
    "엘지에너지솔루션": "373220.KS",
    "삼성SDI": "006400.KS",
    "셀트리온": "068270.KS",
    "한화에어로스페이스": "012450.KS",
    "한화에어로": "012450.KS",
    "HD현대중공업": "329180.KS",
    "현대중공업": "329180.KS",
    "두산에너빌리티": "034020.KS",
    "알테오젠": "196170.KQ",
    "포스코홀딩스": "005490.KS",
    "POSCO": "005490.KS",
    "KB금융": "105560.KS",
    "신한지주": "055550.KS",
    "하나금융지주": "086790.KS",
    "우리금융지주": "316140.KS",
    "카카오뱅크": "323410.KS",
    "크래프톤": "259960.KS",
    "엔씨소프트": "036570.KS",
    "넷마블": "251270.KS",
    "LG화학": "051910.KS",
    "롯데케미칼": "011170.KS",
    "한국전력": "015760.KS",
    "삼성바이오로직스": "207940.KS",
    "바이오로직스": "207940.KS",
    "에코프로비엠": "247540.KQ",
    "에코프로": "086520.KQ",
}

US_STOCKS: dict[str, str] = {
    "엔비디아": "NVDA",
    "NVIDIA": "NVDA",
    "NVDA": "NVDA",
    "애플": "AAPL",
    "AAPL": "AAPL",
    "마이크로소프트": "MSFT",
    "MSFT": "MSFT",
    "테슬라": "TSLA",
    "TSLA": "TSLA",
    "AMD": "AMD",
    "브로드컴": "AVGO",
    "AVGO": "AVGO",
    "메타": "META",
    "META": "META",
    "구글": "GOOGL",
    "알파벳": "GOOGL",
    "GOOGL": "GOOGL",
    "아마존": "AMZN",
    "AMZN": "AMZN",
    "팔란티어": "PLTR",
    "PLTR": "PLTR",
    "인텔": "INTC",
    "INTC": "INTC",
    "퀄컴": "QCOM",
    "QCOM": "QCOM",
    "마이크론": "MU",
    "MU": "MU",
    "ASML": "ASML",
    "ARM": "ARM",
    "아암": "ARM",
}

POSITIVE_WORDS = re.compile(
    r"호재|상승|강세|급등|기대|성장|수주|수요|흑자|배당|서프라이즈|긍정|돌파|매출|확대|신고|최고"
)
NEGATIVE_WORDS = re.compile(
    r"악재|하락|약세|급락|우려|부진|적자|감소|손실|위험|규제|제재|하향|하회|부담|약화|감산"
)

# Broker/source prefixes that should NOT count as stock ticker mentions
# e.g. "JP모건) 전자부품 섹터" → "JP모건" is a research tag, not a JPM stock mention
# Includes all alias forms (Korean names, full English names) used in AliasBook
_BROKER_TICKERS: frozenset[str] = frozenset(
    [
        # ASCII ticker aliases
        "JPM",
        "GS",
        "C",
        "MS",
        "BAC",
        "DB",
        "CS",
        # Korean broker name aliases
        "JP모건",
        "제이피모건",
        "골드만삭스",
        "골드만",
        "모건스탠리",
        "씨티",
        "뱅크오브아메리카",
        # Full English names and variants
        "Goldman Sachs",
        "Goldman",
        "Morgan Stanley",
        "JPMorgan Chase",
        "JP Morgan",
        "JPMorgan",
        "Citigroup",
        # Additional brokers (appear as source tags but not traded as stocks in these msgs)
        "BofA",
        "Bank of America",
        "Wedbush",
        "HSBC",
        "Citi",
        "Piper Sandler",
        "Piper",
        "Jefferies",
        "DA Davidson",
        "Raymond James",
        "Barclays",
        "UBS",
        "Credit Suisse",
        "Deutsche Bank",
        "Wells Fargo",
        "RBC",
        "Truist",
        "Oppenheimer",
        "Baird",
        "Needham",
        "Susquehanna",
        "Bernstein",
        "Mizuho",
        "SMBC Nikko",
        "KeyBanc",
        "Stifel",
    ]
)
_BROKER_SUFFIX_RE = re.compile(r"^[)\]]\s*|^\s*외\b|^:\s*")

# Keywords indicating broker is the SUBJECT (not just the source) of the news
_BROKER_SELF_NEWS_RE = re.compile(
    r"EPS|실적|영업이익|순이익|매출|IB수익|트레이딩수익|자사주|배당|CEO|대손충당금|"
    r"Q[1-4]|[1-4]Q|[Bb]eat|수수료수익|ROE|ROA|주당순이익|순영업수익|거래수익|"
    r"주가\s*(?:상승|하락|급등|급락)|투자등급\s*상향|투자등급\s*하향|신용등급",
    re.IGNORECASE,
)

# Keywords indicating broker appears as a SOURCE/ATTRIBUTOR at start of text
_BROKER_AS_SOURCE_RE = re.compile(
    r"^(?:시장\s*코멘트?|분석가?|애널리스트|스트래티지스트?|리서치|전략가|"
    r"시장\s*전망|투자의견|섹터\s*분석|업종\s*분석|코멘트|주식\s*시장|"
    r"금융시장|섹터|전략|전망|관련\s*코멘트|보고서|리포트|뷰|전략\s*뷰)",
    re.IGNORECASE,
)

# Noise patterns in context windows — disqualify as direct evidence
_NOISE_CONTEXT_RE = re.compile(
    r"tel:|href=|ShowHashtag|☎|연합인포맥스|하나증권\s*해외주식분석|"
    r"키움증권\s*미국\s*주식|유안타\s*리서치센터|Hana\s*Global\s*Guru|"
    r"\d{2,3}[-–]\d{3,4}[-–]\d{4}",  # noqa: RUF001
    re.IGNORECASE,
)


def _is_noise_context(text: str) -> bool:
    return bool(_NOISE_CONTEXT_RE.search(text))


def _infer_sentiment(texts: list[str]) -> str:
    pos = sum(len(POSITIVE_WORDS.findall(t)) for t in texts)
    neg = sum(len(NEGATIVE_WORDS.findall(t)) for t in texts)
    if pos > 0 and neg == 0:
        return "positive"
    if neg > 0 and pos == 0:
        return "negative"
    if pos > 0 and neg > 0:
        return "mixed"
    return "neutral"


def _extract_surrounding_context(text: str, name: str, window: int = 80) -> str:
    idx = text.find(name)
    if idx < 0:
        return ""
    start = max(0, idx - window)
    end = min(len(text), idx + len(name) + window)
    return text[start:end]


def extract_candidates_dict_fallback(
    items: list[RawItem],
    max_symbols: int = 15,
) -> list[StockCandidate]:
    """Dictionary-based fallback extractor. Always safe to call."""
    all_texts = [item.compact_text for item in items]
    combined = " ".join(all_texts)

    mention_map: dict[str, dict[str, Any]] = {}

    for name, symbol in KOREAN_STOCKS.items():
        count = combined.count(name)
        if count == 0:
            continue
        if symbol not in mention_map:
            mention_map[symbol] = {"name": name, "market": "KR", "mentions": 0, "contexts": []}
        mention_map[symbol]["mentions"] += count
        for text in all_texts:
            if name in text:
                ctx = _extract_surrounding_context(text, name)
                if ctx:
                    mention_map[symbol]["contexts"].append(ctx)

    for name, symbol in US_STOCKS.items():
        count = combined.count(name)
        if count == 0:
            continue
        if symbol not in mention_map:
            mention_map[symbol] = {"name": name, "market": "US", "mentions": 0, "contexts": []}
        mention_map[symbol]["mentions"] += count
        for text in all_texts:
            if name in text:
                ctx = _extract_surrounding_context(text, name)
                if ctx:
                    mention_map[symbol]["contexts"].append(ctx)

    candidates: list[StockCandidate] = []
    for symbol, info in sorted(mention_map.items(), key=lambda x: -x[1]["mentions"]):
        contexts = [c for c in info["contexts"][:10] if not _is_noise_context(c)]
        sentiment = _infer_sentiment(contexts)
        catalysts = [ctx for ctx in contexts[:3] if POSITIVE_WORDS.search(ctx)]
        risks = [ctx for ctx in contexts[:3] if NEGATIVE_WORDS.search(ctx)]
        candidates.append(
            StockCandidate(
                symbol=symbol,
                name=info["name"],
                market=info["market"],
                mentions=info["mentions"],
                sentiment=sentiment,
                catalysts=[truncate(c, 60) for c in catalysts],
                risks=[truncate(r, 60) for r in risks],
                source_titles=[
                    truncate(item.display_title, 60)
                    for item in items
                    if info["name"] in item.compact_text
                ][:3],
                direct_evidence_count=len(contexts),
            )
        )
    return candidates[:max_symbols]


# ── AliasBook-based extractor ─────────────────────────────────────────────────


def fast_extract_candidates(
    items: list[RawItem],
    settings: Settings,
) -> list[StockCandidate]:
    """Fast candidate extraction using AliasBook + dict fallback only. No LLM calls."""
    max_symbols = settings.analysis_max_symbols
    try:
        book = load_alias_config(Path(settings.ticker_aliases_path))
        return extract_candidates_with_book(items, max_symbols, book)
    except Exception as exc:
        log.warning("[extractor] fast AliasBook failed, falling back to dict: %s", exc)
        return extract_candidates_dict_fallback(items, max_symbols=max_symbols)


def _is_broker_prefix_match(text: str, alias: str, idx: int) -> bool:
    """Return True if the alias match at idx looks like a broker/source prefix.

    Examples that should be excluded:
      "JPM) 전자부품 섹터" → "JPM" at idx 0 followed by ")"
      "GS 외" → "GS" followed by " 외"
    """
    if alias not in _BROKER_TICKERS:
        return False
    suffix_start = idx + len(alias)
    suffix = text[suffix_start : suffix_start + 4]
    return bool(_BROKER_SUFFIX_RE.match(suffix))


def _is_broker_attribution(text: str, alias: str, idx: int) -> bool:
    """True if alias is a broker/source attribution, not a stock subject.

    For broker tickers, context must contain SELF-NEWS keywords (EPS, 실적, 주가, etc.)
    to be treated as stock subject. Any other context is treated as broker attribution.

    Examples:
    - "JPM) 전자부품 섹터" → True (broker prefix with suffix ")")
    - "Goldman Sachs: AI 데이터센터" → True (no self-news)
    - "Goldman Sachs Q1 EPS beat" → False (has self-news "EPS")
    - "JP모건 주가 급등" → False (has self-news "주가 급등")
    """
    if alias not in _BROKER_TICKERS:
        return False

    suffix_start = idx + len(alias)
    suffix = text[suffix_start : suffix_start + 6]

    # Standard broker prefix: "JPM) ...", "GS: ...", "Goldman 외"
    if _BROKER_SUFFIX_RE.match(suffix):
        return True

    # For ALL broker tickers: require self-news keywords to treat as stock subject
    context_after = text[suffix_start : suffix_start + 100]
    # Return False only for genuine self-news (broker's own earnings/price)
    return not bool(_BROKER_SELF_NEWS_RE.search(context_after))


def extract_candidates_with_book(
    items: list[RawItem],
    max_symbols: int,
    book: AliasBook,
) -> list[StockCandidate]:
    """AliasBook-based extractor with longest-first position-tracking matching."""
    all_texts = [item.compact_text for item in items]
    combined = " ".join(all_texts)

    matched: list[MatchedSymbol] = book.match_symbols(combined)
    # Exclude pure crypto from stock scenario list
    matched = [m for m in matched if m.market != "CRYPTO"]

    candidates: list[StockCandidate] = []
    for m in matched:
        raw_contexts: list[str] = []
        for alias in m.matched_aliases:
            for text in all_texts:
                idx = text.find(alias)
                if idx >= 0:
                    # Skip broker source attributions (e.g. "JPM) 전자부품" / "Goldman Sachs 시장 코멘트")
                    if _is_broker_attribution(text, alias, idx):
                        continue
                    lo = max(0, idx - 80)
                    hi = min(len(text), idx + len(alias) + 80)
                    ctx = text[lo:hi]
                    if not _is_noise_context(ctx):
                        raw_contexts.append(ctx)

        contexts = raw_contexts[:10]
        direct_evidence_count = len(contexts)
        sentiment = _infer_sentiment(contexts)
        catalysts = [ctx for ctx in contexts[:3] if POSITIVE_WORDS.search(ctx)]
        risks = [ctx for ctx in contexts[:3] if NEGATIVE_WORDS.search(ctx)]
        source_titles = [
            truncate(item.display_title, 60)
            for item in items
            if any(alias in item.compact_text for alias in m.matched_aliases)
        ][:3]

        candidates.append(
            StockCandidate(
                symbol=m.symbol,
                name=m.name,
                market=m.market,
                mentions=m.mentions,
                sentiment=sentiment,
                catalysts=[truncate(c, 60) for c in catalysts],
                risks=[truncate(r, 60) for r in risks],
                source_titles=source_titles,
                direct_evidence_count=direct_evidence_count,
            )
        )

    # Sort: mentions desc → positive/mixed sentiment → more catalysts → more sources
    def _sort_key(c: StockCandidate) -> tuple[int, int, int, int]:
        sent_score = {"positive": 2, "mixed": 1, "neutral": 0, "negative": -1}.get(c.sentiment, 0)
        return (-c.mentions, -sent_score, -len(c.catalysts), -len(c.source_titles))

    candidates.sort(key=_sort_key)
    return candidates[:max_symbols]


# ── LLM merge helper ──────────────────────────────────────────────────────────


def _merge_llm_candidates(
    llm_results: list[dict[str, Any]],
    base_candidates: list[StockCandidate],
    max_symbols: int,
) -> list[StockCandidate]:
    merged: dict[str, StockCandidate] = {c.symbol: c for c in base_candidates}

    for item in llm_results:
        name = str(item.get("name") or "").strip()
        symbol = str(item.get("symbol") or "").strip()
        # Normalize LLM-generated US suffix (e.g. "KLAC.US" → "KLAC")
        if symbol.endswith(".US"):
            symbol = symbol[:-3]
        market = str(item.get("market") or "UNKNOWN").upper()
        sentiment = str(item.get("sentiment") or "neutral")
        catalysts = [str(c) for c in (item.get("catalysts") or []) if c]
        risks = [str(r) for r in (item.get("risks") or []) if r]

        if not symbol:
            symbol = KOREAN_STOCKS.get(name) or US_STOCKS.get(name) or ""
            if not symbol:
                continue
            market = "KR" if symbol.endswith((".KS", ".KQ")) else "US"

        if market == "CRYPTO":
            continue

        if symbol in merged:
            existing = merged[symbol]
            existing.mentions += 1
            existing.catalysts = list(dict.fromkeys(existing.catalysts + catalysts))[:5]
            existing.risks = list(dict.fromkeys(existing.risks + risks))[:5]
        else:
            merged[symbol] = StockCandidate(
                symbol=symbol,
                name=name or None,
                market=market,
                mentions=1,
                sentiment=sentiment,
                catalysts=catalysts[:5],
                risks=risks[:5],
            )

    result = sorted(merged.values(), key=lambda c: -c.mentions)
    return result[:max_symbols]


async def extract_candidates(
    ollama: OllamaClient,
    items: list[RawItem],
    digest_text: str,
    settings: Settings,
) -> list[StockCandidate]:
    """Extract stock candidates using AliasBook + LLM, falling back to dict."""
    max_symbols = settings.analysis_max_symbols

    # Try AliasBook first (YAML-driven, richer coverage)
    try:
        aliases_path = Path(settings.ticker_aliases_path)
        book = load_alias_config(aliases_path)
        book_candidates = extract_candidates_with_book(items, max_symbols, book)
        log.info("[extractor] AliasBook: %d candidates", len(book_candidates))
    except Exception as exc:
        log.warning("[extractor] AliasBook failed, using dict fallback: %s", exc)
        book_candidates = extract_candidates_dict_fallback(items, max_symbols=max_symbols)

    # Build input for LLM enrichment
    combined_texts = "\n\n".join(
        f"[{i + 1}] {truncate(item.compact_text, 400)}" for i, item in enumerate(items[:60])
    )
    if digest_text:
        combined_texts += f"\n\n[요약]\n{truncate(digest_text, 1500)}"

    system = (
        "한국어 금융정보 분석가. 텍스트에서 언급된 주식 종목만 추출. "
        "반드시 JSON array만 출력. /no_think"
    )
    prompt = json.dumps(
        {
            "task": "아래 텍스트에서 언급된 주식 종목을 추출하고 감성을 분석해줘.",
            "output_format": (
                "JSON array of objects: "
                '{"name": "종목명", "symbol": "티커(있으면)", "market": "KR|US|UNKNOWN", '
                '"sentiment": "positive|negative|mixed|neutral", '
                '"catalysts": ["호재 이유"], "risks": ["악재 이유"]}'
            ),
            "text": combined_texts[:6000],
        },
        ensure_ascii=False,
    )

    try:
        raw = await ollama.generate_text(
            prompt, system=system, max_ctx=min(settings.ollama_num_ctx, 8192)
        )
        raw = raw.strip()
        start = raw.find("[")
        end = raw.rfind("]")
        if start >= 0 and end > start:
            llm_data: list[dict[str, Any]] = json.loads(raw[start : end + 1])
            if isinstance(llm_data, list):
                merged = _merge_llm_candidates(llm_data, book_candidates, max_symbols)
                log.info("[extractor] LLM+AliasBook merged: %d candidates", len(merged))
                return merged
    except Exception as exc:
        log.warning("[extractor] LLM extraction failed, using AliasBook result: %s", exc)

    return book_candidates
