from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tele_quant.evidence import EvidenceCluster

# Broker/channel header patterns -- stripped from headlines before display
_LENTICULAR_RE = re.compile(r"◈[^◈]*◈")
# Broker name + ")" prefix: "모건스탠리) Nebius 실적" → "Nebius 실적"
_BROKER_PAREN_PREFIX_RE = re.compile(
    r"^(?:JP모건|골드만삭스|골드만|모건스탠리|씨티|뱅크오브아메리카|"
    r"Goldman\s*Sachs?|Morgan\s*Stanley|JP\s*Morga?n|JPMorgan|"
    r"BofA|Bank\s+of\s+America|Wedbush|HSBC|Citi(?:group)?|Piper\s+Sandler|"
    r"JPM|GS|MS|BAC)"
    r"[)]\s+",
    re.IGNORECASE,
)
# Source attribution suffixes: "- 로스 외 *연합인포맥스*" / "*연합인포맥스*"
_SOURCE_SUFFIX_PARTS = (
    "\\s*[-–]\\s*(?:\\S+\\s+외\\s+)?\\*[^*]{1,40}\\*\\s*$",  # noqa: RUF001
    "\\s*\\*(?:연합인포맥스|뉴시스|뉴스1|머니투데이|이데일리)[^*]{0,20}\\*\\s*$",
    "\\s*[-–]\\s*(?:Reuters|Bloomberg|Yonhap)\\s*$",  # noqa: RUF001
)
_SOURCE_SUFFIX_RE = re.compile("|".join(_SOURCE_SUFFIX_PARTS), re.IGNORECASE)
# Subject identification for colon-insertion after broker prefix removal
# Matches: English cap word, ALL-CAPS ticker, or Korean+parens-ticker
_SUBJECT_RE = re.compile(
    r"^([A-Z][A-Za-z0-9]*(?:\([A-Z]+\))?|[A-Z]{2,5}|\S+\([A-Z]{1,5}\))\s+(.+)$"
)
_MAILBOX_BRACKET_RE = re.compile(r"^\U0001f4ee\s*\[[^\]]*\]\s*", re.MULTILINE)
_BROKER_BRACKET_RE = re.compile(
    r"^\[[^\]]*(?:증권|리서치|투자|Securities|Research|Global|"
    r"하나|메리츠|신한|키움|KB|NH|"
    r"삼성|대신|미래에셋|현대차|DB|"
    r"한투|골드만|JP모건|씨티|BofA|유안타|교보|이베스트|SK|기업)[^\]]*\]\s*",
    re.IGNORECASE,
)
_GLOBAL_RESEARCH_RE = re.compile(r"^Global Research\s*", re.IGNORECASE)
_PHONE_RE = re.compile(r"☎️?\s*\d[\d\-\s]{4,14}")
# Korean phone numbers in parentheses: (02-3770-5590), (010-1234-5678)
_KOREAN_PHONE_RE = re.compile(r"\(0\d{1,2}[-–]\d{3,4}[-–]\d{4}\)")  # noqa: RUF001
# Inline news agency attribution anywhere in text: *연합인포맥스*, *뉴시스*
_INLINE_NEWS_AGENCY_RE = re.compile(
    r"\*(?:연합인포맥스|뉴시스|뉴스1|머니투데이|이데일리)[^*]{0,20}\*",
    re.IGNORECASE,
)
_URL_RE = re.compile(r"https?://\S+|t\.me/\S+", re.IGNORECASE)
_US_SUFFIX_RE = re.compile(r"\(([A-Z]{1,5})\.US\)")
# "제목 :" 또는 "제목:" 접두어 제거
_TITLE_PREFIX_RE = re.compile(r"^제목\s*:\s*", re.IGNORECASE)
# Plain-text broker/channel names at start of text (no brackets)
_PLAIN_BROKER_HEADER_RE = re.compile(
    r"^(?:Hana\s+Global\s+Guru\s+Eye|유안타\s*리서치센터|"
    r"하나증권\s*해외주식분석|키움증권\s*미국\s*주식[^가-힣]{0,10}|"
    r"(?:모닝|아침|저녁|일일|주간|프리마켓|애프터마켓)\s*(?:브리핑|뉴스|리포트)?)"
    r"\s*[:\-]?\s*",
    re.IGNORECASE,
)
# Metadata noise prefixes: "link: ...", "카테고리: ...", "출처: ..."
_METADATA_PREFIX_RE = re.compile(
    r"^(?:link|카테고리|출처|출처명|날짜|일시)\s*:\s*\S*\s*",
    re.IGNORECASE | re.MULTILINE,
)
# Characters to strip from result edges
# U+00B7 middle dot, U+2014 em-dash, U+2013 en-dash
_STRIP_CHARS = "[] \t\n·—–"  # noqa: RUF001

# Patterns that indicate the headline is broker/header-only (no investment content)
_HEADER_ONLY_RES = [
    re.compile(r"^◈[^◈]*◈\s*$"),
    re.compile(r"^\[[^\]]*(?:증권|리서치|투자)[^\]]*\]\s*$", re.IGNORECASE),
    re.compile(r"^Global Research\s*$", re.IGNORECASE),
    re.compile(r"^Hana\s+Global\s+Guru\s+Eye\s*$", re.IGNORECASE),
    re.compile(r"^유안타\s*리서치센터\s*$", re.IGNORECASE),
    re.compile(r"^하나증권\s*해외주식분석\s*$", re.IGNORECASE),
    re.compile(r"^키움증권\s*미국\s*주식\s*박기현.*$", re.IGNORECASE),
    re.compile(r"^(?:모닝|아침|일일|프리마켓)\s*(?:브리핑|뉴스)\s*$", re.IGNORECASE),
    re.compile(r"^(?:link|카테고리|출처)\s*:\s*\S*\s*$", re.IGNORECASE),
    re.compile(r"^ShowHashtag\b", re.IGNORECASE),
    re.compile(r"^연합인포맥스\s*$", re.IGNORECASE),
    # Broker greeting: "안녕하세요 키움 이차전지 권준수입니다." style
    re.compile(r"^안녕하세요\s+.{2,30}입니다[\.\s]*$"),
]

# Noise / low-quality patterns: when matched, headline is low-investment-relevance
_LOW_QUALITY_RES = [
    re.compile(r"S&P\s*500\s*map", re.IGNORECASE),
    re.compile(r"부동산\s*자료\s*참고", re.IGNORECASE),
    re.compile(r"방청\s*후기", re.IGNORECASE),
    re.compile(r"머스크\s*소송전", re.IGNORECASE),
    re.compile(r"일반\s*AI\s*뉴스", re.IGNORECASE),
    re.compile(r"^link\s*:", re.IGNORECASE),
    re.compile(r"^카테고리\s*:", re.IGNORECASE),
    re.compile(r"^출처\s*:", re.IGNORECASE),
    re.compile(r"ShowHashtag", re.IGNORECASE),
    re.compile(r"하나증권\s*해외주식분석", re.IGNORECASE),
    re.compile(r"키움증권\s*미국\s*주식\s*박기현", re.IGNORECASE),
]

# Patterns that disqualify a sentence from being used as evidence
_NOISE_SENTENCE_RES = [
    re.compile(r"tel:|href=|ShowHashtag", re.IGNORECASE),
    re.compile(r"☎️?\s*\d[\d\-\s]{4,14}"),
    re.compile(r"\(0\d{1,2}[-–]\d{3,4}[-–]\d{4}\)"),  # noqa: RUF001
    re.compile(r"하나증권\s*해외주식분석|키움증권\s*미국\s*주식", re.IGNORECASE),
    re.compile(r"유안타\s*리서치센터|Hana\s*Global\s*Guru\s*Eye", re.IGNORECASE),
    re.compile(r"연합인포맥스", re.IGNORECASE),
    re.compile(r"S&P\s*500\s*map", re.IGNORECASE),
    re.compile(r"^(?:제목|카테고리|출처명?|증권사|원문)\s*:", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^안녕하세요\s+.{2,30}입니다"),
]


def _strip_broker_paren_prefix(text: str) -> str:
    """'모건스탠리) Nebius 실적' → 'Nebius: 실적';  'Citi) 팔란티어; 서프라이즈' → '팔란티어: 서프라이즈'."""
    m = _BROKER_PAREN_PREFIX_RE.match(text)
    if not m:
        return text
    content = text[m.end() :].strip()
    if not content:
        return text
    # Semicolon separator: "Subject; rest" → "Subject: rest"
    if ";" in content:
        subj, _, rest = content.partition(";")
        return f"{subj.strip()}: {rest.strip()}"
    # Recognizable proper noun at start → insert ":"
    m2 = _SUBJECT_RE.match(content)
    if m2:
        return f"{m2.group(1)}: {m2.group(2)}"
    return content


def is_broker_header_only(text: str) -> bool:
    """Return True if the text is a broker/channel header with no investment content."""
    stripped = text.strip()
    return any(pat.match(stripped) for pat in _HEADER_ONLY_RES)


def is_low_quality_headline(text: str) -> bool:
    """Return True if the headline matches known noise/low-quality patterns."""
    return any(pat.search(text) for pat in _LOW_QUALITY_RES)


_LEADING_ANY_BRACKET_RE = re.compile(r"^(?:\[[^\]]{0,40}\]\s*)+")


def clean_source_header(text: str) -> str:
    """Remove broker/channel header patterns, source suffixes, phone numbers, and URLs."""
    result = text.strip()
    result = _TITLE_PREFIX_RE.sub("", result)
    result = _LENTICULAR_RE.sub("", result)
    result = _MAILBOX_BRACKET_RE.sub("", result)
    result = _BROKER_BRACKET_RE.sub("", result)
    # Strip any remaining leading [word] tags (e.g. [두산테스나] after [기업] was removed)
    result = _LEADING_ANY_BRACKET_RE.sub("", result)
    result = _GLOBAL_RESEARCH_RE.sub("", result)
    result = _PLAIN_BROKER_HEADER_RE.sub("", result)
    result = _METADATA_PREFIX_RE.sub("", result)
    # "모건스탠리) Nebius 실적" → "Nebius: 실적"
    result = _strip_broker_paren_prefix(result)
    # "- 로스 외 *연합인포맥스*" suffix first (anchored to $)
    result = _SOURCE_SUFFIX_RE.sub("", result)
    # Then *연합인포맥스* inline anywhere remaining
    result = _INLINE_NEWS_AGENCY_RE.sub("", result)
    result = _PHONE_RE.sub("", result)
    # Korean phone numbers in parens: (02-3770-5590)
    result = _KOREAN_PHONE_RE.sub("", result)
    result = _URL_RE.sub("", result)
    result = _US_SUFFIX_RE.sub(r"(\1)", result)
    result = re.sub(r"\s{2,}", " ", result).strip()
    result = result.strip(_STRIP_CHARS)
    return result


_BROKER_GREETING_RE = re.compile(r"^안녕하세요\s+.{2,40}입니다")


def extract_issue_sentence(text: str, fallback_title: str = "") -> str:
    """Return the core investment issue from text (<=90 chars, no broker headers)."""
    cleaned = clean_source_header(text)

    # Drop broker greetings before any further processing
    if _BROKER_GREETING_RE.match(cleaned):
        cleaned = ""

    if len(cleaned) < 8:
        cleaned = clean_source_header(fallback_title) if fallback_title else ""

    if _BROKER_GREETING_RE.match(cleaned):
        return ""

    if len(cleaned) < 8:
        return ""

    # Truncate to 90 chars on a word boundary
    if len(cleaned) > 90:
        cut = cleaned[:90].rsplit(" ", 1)[0]
        cleaned = (cut or cleaned[:90]) + "..."

    return cleaned


# ---- Final report cleaner (last-mile before Telegram send) ----

# Lines that must be dropped entirely from the final report
_FINAL_DROP_LINE_RES = [
    re.compile(r"^Hana\s+Global\s+Guru\s+Eye", re.IGNORECASE),
    re.compile(r"^유안타\s*리서치센터", re.IGNORECASE),
    re.compile(r"^하나증권\s*해외주식분석", re.IGNORECASE),
    re.compile(r"^키움증권\s*미국\s*주식", re.IGNORECASE),
    re.compile(r"^연합인포맥스\s*$", re.IGNORECASE),
    re.compile(r"^S&P\s*500\s*map", re.IGNORECASE),
    re.compile(r"^ShowHashtag\b", re.IGNORECASE),
    re.compile(r"^ShowBotCommand\b", re.IGNORECASE),
    re.compile(r"^[-\*•·\s]*제목\s*:", re.IGNORECASE),
    re.compile(r"^[-\*•·\s]*카테고리\s*:", re.IGNORECASE),
    re.compile(r"증권사\s*/?\s*출처\s*:", re.IGNORECASE),  # Naver 메타 어디서든
    re.compile(r"원문\s*/?\s*목록\s*텍스트\s*:", re.IGNORECASE),  # Naver 메타 어디서든
    re.compile(r"^link\s*:\s*\S*\s*$", re.IGNORECASE),
    re.compile(r"^href\s*=", re.IGNORECASE),
    re.compile(r"tel:\s*\+?\d[\d\s\-]{4,}", re.IGNORECASE),
    re.compile(r"☎️?\s*\d[\d\-\s]{4,14}"),
    re.compile(r"\(0\d{1,2}[-–]\d{3,4}[-–]\d{4}\)"),  # noqa: RUF001
    re.compile(r"^모닝\s*브리핑\s*$", re.IGNORECASE),
    re.compile(r"^프리마켓\s*뉴스\s*$", re.IGNORECASE),
    re.compile(r"^출처\s*:", re.IGNORECASE),
    # Broker greetings anywhere in the line: "안녕하세요 키움 이차전지 권준수입니다."
    re.compile(r"안녕하세요\s+.{2,40}입니다"),
    # "Web발신" SMS/fax noise (anywhere in the line)
    re.compile(r"Web발신"),
    # Malformed bracket artifacts: "[기업][종목명]" style
    re.compile(r"^\s*기업\s*\]\s*\["),
]

# Inline patterns to strip from lines (rather than drop the whole line)
_FINAL_STRIP_INLINE_RES = [
    re.compile(r"<a\s[^>]*>.*?</a>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<[^>]+>"),
    re.compile(r"\*(?:연합인포맥스|뉴시스|뉴스1|머니투데이|이데일리)[^*]{0,20}\*", re.IGNORECASE),
    re.compile(r"\s*[-–]\s*(?:\S+\s+외\s+)?\*[^*]{1,40}\*\s*$", re.IGNORECASE),  # noqa: RUF001
    re.compile(r"ShowHashtag\S*", re.IGNORECASE),
    re.compile(r"ShowBotCommand\S*", re.IGNORECASE),
    re.compile(r"href=['\"][^'\"]*['\"]", re.IGNORECASE),
    re.compile(r"tel:\+?\d[\d\s\-]{4,}", re.IGNORECASE),
]

# Broker-as-source prefix patterns that pollute digest lines
_BROKER_SOURCE_LINE_RE = re.compile(
    r"^(?:JP모건|JPMorgan|Goldman\s*Sachs?|골드만삭스?|모건스탠리|Morgan\s*Stanley|"
    r"씨티|Citi(?:group)?|뱅크오브아메리카|BofA|Bank\s+of\s+America|"
    r"Wedbush|HSBC|Piper\s+Sandler|Jefferies|DA\s+Davidson|"
    r"하나증권|유안타|키움증권|메리츠|신한투자|KB증권|NH투자|삼성증권|"
    r"대신증권|미래에셋|현대차증권|DB금융)\s*[):]\s*",
    re.IGNORECASE,
)


def _final_drop_line(line: str) -> bool:
    """Return True if the line should be dropped entirely from the final report."""
    stripped = line.strip()
    if not stripped:
        return False
    # Check header-only patterns (existing)
    if is_broker_header_only(stripped):
        return True
    # Check new drop patterns
    return any(p.search(stripped) for p in _FINAL_DROP_LINE_RES)


def apply_final_report_cleaner(text: str) -> str:
    """Last-mile cleaner applied to all text before Telegram send.

    Drops lines with broker headers / noise / link/tel junk.
    Strips inline HTML anchors, ShowHashtag, news-agency attributions.
    Does NOT modify section headers (emoji lines like 1️⃣, ─── etc.).
    """
    if not text:
        return text

    result_lines: list[str] = []
    for line in text.splitlines():
        # Drop noise lines entirely
        if _final_drop_line(line):
            continue
        # Strip inline noise
        cleaned = line
        for pat in _FINAL_STRIP_INLINE_RES:
            cleaned = pat.sub("", cleaned)
        # Strip broker-as-source prefix from non-section lines
        stripped = cleaned.strip()
        if stripped and not stripped.startswith(
            ("─", "1️", "2️", "3️", "4️", "5️", "6️", "7️", "8️", "🧠", "🟢", "🔴", "🟡")
        ):
            m = _BROKER_SOURCE_LINE_RE.match(stripped)
            if m:
                remainder = stripped[m.end() :].strip()
                if remainder:
                    cleaned = (" " * (len(line) - len(line.lstrip()))) + remainder
                else:
                    continue  # drop broker-header-only line
        result_lines.append(cleaned)

    # Collapse 2+ consecutive blank lines into 1
    collapsed: list[str] = []
    blank_run = 0
    for line in result_lines:
        if not line.strip():
            blank_run += 1
            if blank_run <= 1:
                collapsed.append(line)
        else:
            blank_run = 0
            collapsed.append(line)

    return "\n".join(collapsed).strip()


def is_noise_sentence(text: str) -> bool:
    """Return True if the text contains noise that disqualifies it as investment evidence."""
    return any(p.search(text) for p in _NOISE_SENTENCE_RES)


def summarize_issue_for_display(cluster: EvidenceCluster) -> str:
    """Get a display-ready headline from a cluster (strips broker headers)."""
    headline = extract_issue_sentence(cluster.headline, "")
    if not headline:
        hint = cluster.summary_hint[:150].replace("\n", " ").strip()
        headline = extract_issue_sentence(hint, "")
    if not headline:
        headline = "핵심 내용 추가 확인 필요"
    return headline
