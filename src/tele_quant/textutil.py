from __future__ import annotations

import hashlib
import html
import re
import unicodedata
from collections.abc import Iterable

URL_RE = re.compile(r"https?://\S+|t\.me/\S+", re.IGNORECASE)
WHITESPACE_RE = re.compile(r"\s+")
EMOJI_NOISE_RE = re.compile(r"[\u200b\u200c\u200d\ufeff]")
# Matches /bot<TOKEN>/ in Telegram API URLs
_BOT_TOKEN_RE = re.compile(r"(api\.telegram\.org/bot)[^/\s]+")
# Python dataclass/object repr patterns that must not appear in Telegram output
_DATACLASS_LINE_RE = re.compile(
    r"\b(TradeScenario|StockCandidate|ExpandedCandidate|ScoreCard|"
    r"EvidenceCluster|ResearchLeadLagPair|IntradayTechnicalSnapshot|"
    r"TechnicalSnapshot|FundamentalSnapshot|WeeklyInput|WeeklySummary|"
    r"RunReport|RunStats|RawItem)\s*\("
)


def clean_text(text: str) -> str:
    text = html.unescape(text or "")
    text = unicodedata.normalize("NFKC", text)
    text = EMOJI_NOISE_RE.sub("", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [WHITESPACE_RE.sub(" ", line).strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines).strip()


def normalize_for_hash(text: str) -> str:
    text = clean_text(text).lower()
    text = URL_RE.sub("", text)
    text = re.sub(r"[#▶️✅■□◆◇★☆※→←↑↓ㆍ·•|]+", " ", text)
    text = WHITESPACE_RE.sub(" ", text)
    return text.strip()


def content_hash(text: str) -> str:
    return hashlib.sha256(normalize_for_hash(text).encode("utf-8")).hexdigest()


def short_hash(text: str, n: int = 12) -> str:
    return content_hash(text)[:n]


def truncate(text: str, max_chars: int, suffix: str = "…") -> str:
    text = text or ""
    if len(text) <= max_chars:
        return text
    return text[: max_chars - len(suffix)].rstrip() + suffix


def chunk_message(text: str, limit: int = 3200) -> list[str]:
    """Split text into Telegram-safe chunks.

    Telegram sendMessage limit is 4096 chars. We use 3200 to stay comfortably below
    even after prefixes like "(1/3)\n" are added. Splitting is line-based so Korean
    sentences are not broken mid-character. Lines longer than limit are force-split.
    """
    text = text.strip()
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    current_lines: list[str] = []
    current_len = 0

    for line in text.splitlines():
        # Force-split any single line that exceeds the limit
        if len(line) > limit:
            if current_lines:
                chunks.append("\n".join(current_lines).strip())
                current_lines = []
                current_len = 0
            for i in range(0, len(line), limit):
                part = line[i : i + limit].strip()
                if part:
                    chunks.append(part)
            continue

        add_len = len(line) + 1  # +1 for the newline
        if current_lines and current_len + add_len > limit:
            chunks.append("\n".join(current_lines).strip())
            current_lines = [line]
            current_len = len(line) + 1
        else:
            current_lines.append(line)
            current_len += add_len

    if current_lines:
        chunks.append("\n".join(current_lines).strip())

    return [c for c in chunks if c]


def sanitize_for_telegram(text: str) -> str:
    """Remove Python dataclass repr lines that must not appear in Telegram messages.

    Protects against accidental leakage of TradeScenario(...), StockCandidate(...) etc.
    Multi-line reprs (opening paren on one line, closing on another) are collapsed.
    """
    lines = text.splitlines()
    cleaned: list[str] = []
    depth = 0
    for line in lines:
        if depth == 0 and _DATACLASS_LINE_RE.search(line):
            depth = line.count("(") - line.count(")")
            if depth <= 0:
                depth = 0
            continue  # skip this line
        if depth > 0:
            depth += line.count("(") - line.count(")")
            if depth <= 0:
                depth = 0
            continue  # still inside repr
        cleaned.append(line)
    return "\n".join(cleaned)


def join_nonempty(parts: Iterable[str], sep: str = "\n") -> str:
    return sep.join(part.strip() for part in parts if part and part.strip())


def mask_bot_token(text: str) -> str:
    """Replace Telegram bot token in URLs with ***REDACTED***."""
    return _BOT_TOKEN_RE.sub(r"\1***REDACTED***", text)


_SECRET_QUERY_KEYS: frozenset[str] = frozenset(
    ["api_key", "apikey", "token", "access_token", "key", "secret", "password", "client_secret"]
)


def mask_url_secrets(url: str) -> str:
    """Mask secret query-string params (api_key, token, …) in a URL."""
    from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse

    def _quote_via(s: str, safe: str, encoding: str, errors: str) -> str:
        return quote(s, safe="*", encoding=encoding, errors=errors)

    try:
        parsed = urlparse(url)
        if not parsed.query:
            return url
        qs = parse_qs(parsed.query, keep_blank_values=True)
        masked = {
            k: ["***REDACTED***"] if k.lower() in _SECRET_QUERY_KEYS else v for k, v in qs.items()
        }
        new_query = urlencode(masked, doseq=True, quote_via=_quote_via)
        return urlunparse(parsed._replace(query=new_query))
    except Exception:
        return url
