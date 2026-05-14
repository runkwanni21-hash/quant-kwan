"""Alias quality audit — detects overly broad aliases that may cause false positives."""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)

# Common Korean/English words that are likely to appear in non-stock contexts.
_COMMON_WORDS: frozenset[str] = frozenset(
    {
        "대한",
        "한국",
        "코리아",
        "글로벌",
        "인터내셔널",
        "테크",
        "그룹",
        "홀딩스",
        "Inc",
        "Corp",
        "Ltd",
        "Co",
        "Group",
        "Holdings",
        "Capital",
        "Energy",
        "Tech",
        "Global",
        "Korea",
    }
)

# Single-char ASCII aliases are almost always false-positive risks
_SINGLE_CHAR_RE = __import__("re").compile(r"^[A-Za-z]$")


@dataclass
class AuditEntry:
    symbol: str
    name: str
    market: str
    alias: str
    issue: str
    severity: str  # "HIGH" | "MEDIUM" | "LOW"


def run_audit(yaml_path: Path | None = None) -> list[AuditEntry]:
    """Load alias book and return audit findings sorted by severity."""
    from tele_quant.analysis.aliases import MACRO_KEYWORDS, load_alias_config

    book = load_alias_config(yaml_path)
    entries: list[AuditEntry] = []

    for sym in book.all_symbols:
        for alias in sym.aliases:
            issues: list[tuple[str, str]] = []  # (issue_text, severity)

            # Single ASCII character — very high false-positive risk
            if _SINGLE_CHAR_RE.match(alias):
                issues.append(("단일 ASCII 문자 alias — 오탐 위험 최고", "HIGH"))

            # Macro keyword used as stock alias
            if alias in MACRO_KEYWORDS:
                issues.append((f"MACRO_KEYWORDS에 포함: '{alias}'", "HIGH"))

            # Very short non-Korean alias that is NOT auto-protected by _alias_requires_context.
            # 1-5자 uppercase ASCII is already auto-gated by the AliasBook matching logic,
            # so only flag aliases that slip through: lowercase or mixed-case short aliases.
            if (
                len(alias) <= 2
                and alias.isascii()
                and not (alias.isupper() and len(alias) >= 1)  # uppercase already auto-protected
                and alias not in sym.require_context_aliases
            ):
                issues.append((f"짧은 ASCII alias '{alias}' + 자동보호 없음 + require_context 없음", "MEDIUM"))

            # Common generic word alias
            if alias in _COMMON_WORDS:
                issues.append((f"일반 단어 '{alias}' alias — 다른 종목과 충돌 가능", "MEDIUM"))

            # Very long alias (>20 chars) — may be full sentence
            if len(alias) > 20:
                issues.append((f"alias 길이 {len(alias)}자 — 너무 길어 불필요", "LOW"))

            for issue, sev in issues:
                entries.append(
                    AuditEntry(
                        symbol=sym.symbol,
                        name=sym.name,
                        market=sym.market,
                        alias=alias,
                        issue=issue,
                        severity=sev,
                    )
                )

    # Sort HIGH → MEDIUM → LOW
    sev_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    entries.sort(key=lambda e: (sev_order.get(e.severity, 9), e.symbol))
    return entries


def save_audit_csv(entries: list[AuditEntry], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["severity", "symbol", "name", "market", "alias", "issue"],
        )
        writer.writeheader()
        for e in entries:
            writer.writerow(
                {
                    "severity": e.severity,
                    "symbol": e.symbol,
                    "name": e.name,
                    "market": e.market,
                    "alias": e.alias,
                    "issue": e.issue,
                }
            )


def audit_summary(entries: list[AuditEntry]) -> str:
    high = sum(1 for e in entries if e.severity == "HIGH")
    med = sum(1 for e in entries if e.severity == "MEDIUM")
    low = sum(1 for e in entries if e.severity == "LOW")
    total = len(entries)
    lines = [
        f"Alias Audit: {total}건 이슈 발견",
        f"  HIGH   {high:4d}건 — 즉시 검토 필요",
        f"  MEDIUM {med:4d}건 — 검토 권장",
        f"  LOW    {low:4d}건 — 낮은 우선순위",
    ]
    if high == 0 and med == 0:
        lines.append("  ✅ 심각한 이슈 없음")
    elif high > 0:
        lines.append("  ⚠️ HIGH 이슈를 먼저 처리하세요")
    return "\n".join(lines)
