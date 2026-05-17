"""Advisory Policy — 알림 발송 중앙 정책 모듈.

모든 알림 항목(Surge, Price Alert, Daily Alpha, 브리핑)의
발송 기준을 이 파일 하나에서 관리한다.

설계 원칙:
- score >= 90 + direct_evidence → 즉시 발송 (URGENT)
- score >= 70 → 4H 브리핑 포함 (ACTION / WATCH)
- 나머지 → 무시 또는 지연

주의: 공개 정보 기반 리서치 보조. 투자 판단 책임은 사용자에게 있음.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tele_quant.settings import Settings

log = logging.getLogger(__name__)


# ── 심각도 레벨 ───────────────────────────────────────────────────────────────

class AdvisorySeverity(StrEnum):
    """알림 심각도 레벨."""

    INFO = "INFO"       # 참고용 정보 — 발송 안 함
    WATCH = "WATCH"     # 관찰 필요 — 4H 브리핑 포함 (score >= 70)
    ACTION = "ACTION"   # 행동 필요 — 4H 브리핑 상단 (score >= 80)
    URGENT = "URGENT"   # 긴급 — 즉시 발송 (score >= 90 + direct_evidence)


# ── 알림 항목 ─────────────────────────────────────────────────────────────────

@dataclass
class AdvisoryItem:
    """단일 알림 항목.

    source: 출처 (surge_alert / price_alert / daily_alpha / macro_pulse 등)
    market: KR | US | GLOBAL
    symbol: 종목 티커 (매크로 이벤트면 빈 문자열 허용)
    title: 제목 (1줄)
    severity: AdvisorySeverity
    score: 0~100 점수
    reason: 핵심 근거 (2줄 이하 권장)
    action: 행동 힌트 ("진입 검토 후보", "무효화 관찰" 등 — 매수/매도 확정 표현 금지)
    dedupe_key: 중복 방지 키 (같은 키는 최신 1건만 유지)
    direct_evidence: 직접 증거 있음 여부 (DART 공시, SEC 8-K 원문 등)
    price_unreacted: 주가 미반영 여부 추정
    chasing_risk: 이미 많이 올라 추격 위험 있음
    """

    source: str
    market: str
    symbol: str
    title: str
    severity: AdvisorySeverity
    score: float
    reason: str
    action: str
    dedupe_key: str
    direct_evidence: bool = False
    price_unreacted: bool = False
    chasing_risk: bool = False
    extra: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        # 금지 표현 자동 치환
        _FORBIDDEN = [
            "매수 권장", "매도 권장", "확정 수익", "자동매매",
            "실계좌", "무조건 상승", "수익 보장",
        ]
        for phrase in _FORBIDDEN:
            if phrase in self.title:
                log.warning("[advisory] 금지 표현 감지: '%s' in title", phrase)
                self.title = self.title.replace(phrase, "[리서치 보조]")
            if phrase in self.action:
                self.action = self.action.replace(phrase, "[리서치 보조]")

    @classmethod
    def make_dedupe_key(cls, source: str, symbol: str, title: str) -> str:
        """source + symbol + title(앞 40자)로 dedupe 키 생성."""
        raw = f"{source}:{symbol}:{title[:40]}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]


# ── 발송 정책 함수 ────────────────────────────────────────────────────────────

def should_send_immediately(item: AdvisoryItem, settings: Settings) -> bool:
    """즉시 발송 여부 결정.

    조건:
    1. score >= urgent_alert_min_score (기본 90.0)
    2. direct_evidence = True (공시/8-K 직접 증거 있음)
    3. advisory_only_mode가 False면 모든 URGENT 즉시 발송
    """
    if not getattr(settings, "advisory_only_mode", True):
        # advisory_only_mode=False 이면 심각도만 체크
        return item.severity == AdvisorySeverity.URGENT

    threshold = getattr(settings, "urgent_alert_min_score", 90.0)
    return item.score >= threshold and item.direct_evidence


def should_include_in_4h(item: AdvisoryItem, settings: Settings) -> bool:
    """4H 브리핑 포함 여부 결정.

    조건:
    - score >= advisory_min_score (기본 70.0)
    - 즉시 발송 대상이 아닌 경우
    """
    min_score = getattr(settings, "advisory_min_score", 70.0)
    return item.score >= min_score and not should_send_immediately(item, settings)


def classify_severity(score: float, direct_evidence: bool = False) -> AdvisorySeverity:
    """점수 → 심각도 자동 분류."""
    if score >= 90 and direct_evidence:
        return AdvisorySeverity.URGENT
    if score >= 80:
        return AdvisorySeverity.ACTION
    if score >= 70:
        return AdvisorySeverity.WATCH
    return AdvisorySeverity.INFO


# ── 알림 필터·정렬 ────────────────────────────────────────────────────────────

def filter_4h_items(
    items: list[AdvisoryItem],
    settings: Settings,
) -> list[AdvisoryItem]:
    """4H 브리핑에 포함할 항목 필터·정렬.

    - should_include_in_4h() 통과한 것만
    - score 내림차순 정렬
    - max_longs / max_shorts / max_watch 한도 적용
    """
    eligible = [it for it in items if should_include_in_4h(it, settings)]
    eligible.sort(key=lambda x: x.score, reverse=True)
    return eligible


def filter_urgent_items(
    items: list[AdvisoryItem],
    settings: Settings,
) -> list[AdvisoryItem]:
    """즉시 발송 대상 항목 필터."""
    return [it for it in items if should_send_immediately(it, settings)]


def dedupe_items(items: list[AdvisoryItem]) -> list[AdvisoryItem]:
    """같은 dedupe_key 중 score 가장 높은 것만 남긴다."""
    seen: dict[str, AdvisoryItem] = {}
    for item in items:
        key = item.dedupe_key
        if key not in seen or item.score > seen[key].score:
            seen[key] = item
    return list(seen.values())


# ── 텔레그램 출력 포맷 ────────────────────────────────────────────────────────

DISCLAIMER = (
    "⚠ 공개 정보 기반 리서치 보조 — 매수·매도 확정 아님. "
    "투자 판단 책임은 사용자에게 있음"
)


def format_advisory_item(item: AdvisoryItem, index: int = 0) -> str:
    """단일 AdvisoryItem을 텔레그램 텍스트 줄로 변환."""
    prefix = f"{index}. " if index > 0 else ""
    chasing = "⚡추격주의" if item.chasing_risk else ""
    unreacted = "🔍미반영" if item.price_unreacted else ""
    flags = " ".join(f for f in [chasing, unreacted] if f)
    sym = item.symbol.replace(".KS", "").replace(".KQ", "") if item.symbol else ""
    score_bar = "★" * min(5, int(item.score // 20))

    lines = [
        f"{prefix}{item.title} ({sym}) {score_bar} {item.score:.0f}점 {flags}".strip(),
        f"   근거: {item.reason[:100]}",
    ]
    if item.action:
        lines.append(f"   힌트: {item.action[:60]}")
    return "\n".join(lines)
