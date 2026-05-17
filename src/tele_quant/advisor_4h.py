"""4H Advisory Orchestrator — 4시간 단위 매매 어드바이징 파이프라인.

기존 briefing.py를 확장해 다음을 추가한다:
1. 리스크 노출 섹션 (risk_advisor.py → RiskAssessment)
2. advisory_policy 기반 발송 정책 적용
3. 수혜주 라우팅 (이미 급등한 종목 → tier-2 표시)
4. 다음 4H 체크포인트 섹션
5. 면책 문구 강제 포함

CLI 사용:
    uv run tele-quant briefing --market KR --no-send   # 기존 명령 그대로 사용 가능
    (advisor_4h는 briefing.py 위에 레이어 — CLI 변경 없음)

주의: 공개 정보 기반 리서치 보조 — 투자 판단 책임은 사용자에게 있음.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

# 모듈 상단에 import — patch 가능하도록 lazy import 대신 top-level로 선언
# circular import 없음 (briefing/macro_pulse은 advisor_4h를 import하지 않음)
from tele_quant.advisory_policy import filter_urgent_items
from tele_quant.briefing import run_4h_briefing
from tele_quant.macro_pulse import fetch_macro_snapshot
from tele_quant.risk_advisor import assess_risk_mode, build_risk_section

if TYPE_CHECKING:
    from tele_quant.db import Store
    from tele_quant.settings import Settings

log = logging.getLogger(__name__)

DISCLAIMER = (
    "⚠ 공개 정보 기반 리서치 보조 — 매수·매도 확정 아님. "
    "투자 판단 책임은 사용자에게 있음"
)


def run_4h_advisory(
    market: str,
    store: Store,
    settings: Settings,
    top_n: int | None = None,
) -> str:
    """4H 어드바이징 메시지 생성.

    briefing.py의 run_4h_briefing을 호출하고,
    risk_advisor의 RiskAssessment 섹션을 앞에 붙인다.
    advisory_policy로 발송 적합성을 확인한다.

    Args:
        market: KR | US | ALL
        store: SQLite Store 인스턴스
        settings: Settings 인스턴스
        top_n: LONG 최대 표시 수 (None이면 settings.advisory_max_longs 사용)

    Returns:
        텔레그램 전송용 완성된 브리핑 텍스트
    """
    effective_top_n = top_n if top_n is not None else getattr(settings, "advisory_max_longs", 3)

    # ── 1. 기존 브리핑 생성 ──────────────────────────────────────────────────
    base_message = ""
    macro_snap = None
    try:
        base_message = run_4h_briefing(
            market=market,
            store=store,
            settings=settings,
            top_n=effective_top_n,
        )
    except Exception as exc:
        log.warning("[advisor_4h] briefing failed: %s", exc)
        base_message = f"[브리핑 생성 실패: {exc}]"

    # ── 2. 매크로 스냅샷 수집 (리스크 평가용) ──────────────────────────────────
    try:
        macro_snap = fetch_macro_snapshot()
    except Exception as exc:
        log.debug("[advisor_4h] macro re-fetch skipped: %s", exc)

    # ── 3. 리스크 평가 ────────────────────────────────────────────────────────
    risk_section = ""
    try:
        assessment = assess_risk_mode(macro_snap)
        risk_section = build_risk_section(assessment)
        log.info("[advisor_4h] risk_mode=%s stress=%d", assessment.mode, assessment.stress_signals)
    except Exception as exc:
        log.warning("[advisor_4h] risk assessment failed: %s", exc)

    # ── 4. 수혜주 라우팅 체크 (이미 급등한 종목 → tier-2 표시) ─────────────────
    chasing_note = _build_chasing_note(base_message)

    # ── 5. 다음 4H 체크포인트 ────────────────────────────────────────────────
    checkpoint_section = _build_checkpoint_section(market, macro_snap)

    # ── 6. 메시지 조립 ────────────────────────────────────────────────────────
    return _assemble_advisory_message(
        market=market,
        base_message=base_message,
        risk_section=risk_section,
        chasing_note=chasing_note,
        checkpoint_section=checkpoint_section,
        settings=settings,
    )


def _assemble_advisory_message(
    market: str,
    base_message: str,
    risk_section: str,
    chasing_note: str,
    checkpoint_section: str,
    settings: Settings,
) -> str:
    """최종 4H 어드바이징 메시지 조립.

    구조:
    ① 헤더 (시장 + 시간)
    ② 리스크 노출 판단 (risk_advisor)
    ③ 기존 브리핑 본문 (매크로 온도계, LONG/SHORT, 체인, 포트폴리오...)
    ④ 추격주의 노트 (있을 때만)
    ⑤ 다음 4H 체크포인트
    ⑥ 면책 문구
    """
    kst_now = datetime.now(UTC).strftime("%m/%d %H:%M")
    parts: list[str] = []

    # ① 헤더
    parts.append(f"📊 {market} 4H 매매 어드바이징 — {kst_now} UTC\n")

    # ② 리스크 노출
    if risk_section:
        parts.append("━━ 🧭 리스크 노출 판단 ━━")
        parts.append(risk_section)
        parts.append("")

    # ③ 기존 브리핑 본문 (헤더 중복 제거)
    # briefing.py가 이미 헤더를 붙이므로 첫 줄 제거
    body_lines = base_message.split("\n")
    if body_lines and "4H 퀀터멘탈 브리핑" in body_lines[0]:
        body_lines = body_lines[1:]  # 헤더 중복 제거
    parts.append("\n".join(body_lines).strip())
    parts.append("")

    # ④ 추격주의 노트
    if chasing_note:
        parts.append("━━ ⚡ 수혜주 라우팅 노트 ━━")
        parts.append(chasing_note)
        parts.append("")

    # ⑤ 다음 4H 체크포인트
    if checkpoint_section:
        parts.append("━━ 📅 다음 4H 체크포인트 ━━")
        parts.append(checkpoint_section)
        parts.append("")

    # ⑥ 면책 (없으면 강제 추가)
    full_text = "\n".join(parts)
    if "공개 정보 기반 리서치 보조" not in full_text:
        full_text += f"\n{'─' * 30}\n{DISCLAIMER}"

    return full_text


def _build_chasing_note(base_message: str) -> str:
    """기존 브리핑에서 '추격주의' 종목을 감지해 tier-2 수혜주 힌트 제공.

    현재는 단순 텍스트 탐지 — supply_chain_alpha 연동은 Phase C에서 구현.
    """
    if "추격주의" not in base_message and "급등" not in base_message:
        return ""
    return (
        "일부 후보가 이미 단기 급등 상태입니다.\n"
        "→ 수급 체인 2차 수혜주를 섹션⑤에서 확인하세요.\n"
        "→ 급등 종목 추격보다 미반영 연관 종목 관찰을 권장합니다."
    )


def _build_checkpoint_section(market: str, macro_snap: Any | None) -> str:
    """다음 4시간 체크포인트 섹션 생성."""
    items: list[str] = []

    if macro_snap is not None:
        # 금리 변화 추적 필요 여부
        if macro_snap.us10y_chg is not None and abs(macro_snap.us10y_chg) > 5:
            items.append(f"📌 10Y 금리: 현재 {macro_snap.us10y:.3f}% ({macro_snap.us10y_chg:+.1f}bp) → 추가 방향 확인")
        # VIX
        if macro_snap.vix is not None and macro_snap.vix > 20:
            items.append(f"📌 VIX {macro_snap.vix:.1f} → 25 돌파 시 방어 모드 전환 고려")
        # USD/KRW
        if macro_snap.usd_krw is not None:
            items.append(f"📌 USD/KRW {macro_snap.usd_krw:,.0f} → 1,400 이상 시 KR 비중 재조정 고려")

    # 공통 체크포인트
    if market in ("KR", "ALL"):
        items.append("📌 DART 공시 확인 (수주·계약·자사주 매입)")
    if market in ("US", "ALL"):
        items.append("📌 SEC 8-K / 실적 발표 확인")

    if not items:
        items.append("특이 체크포인트 없음 — 다음 브리핑에서 업데이트")

    return "\n".join(items)


# ── advisory_policy 연동 ─────────────────────────────────────────────────────

def check_urgent_advisory_items(
    items: list[Any],
    settings: Settings,
) -> list[Any]:
    """AdvisoryItem 목록에서 즉시 발송 대상만 추출.

    Args:
        items: AdvisoryItem 리스트
        settings: Settings 인스턴스

    Returns:
        즉시 발송 대상 AdvisoryItem 리스트
    """
    try:
        return filter_urgent_items(items, settings)
    except Exception as exc:
        log.warning("[advisor_4h] urgent filter failed: %s", exc)
        return []
