from __future__ import annotations

import re

from tele_quant.analysis.models import TradeScenario

_RSI_RE = re.compile(r"RSI\w*:\s*([\d.]+)")

_CONFIDENCE_KR = {"high": "높음", "medium": "보통", "low": "낮음"}

# RSI 과열 기준
_RSI_CHASE_WARN = 85.0
_RSI_CHASE_HARD = 90.0


def _fmt_list(items: list[str], prefix: str = "-") -> list[str]:
    return [f"{prefix} {item}" for item in items if item.strip()]


def _score_line(s: TradeScenario) -> str:
    """점수: 82/100 형태 + 구성 줄."""
    parts: list[str] = []
    if s.evidence_score > 0:
        parts.append(f"증거 {s.evidence_score:.0f}")
    if s.technical_score > 0:
        parts.append(f"기술 {s.technical_score:.0f}")
    if s.valuation_score > 0:
        parts.append(f"가치 {s.valuation_score:.0f}")
    if s.macro_risk_score > 0:
        parts.append(f"리스크 {s.macro_risk_score:.0f}")
    if s.timing_score > 0:
        parts.append(f"타이밍 {s.timing_score:.0f}")
    comp = " / ".join(parts)
    return f"점수: {s.score:.0f}/100  구성: {comp}" if comp else f"점수: {s.score:.0f}/100"


def _is_chasing(s: TradeScenario) -> bool:
    """RSI 과열 여부 판단."""
    for line in s.chart_summary.splitlines():
        if "RSI14:" in line:
            try:
                val = float(line.split("RSI14:")[-1].strip())
                return val >= _RSI_CHASE_WARN
            except ValueError:
                pass
    return False


def _rsi_from_chart(s: TradeScenario) -> float | None:
    for line in s.chart_summary.splitlines():
        if "RSI14:" in line:
            try:
                return float(line.split("RSI14:")[-1].strip())
            except ValueError:
                pass
    return None


def _scenario_block_core(s: TradeScenario, idx: int) -> list[str]:
    """공통 블록: 이름·점수·근거·기술·가치."""
    star = "⭐ " if s.is_watchlist else ""
    name_part = f"{s.name} / {s.symbol}" if s.name else s.symbol
    lines: list[str] = [f"{idx}. {star}{name_part}"]

    if s.is_watchlist and s.watchlist_group:
        lines.append(f"   [관심종목: {s.watchlist_group}] 감시 대상")

    lines.append(f"   {_score_line(s)}")
    lines.append(f"   신뢰도: {_CONFIDENCE_KR.get(s.confidence, s.confidence)}")

    up = [r for r in s.reasons_up if r.strip()]
    if up:
        lines.append(f"   근거: {up[0]}")
        if len(up) > 1:
            lines.append(f"         {up[1]}")

    # 검증 줄
    if s.verify_summary:
        lines.append(f"   검증: {s.verify_summary}")
    else:
        lines.append("   검증: yfinance 기준 가격·거래량만 확인")

    # 기술적 지표
    if s.chart_summary:
        lines.append("   기술:")
        for line in s.chart_summary.splitlines():
            if line.strip():
                lines.append(f"   {line}")

    # 4H 단기 기술 (intraday)
    if s.intraday_4h_summary:
        lines.append("   4H 단기:")
        for line in s.intraday_4h_summary.splitlines():
            if line.strip():
                lines.append(f"   {line}")

    # 재무
    if s.fundamental_summary and s.fundamental_summary not in ("재무 데이터 없음", "데이터 부족"):
        lines.append(f"   가치: {s.fundamental_summary}")

    # 초보자 친화 설명 (Ollama 생성, 있으면 beginner_hint 대체)
    if s.plain_summary:
        lines.append("   📖 쉬운 설명:")
        for pl in s.plain_summary.splitlines():
            if pl.strip():
                lines.append(f"   {pl.strip()}")
    elif s.beginner_hint:
        lines.append(f"   💡 {s.beginner_hint}")

    # 연구DB 보조근거 (relation feed lead-lag)
    if s.relation_feed_note:
        lines.append(f"   연구DB 보조근거: {s.relation_feed_note}")

    return lines


def _long_block(s: TradeScenario, idx: int) -> list[str]:
    lines = _scenario_block_core(s, idx)

    entry = (
        s.entry_zone
        if s.entry_zone and s.entry_zone != "데이터 부족"
        else "SMA20 또는 볼린저 중단 부근"
    )
    lines.append("   관심 진입:")
    lines.append(f"   - 눌림형: {entry} 구간에서 지지 확인")
    lines.append("   - 돌파형: 최근 고점 돌파 + 거래량 1.5배 이상 확인")
    lines.append("   - 초보자 해석: 지금 추격보다 눌림/거래량 확인 후 접근")

    stop = (
        s.invalidation
        if s.invalidation and s.invalidation != "데이터 부족"
        else "볼린저 하단 이탈 후 회복 실패"
    )
    lines.append("   손절·무효화:")
    lines.append(f"   - {stop} 종가 이탈 시 시나리오 약화")
    lines.append("   - 장대음봉 + 거래량 증가 시 리스크 관리")

    target = (
        s.take_profit if s.take_profit and s.take_profit != "데이터 부족" else "최근 20일 고점/저항"
    )
    lines.append("   목표/매도 관찰:")
    lines.append(f"   - 1차: {target} 저항")
    lines.append("   - 2차: 볼린저 상단/직전 매물대")
    lines.append("   - RSI 75 이상이면 일부 차익 관찰")

    if s.risk_notes:
        lines.append(f"   리스크: {s.risk_notes[0]}")

    return lines


def _short_block(s: TradeScenario, idx: int) -> list[str]:
    lines = _scenario_block_core(s, idx)

    dn = [r for r in s.reasons_down if r.strip()]
    if dn:
        lines.append(f"   악재 근거: {dn[0]}")

    cond = "추세 하락 + OBV 감소 확인 후 관심"
    lines.append(f"   숏 관심 조건: {cond}")

    stop = (
        s.invalidation
        if s.invalidation and s.invalidation != "데이터 부족"
        else "추세 반전 + 거래량 증가"
    )
    lines.append(f"   무효화: {stop} 또는 악재 해소 시")

    target = s.take_profit if s.take_profit and s.take_profit != "데이터 부족" else "직전 지지 구간"
    lines.append(f"   하락 목표/지지: {target}")

    lines.append("   반등 리스크: 악재 과도 반영 시 단기 되돌림 주의")

    return lines


def _watch_block(s: TradeScenario, idx: int) -> list[str]:
    star = "⭐ " if s.is_watchlist else ""
    name_part = f"{s.name} / {s.symbol}" if s.name else s.symbol
    note = (
        s.technical_summary
        if s.technical_summary != "기술적 데이터 없음"
        else "기술적 위치 확인 필요"
    )
    lines = [f"{idx}. {star}{name_part} — {note}"]
    if s.is_watchlist:
        lines.append(f"   ↳ 관심종목 감시 대상 (점수: {s.score:.0f})")
    return lines


def _chart_rsi(s: TradeScenario) -> float | None:
    for line in s.chart_summary.splitlines():
        m = _RSI_RE.search(line)
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                pass
    return None


def _intraday_rsi(s: TradeScenario) -> float | None:
    for line in s.intraday_4h_summary.splitlines():
        m = _RSI_RE.search(line)
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                pass
    return None


def _watchlist_section(
    watchlist_longs: list[TradeScenario], watchlist_others: list[TradeScenario]
) -> list[str]:
    """관심종목 우선 체크 섹션 (RSI 과열/약화 신호 포함)."""
    lines: list[str] = ["⭐ 내 관심종목 우선 체크", ""]
    if not watchlist_longs and not watchlist_others:
        lines.append("- 이번 리포트 기간 관심종목 언급 없음")
    else:
        for s in watchlist_longs:
            name_part = f"{s.name} / {s.symbol}" if s.name else s.symbol
            rsi = _chart_rsi(s)
            rsi4h = _intraday_rsi(s)
            tags: list[str] = []
            if rsi is not None and rsi >= 85:
                tags.append(f"과열 주의 RSI {rsi:.0f}")
            elif rsi4h is not None and rsi4h <= 30:
                tags.append(f"4H RSI {rsi4h:.0f} 약세")
            elif s.reasons_up:
                tags.append(f"새로 강해짐: {s.reasons_up[0][:40]}")
            tag_str = f" [{', '.join(tags)}]" if tags else f" ({s.grade})"
            lines.append(f"- {name_part}: 호재{tag_str}")
        for s in watchlist_others:
            name_part = f"{s.name} / {s.symbol}" if s.name else s.symbol
            rsi = _chart_rsi(s)
            rsi4h = _intraday_rsi(s)
            mood = "악재" if s.direction == "bearish" else "혼조"
            tags = []
            if rsi is not None and rsi >= 85:
                tags.append(f"과열 주의 RSI {rsi:.0f}")
            elif rsi4h is not None and rsi4h <= 30:
                tags.append(f"4H RSI {rsi4h:.0f} 약화")
            tag_str = f" [{', '.join(tags)}]" if tags else f" ({s.grade})"
            lines.append(f"- {name_part}: {mood}{tag_str}")
    lines.append("")
    return lines


def _chasing_block(chase_list: list[TradeScenario]) -> list[str]:
    """🚫 추격주의 섹션."""
    lines: list[str] = ["🚫 추격주의", ""]
    for s in chase_list:
        rsi = _rsi_from_chart(s)
        name_part = f"{s.name} / {s.symbol}" if s.name else s.symbol
        reasons: list[str] = []
        if rsi is not None and rsi >= _RSI_CHASE_HARD:
            reasons.append(f"RSI {rsi:.0f} — 강한 과열, 신규 진입 보수적")
        elif rsi is not None and rsi >= _RSI_CHASE_WARN:
            reasons.append(f"RSI {rsi:.0f} — 신규 진입보다 눌림 대기")
        for line in s.chart_summary.splitlines():
            if "볼린저: 상단돌파" in line:
                reasons.append("볼린저 상단돌파")
            if "거래량:" in line:
                try:
                    parts = line.split("대비")
                    if len(parts) > 1 and float(parts[1].replace("배", "").strip()) > 2.0:
                        reasons.append("거래량 과열")
                except (ValueError, IndexError):
                    pass
        reason_str = " / ".join(reasons) if reasons else "단기 급등 과열"
        lines.append(f"- {name_part}: {reason_str}")
    lines.append("")
    return lines


def _compact_tech_summary(s: TradeScenario) -> str:
    """Parse chart_summary + intraday_4h_summary into one compact line."""
    parts: list[str] = []
    # Prefer 4H intraday if available
    source = s.intraday_4h_summary if s.intraday_4h_summary else s.chart_summary
    for line in source.splitlines():
        stripped = line.strip()
        if "RSI" in stripped:
            val = stripped.replace("RSI14:", "RSI").replace("- RSI:", "RSI").strip("- ")
            parts.append(val)
        elif "OBV:" in stripped:
            parts.append(stripped.strip("- "))
        elif "거래량:" in stripped:
            v = stripped.replace("거래량: 20일 평균 대비 ", "거래량 ").strip("- ")
            parts.append(v)
    if not parts and s.technical_summary and s.technical_summary != "기술적 데이터 없음":
        return s.technical_summary[:60]
    return " / ".join(parts) if parts else ""


def _compact_chase_note(s: TradeScenario) -> str:
    rsi = _rsi_from_chart(s)
    if rsi is not None and rsi >= _RSI_CHASE_HARD:
        return f"RSI {rsi:.0f} 강한 과열 — 거래량 확인 전 추격 금지"
    if rsi is not None and rsi >= _RSI_CHASE_WARN:
        return f"RSI {rsi:.0f} 과열권 — 눌림 대기 후 접근"
    for line in s.chart_summary.splitlines():
        if "OBV: 하락" in line:
            return "OBV 하락 — 거래량 약화, 진입 신중"
    return "거래량 확인 후 접근"


def _compact_scenario_line(s: TradeScenario, idx: int, max_reasons: int = 2) -> list[str]:
    """압축 출력: 왜 / 확인 / 진입 전 조건 / 무효화 / 주의."""
    star = "⭐ " if s.is_watchlist else ""
    name_part = f"{s.name} / {s.symbol}" if s.name else s.symbol
    lines: list[str] = [f"{idx}. {star}{name_part} — {s.score:.0f}점"]

    # 왜
    reasons = [r for r in (s.reasons_up if s.side != "SHORT" else s.reasons_down) if r.strip()]
    if not reasons:
        reasons = [r for r in s.reasons_up if r.strip()]
    if reasons:
        reason_str = ", ".join(reasons[:max_reasons])
        lines.append(f"- 왜: {reason_str}")

    # 확인 (기술)
    tech = _compact_tech_summary(s)
    if tech:
        lines.append(f"- 확인: {tech}")

    # 진입 전 조건
    if s.entry_zone and s.entry_zone not in ("데이터 부족", "SMA20 또는 볼린저 중단 부근"):
        lines.append(f"- 진입 전 조건: {s.entry_zone}")

    # 무효화
    if s.invalidation and s.invalidation not in ("데이터 부족",):
        lines.append(f"- 무효화: {s.invalidation}")

    # 주의
    lines.append(f"- 주의: {_compact_chase_note(s)}")

    # 가치 (단일 줄)
    if s.fundamental_summary and s.fundamental_summary not in ("재무 데이터 없음", "데이터 부족"):
        lines.append(f"- 가치: {s.fundamental_summary}")

    if s.relation_feed_note:
        lines.append(f"- 연구DB: {s.relation_feed_note}")

    # 초보자 설명 (compact에서도 표시, 첫 줄만)
    if s.plain_summary:
        first_line = next(
            (ln.strip() for ln in s.plain_summary.splitlines() if ln.strip()), ""
        )
        if first_line:
            lines.append(f"- 📖 {first_line}")

    return lines


def format_analysis_report(
    scenarios: list[TradeScenario],
    compact: bool = False,
    compact_max_longs: int = 5,
    compact_max_shorts: int = 2,
    compact_max_watch: int = 8,
    compact_max_reasons: int = 2,
) -> str:
    if not scenarios:
        return ""

    long_list = [s for s in scenarios if s.side == "LONG"]
    short_list = [s for s in scenarios if s.side == "SHORT"]
    watch_list = [s for s in scenarios if s.side == "WATCH"]

    if compact:
        long_list = long_list[:compact_max_longs]
        short_list = short_list[:compact_max_shorts]
        watch_list = watch_list[:compact_max_watch]

    # watchlist 종목 분류
    wl_longs = [s for s in long_list if s.is_watchlist]
    wl_others = [s for s in (short_list + watch_list) if s.is_watchlist]

    # watchlist 종목 우선 정렬 (같은 점수면 watchlist 먼저)
    long_list_sorted = sorted(long_list, key=lambda s: -s.score - (5.0 if s.is_watchlist else 0.0))
    short_list_sorted = sorted(short_list, key=lambda s: -s.score)

    # 추격주의 목록 (compact 모드에서도 표시)
    chase_list = [s for s in scenarios if _is_chasing(s)]

    lines: list[str] = [
        "📊 Tele Quant 롱/숏 관심 시나리오",
        "",
    ]

    # ⭐ 내 관심종목 섹션
    if wl_longs or wl_others:
        lines.extend(_watchlist_section(wl_longs, wl_others))

    if long_list_sorted:
        lines.append("🟢 롱 관심 후보")
        for idx, s in enumerate(long_list_sorted, 1):
            if compact:
                lines.extend(_compact_scenario_line(s, idx, compact_max_reasons))
            else:
                lines.extend(_long_block(s, idx))
            lines.append("")

    if short_list_sorted:
        lines.append("🔴 숏/매도 경계 후보")
        for idx, s in enumerate(short_list_sorted, 1):
            if compact:
                lines.extend(_compact_scenario_line(s, idx, compact_max_reasons))
            else:
                lines.extend(_short_block(s, idx))
            lines.append("")

    if watch_list:
        lines.append("🟡 관망/추적")
        for idx, s in enumerate(watch_list, 1):
            if compact:
                lines.extend(_compact_scenario_line(s, idx, compact_max_reasons))
            else:
                lines.extend(_watch_block(s, idx))
        lines.append("")

    if chase_list:
        lines.extend(_chasing_block(chase_list))

    lines.append("─" * 30)
    lines.append("공개 정보 기반 개인 리서치 보조용이며 투자 판단 책임은 사용자에게 있음.")
    return "\n".join(lines).strip()
