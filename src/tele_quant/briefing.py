"""4H 퀀터멘탈 브리핑 — 매크로 + 종목 + 포트폴리오 통합 리포트.

개인투자자 전략:
1. 기관 사각지대(300B~10T KRW / $300M~$10B) 집중 — 유동성 부담으로 기관이 못 들어오는 구간
2. DART 신속 반응 — 기관(리서치팀 검토→컴플라이언스→승인) 대비 수 시간 선점
3. 집중 포트 6종목 — 기관은 리스크 분산 필수, 개인은 확신 종목에 집중 가능
4. 벤치마크 없는 절대수익 추구 — 지수 대비 성과 아닌 실제 수익률 최적화

주의: 공개 정보 기반 리서치 보조. 투자 판단 책임은 사용자에게 있음.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tele_quant.db import Store
    from tele_quant.settings import Settings

log = logging.getLogger(__name__)


def run_4h_briefing(
    market: str,
    store: Store,
    settings: Settings,
    top_n: int = 5,
) -> str:
    """4H 퀀터멘탈 브리핑 전체 파이프라인.

    1. 매크로 지표 수집
    2. Daily Alpha 스크리닝
    3. 모의 포트폴리오 청산 체크
    4. 80점 이상 신규 진입
    5. 브리핑 조립
    """
    # ── 1. 매크로 ────────────────────────────────────────────────────────────
    macro_snap = None
    try:
        from tele_quant.macro_pulse import fetch_macro_snapshot
        macro_snap = fetch_macro_snapshot()
        log.info("[briefing] macro fetched regime=%s", macro_snap.regime)
    except Exception as exc:
        log.warning("[briefing] macro fetch failed: %s", exc)

    # ── 2. Daily Alpha 스크리닝 ───────────────────────────────────────────────
    long_picks: list[Any] = []
    short_picks: list[Any] = []
    try:
        from tele_quant.daily_alpha import run_daily_alpha
        long_picks, short_picks = run_daily_alpha(market, store=store, top_n=top_n)
        log.info("[briefing] alpha LONG=%d SHORT=%d", len(long_picks), len(short_picks))
    except Exception as exc:
        log.warning("[briefing] daily_alpha failed: %s", exc)

    # ── 3. 펀더멘탈 강화 ─────────────────────────────────────────────────────
    fund_snaps: dict[str, Any] = {}
    all_picks = long_picks + short_picks
    if all_picks:
        try:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            from tele_quant.fundamentals import fetch_fundamentals
            with ThreadPoolExecutor(max_workers=8) as pool:
                futs = {
                    pool.submit(fetch_fundamentals, p.symbol, p.market, getattr(p, "sector", "")): p.symbol
                    for p in all_picks
                }
                for fut in as_completed(futs):
                    sym = futs[fut]
                    import contextlib
                    with contextlib.suppress(Exception):
                        fund_snaps[sym] = fut.result()
        except Exception as exc:
            log.warning("[briefing] fundamentals fetch failed: %s", exc)

    # ── 4. 모의 포트폴리오 청산 체크 ─────────────────────────────────────────
    closed_positions: list[Any] = []
    try:
        from tele_quant.mock_portfolio import check_exits
        closed_positions = check_exits(store)
        if closed_positions:
            log.info("[briefing] closed %d positions", len(closed_positions))
    except Exception as exc:
        log.warning("[briefing] exit check failed: %s", exc)

    # ── 5. 신규 진입 ─────────────────────────────────────────────────────────
    entered: list[str] = []
    try:
        from tele_quant.mock_portfolio import enter_position
        for pick in long_picks + short_picks:
            snap = fund_snaps.get(pick.symbol)
            if enter_position(store, pick, snap):
                entered.append(pick.symbol)
    except Exception as exc:
        log.warning("[briefing] enter position failed: %s", exc)

    # ── 6. 공급망 체인 신호 ───────────────────────────────────────────────────
    chain_section = ""
    try:
        from tele_quant.supply_chain_alpha import (
            build_spillover_section,
            detect_movers,
            find_spillover_targets,
            load_supply_chain_rules,
        )
        rules = load_supply_chain_rules()
        movers = detect_movers(market=market)
        if movers:
            longs, shorts = find_spillover_targets(movers, rules)
            chain_section = build_spillover_section(longs[:3], shorts[:2], movers[:3])
    except Exception as exc:
        log.debug("[briefing] chain section failed: %s", exc)

    # ── 7. 테마 보드 ─────────────────────────────────────────────────────────
    theme_section = ""
    try:
        from tele_quant.theme_board import build_theme_board
        theme_section = build_theme_board(market, store, settings)
        # 너무 길면 앞부분만
        lines = theme_section.split("\n")
        theme_section = "\n".join(lines[:30]) if len(lines) > 30 else theme_section
    except Exception as exc:
        log.debug("[briefing] theme board failed: %s", exc)

    # ── 8. 포트폴리오 섹션 ───────────────────────────────────────────────────
    portfolio_section = ""
    try:
        from tele_quant.mock_portfolio import build_portfolio_section
        portfolio_section = build_portfolio_section(store)
    except Exception as exc:
        log.warning("[briefing] portfolio section failed: %s", exc)

    # ── 9. 조립 ──────────────────────────────────────────────────────────────
    return build_briefing_message(
        market=market,
        macro_snap=macro_snap,
        long_picks=long_picks,
        short_picks=short_picks,
        fund_snaps=fund_snaps,
        portfolio_section=portfolio_section,
        chain_section=chain_section,
        theme_section=theme_section,
        top_n=top_n,
    )


def build_briefing_message(
    market: str,
    macro_snap: Any | None,
    long_picks: list[Any],
    short_picks: list[Any],
    fund_snaps: dict[str, Any],
    portfolio_section: str,
    chain_section: str = "",
    theme_section: str = "",
    top_n: int = 5,
) -> str:
    """최종 브리핑 메시지 조립."""
    from tele_quant.fundamentals import build_fundamental_line, get_edge_label

    kst_now = datetime.now(UTC).strftime("%m/%d %H:%M")
    lines: list[str] = [f"📊 {market} 4H 퀀터멘탈 브리핑 — {kst_now} UTC\n"]

    # ── 섹션1: 매크로 온도계 ─────────────────────────────────────────────────
    if macro_snap is not None:
        from tele_quant.macro_pulse import build_macro_section
        lines.append("━━ 💹 매크로 온도계 ━━")
        lines.append(build_macro_section(macro_snap))
        lines.append("")

    # ── 섹션2: 주도 테마 요약 (theme_board 압축) ─────────────────────────────
    if theme_section:
        lines.append("━━ 🏆 주도 섹터·테마 ━━")
        # 핵심 줄만 추출 (섹터 현황 이후 첫 10줄)
        tb_lines = [ln for ln in theme_section.split("\n") if ln.strip() and "━" not in ln]
        for tl in tb_lines[:8]:
            lines.append(tl)
        lines.append("")

    # ── 섹션3: LONG 관찰 후보 ────────────────────────────────────────────────
    if long_picks:
        lines.append(f"━━ 📈 LONG 관찰 후보 Top {min(top_n, len(long_picks))} ━━")
        lines.append("(공개 정보 기반 리서치 보조 — 실제 매수 권장 아님)")
        for i, pick in enumerate(long_picks[:top_n], 1):
            snap = fund_snaps.get(pick.symbol)
            edge = get_edge_label(snap) if snap else ""
            fund_line = build_fundamental_line(snap) if snap else "재무데이터 없음"
            score_stars = "★" * min(5, int(pick.final_score // 20))

            name = pick.name or pick.symbol
            sym_short = pick.symbol.replace(".KS", "").replace(".KQ", "")
            lines.append(
                f"{i}. {name}({sym_short}) {score_stars} {pick.final_score:.0f}점"
                + (f"  {edge}" if edge else "")
            )
            lines.append(f"   {fund_line}")

            # 진입·무효화·목표
            entry = _format_price_zone(pick.entry_zone, pick.market)
            inval = _format_price(pick.invalidation_level, pick.market)
            target = _format_price(pick.target_zone, pick.market)
            if entry or inval or target:
                price_line = []
                if entry:
                    price_line.append(f"진입 {entry}")
                if inval:
                    price_line.append(f"무효화 {inval}")
                if target:
                    price_line.append(f"목표 {target}")
                lines.append("   " + " | ".join(price_line))

            # 핵심 근거 한 줄
            reason = (
                getattr(pick, "catalyst_reason", "")
                or getattr(pick, "technical_reason", "")
                or getattr(pick, "valuation_reason", "")
                or ""
            )
            if reason:
                lines.append(f"   근거: {reason[:90]}")
        lines.append("")

    # ── 섹션4: SHORT 관찰 후보 ───────────────────────────────────────────────
    if short_picks:
        lines.append(f"━━ 📉 SHORT 관찰 후보 Top {min(3, len(short_picks))} ━━")
        for i, pick in enumerate(short_picks[:3], 1):
            snap = fund_snaps.get(pick.symbol)
            fund_line = build_fundamental_line(snap) if snap else ""
            name = pick.name or pick.symbol
            sym_short = pick.symbol.replace(".KS", "").replace(".KQ", "")
            lines.append(
                f"{i}. {name}({sym_short}) {pick.final_score:.0f}점"
            )
            if fund_line:
                lines.append(f"   {fund_line}")
            reason = (
                getattr(pick, "catalyst_reason", "")
                or getattr(pick, "technical_reason", "")
                or ""
            )
            if reason:
                lines.append(f"   {reason[:90]}")
        lines.append("")

    # ── 섹션5: 수혜주·피해주 체인 ───────────────────────────────────────────
    if chain_section:
        lines.append("━━ 🔗 수혜주·피해주 체인 ━━")
        for cl in chain_section.split("\n")[:12]:
            if cl.strip():
                lines.append(cl)
        lines.append("")

    # ── 섹션6: 모의 포트폴리오 ───────────────────────────────────────────────
    if portfolio_section:
        lines.append("━━ 💼 모의 포트폴리오 ━━")
        lines.append(portfolio_section)
        lines.append("")

    # ── 섹션7: 개인투자자 강점 힌트 ─────────────────────────────────────────
    edge_picks = [
        (p, fund_snaps.get(p.symbol))
        for p in (long_picks + short_picks)
        if fund_snaps.get(p.symbol) and fund_snaps[p.symbol].is_blind_spot
    ]
    if edge_picks:
        lines.append("━━ ⚡ 개인투자자 전략 힌트 ━━")
        lines.append("기관이 진입 어려운 구간 (시총 300B~10T KRW / $300M~$10B):")
        for pick, snap in edge_picks[:3]:
            sym_short = pick.symbol.replace(".KS", "").replace(".KQ", "")
            mc_str = _format_market_cap(snap)
            lines.append(
                f"  🎯 {pick.name}({sym_short}) {mc_str} | {pick.final_score:.0f}점 [{pick.side}]"
            )
        lines.append("  → 집중 포트 최대 6종목, 벤치마크 없는 절대수익 추구")
        lines.append("")

    # ── 면책 ─────────────────────────────────────────────────────────────────
    lines.append("─" * 30)
    lines.append("⚠ 공개 정보 기반 리서치 보조 — 매수·매도 확정 아님. 투자 판단 책임은 사용자에게 있음")

    return "\n".join(lines)


# ── Price format helpers ──────────────────────────────────────────────────────

def _format_price(raw: str | float | None, market: str) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s or s in ("None", "nan", ""):
        return ""
    # 숫자만 추출
    import re
    nums = re.findall(r"[\d,]+(?:\.\d+)?", s)
    if not nums:
        return s[:20]
    try:
        v = float(nums[0].replace(",", ""))
        if market == "KR":
            return f"{v:,.0f}원"
        return f"${v:,.2f}"
    except Exception:
        return s[:20]


def _format_price_zone(raw: str | None, market: str) -> str:
    if not raw or str(raw).strip() in ("None", ""):
        return ""
    return raw[:30]


def _format_market_cap(snap: Any) -> str:
    if snap.market_cap_krw:
        t = snap.market_cap_krw / 1_000_000_000_000
        return f"시총 {t:.1f}조" if t >= 1 else f"시총 {snap.market_cap_krw/100_000_000:.0f}억"
    if snap.market_cap_usd:
        b = snap.market_cap_usd / 1_000_000_000
        return f"MC ${b:.1f}B"
    return ""
