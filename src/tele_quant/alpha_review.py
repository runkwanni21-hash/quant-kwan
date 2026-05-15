"""Alpha Review — 장 마감 후 당일/최근 N일 추천 종목 성과 자동 요약.

daily-alpha 추천 후 실제 가격 움직임을 추적해
주간 리포트 전에 중간 피드백을 텔레그램으로 전송한다.

주의: 매수·매도 확정 표현 금지. 성과 참고용 요약이며 실제 투자 판단은 사용자 책임.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tele_quant.db import Store

log = logging.getLogger(__name__)

_REVIEW_LOOKBACK_DAYS = 5


def _fetch_prices(symbols: list[str]) -> dict[str, float]:
    try:
        import yfinance as yf
        if not symbols:
            return {}
        tickers = yf.Tickers(" ".join(symbols))
        result: dict[str, float] = {}
        for sym in symbols:
            try:
                info = tickers.tickers[sym].fast_info
                price = getattr(info, "last_price", None) or getattr(info, "previous_close", None)
                if price and float(price) > 0:
                    result[sym] = float(price)
            except Exception:
                pass
        return result
    except Exception as exc:
        log.warning("[alpha-review] 가격 조회 실패: %s", exc)
        return {}


def build_alpha_review(
    store: Store,
    market: str,
    days_back: int = 1,
) -> str:
    """최근 days_back일 추천 종목의 현재 성과 요약 문자열 반환."""
    from zoneinfo import ZoneInfo

    KST = ZoneInfo("Asia/Seoul")
    now_kst = datetime.now(KST)
    since = datetime.now(UTC) - timedelta(days=days_back)

    picks = store.recent_daily_alpha_picks(since=since, market=market)
    picks = [p for p in picks if p.get("signal_price")]

    if not picks:
        return ""

    symbols = list({p["symbol"] for p in picks})
    prices = _fetch_prices(symbols)

    rows_long: list[tuple[dict, float, float]] = []
    rows_short: list[tuple[dict, float, float]] = []

    for pick in picks:
        current = prices.get(pick["symbol"])
        if current is None or not pick.get("signal_price"):
            continue
        sig = float(pick["signal_price"])
        raw_ret = (current - sig) / sig * 100
        # SHORT은 하락이 수익
        eff_ret = -raw_ret if pick["side"] == "SHORT" else raw_ret
        if pick["side"] == "LONG":
            rows_long.append((pick, current, eff_ret))
        else:
            rows_short.append((pick, current, eff_ret))

    if not rows_long and not rows_short:
        return ""

    is_kr = market == "KR"

    def fmt_price(v: float) -> str:
        return f"{v:,.0f}원" if is_kr else f"${v:.2f}"

    def fmt_ret(r: float) -> str:
        arrow = "▲" if r > 0 else "▼"
        return f"{arrow}{abs(r):.1f}%"

    period_label = "당일" if days_back <= 1 else f"최근 {days_back}일"
    label = "KR 한국장" if market == "KR" else "US 미국장"
    lines: list[str] = [
        f"📋 Alpha 중간 성과 ({label} {period_label}) — {now_kst.strftime('%m/%d %H:%M KST')}",
        "",
    ]

    all_rets = [r for _, _, r in rows_long + rows_short]
    win = sum(1 for r in all_rets if r > 0)
    lose = sum(1 for r in all_rets if r <= 0)
    avg = sum(all_rets) / len(all_rets) if all_rets else 0
    lines.append(f"  총 {len(all_rets)}건  승 {win} / 패 {lose}  평균 {avg:+.1f}%")
    lines.append("")

    if rows_long:
        lines.append("🟢 LONG 추천 성과")
        for pick, curr, eff_ret in sorted(rows_long, key=lambda x: -x[2]):
            emoji = "✅" if eff_ret > 0 else "❌"
            signal_date = (pick.get("created_at") or "")[:10]
            lines.append(
                f"  {emoji} {pick['name']} ({pick['symbol']})  "
                f"기준 {fmt_price(float(pick['signal_price']))} → {fmt_price(curr)}  "
                f"{fmt_ret(eff_ret)}  [{signal_date}]"
            )

    if rows_short:
        if rows_long:
            lines.append("")
        lines.append("🔴 SHORT 추천 성과")
        for pick, curr, eff_ret in sorted(rows_short, key=lambda x: -x[2]):
            emoji = "✅" if eff_ret > 0 else "❌"
            signal_date = (pick.get("created_at") or "")[:10]
            lines.append(
                f"  {emoji} {pick['name']} ({pick['symbol']})  "
                f"기준 {fmt_price(float(pick['signal_price']))} → {fmt_price(curr)}  "
                f"{fmt_ret(eff_ret)}  [{signal_date}]"
            )

    # Style-based performance breakdown with next-week suggestions
    all_entries: list[tuple[dict, float]] = [
        (p, r) for p, _, r in rows_long + rows_short
    ]
    if len(all_entries) >= 2:
        style_groups: dict[str, list[float]] = {}
        for pick, ret in all_entries:
            sty = (pick.get("style") or "기타").split(" + ")[0]
            style_groups.setdefault(sty, []).append(ret)

        lines.append("")
        lines.append("  📊 스타일별 성과")
        suggestions: list[str] = []
        for sty, rets in sorted(style_groups.items(), key=lambda x: -sum(x[1]) / len(x[1])):
            avg_r = sum(rets) / len(rets)
            wins = sum(1 for r in rets if r > 0)
            win_pct = wins / len(rets) * 100
            lines.append(
                f"    {sty}: 평균 {avg_r:+.1f}%  승률 {wins}/{len(rets)} ({win_pct:.0f}%)"
            )
            if win_pct >= 60:
                suggestions.append(f"{sty} ↑ 비중 확대 고려")
            elif win_pct <= 40 and len(rets) >= 2:
                suggestions.append(f"{sty} ↓ 비중 축소 고려")
        if suggestions:
            lines.append("  💡 다음 주 제안: " + " / ".join(suggestions[:3]))

    lines.append("")
    lines.append("  ※ 기계적 스크리닝 성과 요약. 실제 투자 판단은 본인 책임.")
    return "\n".join(lines)
