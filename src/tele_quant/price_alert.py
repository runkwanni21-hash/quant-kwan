"""Price Alert Monitor — 목표가/무효화 레벨 도달 시 텔레그램 알림.

daily-alpha 추천 후보의 현재가를 장중 30분마다 확인해
목표가 도달 또는 무효화 레벨 이탈 시 즉시 텔레그램으로 전송한다.

주의: 매수/매도 확정 지시 아님. 기계적 스크리닝 알림이며 실제 투자 판단은 사용자 책임.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tele_quant.db import Store

log = logging.getLogger(__name__)

_ALERT_LOOKBACK_DAYS = 5

# KST 기준 장중 시간 (분 단위)
_KR_OPEN_MIN = 9 * 60       # 09:00
_KR_CLOSE_MIN = 15 * 60 + 30  # 15:30
_US_OPEN_MIN = 23 * 60 + 30   # 23:30 KST (NYSE 09:30 EST)
_US_CLOSE_MIN = 6 * 60        # 06:00 KST 다음날 (NYSE 16:00 EST)


def _is_kr_market_hours(now_kst: datetime) -> bool:
    if now_kst.weekday() >= 5:  # 토·일 휴장
        return False
    m = now_kst.hour * 60 + now_kst.minute
    return _KR_OPEN_MIN <= m <= _KR_CLOSE_MIN


def _is_us_market_hours(now_kst: datetime) -> bool:
    if now_kst.weekday() >= 6:  # 일요일(KST) 새벽은 토요일 US 포함이므로 6만 제외
        return False
    m = now_kst.hour * 60 + now_kst.minute
    # 23:30~ 자정 넘어 06:00까지
    return m >= _US_OPEN_MIN or m < _US_CLOSE_MIN


def _fetch_current_prices(symbols: list[str]) -> dict[str, float]:
    """yfinance fast_info로 현재가 일괄 조회."""
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
        log.warning("[price-alert] 가격 조회 실패: %s", exc)
        return {}


def _format_alert(pick: dict[str, Any], alert_type: str, current_price: float) -> str:
    side = pick.get("side", "LONG")
    symbol = pick.get("symbol", "")
    name = pick.get("name", "") or symbol
    market = pick.get("market", "KR")
    is_kr = market == "KR"
    tgt = pick.get("target_price")
    inv = pick.get("invalidation_price")

    def fmt(v: float) -> str:
        return f"{v:,.0f}원" if is_kr else f"${v:.2f}"

    side_label = "🟢 LONG" if side == "LONG" else "🔴 SHORT"
    signal_date = (pick.get("created_at") or "")[:10]
    score = pick.get("final_score", 0)

    if alert_type == "TARGET":
        emoji, title = "🎯", "목표가 도달"
        desc = f"목표가 {fmt(tgt)} 도달 — 수익권 진입" if tgt else "목표가 도달"
    else:
        emoji, title = "🚨", "무효화 레벨 이탈"
        if side == "LONG":
            desc = f"하향 이탈 ({fmt(inv)}) — 관찰 후보 무효"
        else:
            desc = f"상향 돌파 ({fmt(inv)}) — 관찰 후보 무효"

    return (
        f"{emoji} [{title}]\n"
        f"  {name} ({symbol})  {side_label}\n"
        f"  현재가: {fmt(current_price)}  |  추천일: {signal_date}  점수: {score:.0f}\n"
        f"  {desc}\n"
        f"  ※ 기계적 스크리닝 알림. 실제 투자 판단은 본인 책임."
    )


def run_price_alerts(
    store: Store,
    market: str | None = None,
    send: bool = True,
    force: bool = False,
) -> list[dict[str, Any]]:
    """장중 가격 체크 → 목표가/무효화 도달 시 텔레그램 전송.

    Returns list of triggered alert dicts.
    force=True 이면 장중 시간대 체크 생략 (테스트/수동 실행용).
    """
    from zoneinfo import ZoneInfo

    KST = ZoneInfo("Asia/Seoul")
    now_kst = datetime.now(KST)

    kr_active = force or (market in (None, "KR") and _is_kr_market_hours(now_kst))
    us_active = force or (market in (None, "US") and _is_us_market_hours(now_kst))

    if not kr_active and not us_active:
        log.info("[price-alert] 장중 시간 아님, 종료")
        return []

    since = datetime.now(UTC) - timedelta(days=_ALERT_LOOKBACK_DAYS)
    candidates: list[dict[str, Any]] = []

    if kr_active:
        candidates += store.get_active_picks_for_alert(since, market="KR")
    if us_active:
        candidates += store.get_active_picks_for_alert(since, market="US")

    # 중복 제거 (KR/US 모두 active일 경우)
    seen: set[int] = set()
    candidates = [p for p in candidates if not (p["id"] in seen or seen.add(p["id"]))]  # type: ignore[func-returns-value]

    if not candidates:
        log.info("[price-alert] 알림 대상 없음")
        return []

    symbols = list({p["symbol"] for p in candidates})
    log.info("[price-alert] %d종목 가격 확인 중", len(symbols))
    prices = _fetch_current_prices(symbols)

    triggered: list[dict[str, Any]] = []

    for pick in candidates:
        current = prices.get(pick["symbol"])
        if current is None:
            continue

        side = pick.get("side", "LONG")
        tgt = float(pick["target_price"])
        inv = float(pick["invalidation_price"])

        alert_type: str | None = None
        if side == "LONG":
            if current >= tgt:
                alert_type = "TARGET"
            elif current <= inv:
                alert_type = "INVALID"
        else:
            if current <= tgt:
                alert_type = "TARGET"
            elif current >= inv:
                alert_type = "INVALID"

        if alert_type:
            msg = _format_alert(pick, alert_type, current)
            log.info("[price-alert] %s %s → %s @ %.2f", side, pick["symbol"], alert_type, current)
            triggered.append({"pick": pick, "type": alert_type, "price": current, "msg": msg})

            if send:
                import asyncio

                from tele_quant.settings import Settings
                from tele_quant.telegram_sender import TelegramSender

                async def _send(text: str) -> None:
                    sender = TelegramSender(Settings())
                    await sender.send(text)

                try:
                    asyncio.run(_send(msg))
                except Exception as exc:
                    log.warning("[price-alert] 전송 실패 %s: %s", pick["symbol"], exc)

                store.mark_alert_sent(pick["id"], 1 if alert_type == "TARGET" else 2)

    log.info("[price-alert] 트리거 %d건", len(triggered))
    return triggered
