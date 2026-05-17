"""Mock Portfolio — 모의 포트폴리오 자동 진입·청산 추적.

개인투자자 강점 구현:
- MAX 6종목 집중 포트 (기관은 분산 필수, 개인은 집중으로 알파 극대화)
- 기관 사각지대(300B~10T KRW) 우선 편입
- 진입: score≥80 자동 진입
- 청산: 목표가 도달 / 무효화가 이탈 / 7일 타임아웃
- 매수·매도 확정 표현 금지 — 모의 추적만
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tele_quant.daily_alpha import DailyAlphaPick
    from tele_quant.db import Store
    from tele_quant.fundamentals import FundamentalSnapshot

log = logging.getLogger(__name__)

MAX_POSITIONS = 6       # 기관 대비 집중 포트 (알파 극대화)
MIN_SCORE = 80.0        # 자동 진입 최소 스코어
HOLD_MAX_DAYS = 7       # 최대 보유일
EDGE_BONUS_SCORE = 75.0 # 기관사각지대는 score 75점 이상도 관찰 편입

# ── Position entry ────────────────────────────────────────────────────────────

def enter_position(
    store: Store,
    pick: DailyAlphaPick,
    snap: FundamentalSnapshot | None = None,
) -> bool:
    """score≥80 이상 pick을 모의 포트폴리오에 진입. 성공 시 True."""
    threshold = MIN_SCORE
    if snap is not None and snap.is_blind_spot:
        threshold = EDGE_BONUS_SCORE  # 기관 사각지대면 75점도 진입

    if pick.final_score < threshold:
        return False

    # 현재 포지션 수 확인
    open_pos = get_open_positions(store)
    if len(open_pos) >= MAX_POSITIONS:
        log.info("[portfolio] max positions (%d) reached — skip %s", MAX_POSITIONS, pick.symbol)
        return False

    # 동일 종목 이미 보유 중이면 스킵
    open_syms = {p["symbol"] for p in open_pos}
    if pick.symbol in open_syms:
        log.debug("[portfolio] %s already in portfolio", pick.symbol)
        return False

    entry_price = pick.signal_price or 0.0
    if entry_price <= 0:
        return False

    is_blind = snap.is_blind_spot if snap else False

    try:
        now = datetime.now(UTC).isoformat()
        with store.connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO mock_portfolio_positions
                (created_at, symbol, name, market, side, sector,
                 entry_price, entry_score, entry_at,
                 invalidation_price, target_price,
                 status, is_institutional_blind_spot, source_pick_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    now, pick.symbol, pick.name, pick.market, pick.side,
                    getattr(pick, "sector", ""),
                    entry_price, pick.final_score, now,
                    pick.invalidation_level, pick.target_zone,
                    "open", int(is_blind),
                    getattr(pick, "id", None),
                ),
            )
            conn.commit()
        log.info("[portfolio] entered %s %s @ %.2f score=%.0f",
                 pick.side, pick.symbol, entry_price, pick.final_score)
        return True
    except Exception as exc:
        log.warning("[portfolio] enter failed %s: %s", pick.symbol, exc)
        return False


# ── Position exit ─────────────────────────────────────────────────────────────

def _get_current_price(symbol: str) -> float | None:
    """현재가 조회 (yfinance)."""
    try:
        import yfinance as yf
        hist = yf.Ticker(symbol).history(period="2d", interval="1d", auto_adjust=True)
        if hist is not None and not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception:
        pass
    return None


def check_exits(store: Store) -> list[dict]:
    """목표가 도달 / 무효화 이탈 / 7일 타임아웃 확인 후 청산. 청산된 포지션 반환."""
    closed: list[dict] = []
    open_pos = get_open_positions(store)
    if not open_pos:
        return closed

    for pos in open_pos:
        sym = pos["symbol"]
        side = pos["side"]
        entry_price = pos.get("entry_price") or 0.0
        inval_raw = pos.get("invalidation_price")
        target_raw = pos.get("target_price")
        entry_at_str = pos.get("entry_at", "")

        current = _get_current_price(sym)
        if current is None:
            continue

        # 보유기간
        try:
            entry_at = datetime.fromisoformat(entry_at_str.replace("Z", "+00:00"))
        except Exception:
            entry_at = datetime.now(UTC) - timedelta(days=1)
        days_held = (datetime.now(UTC) - entry_at.replace(tzinfo=UTC) if entry_at.tzinfo is None else datetime.now(UTC) - entry_at).days

        # 수익률 계산
        if entry_price > 0:
            if side == "LONG":
                ret_pct = (current - entry_price) / entry_price * 100
            else:
                ret_pct = (entry_price - current) / entry_price * 100
        else:
            ret_pct = 0.0

        # 청산 조건 판단
        exit_reason = None

        # 무효화 이탈
        try:
            inval = float(inval_raw) if inval_raw else None
        except (TypeError, ValueError):
            inval = None
        if inval is not None and (
            (side == "LONG" and current < inval) or (side == "SHORT" and current > inval)
        ):
            exit_reason = "closed_stop"

        # 목표가 도달
        try:
            target = float(target_raw) if target_raw else None
        except (TypeError, ValueError):
            target = None
        if target is not None and exit_reason is None and (
            (side == "LONG" and current >= target) or (side == "SHORT" and current <= target)
        ):
            exit_reason = "closed_target"

        # 7일 타임아웃
        if exit_reason is None and days_held >= HOLD_MAX_DAYS:
            exit_reason = "closed_timeout"

        if exit_reason:
            _close_position(store, pos["id"], current, ret_pct, exit_reason)
            closed.append({**pos, "exit_price": current, "return_pct": ret_pct, "exit_reason": exit_reason})

    return closed


def _close_position(store: Store, pos_id: int, exit_price: float, ret_pct: float, status: str) -> None:
    now = datetime.now(UTC).isoformat()
    try:
        with store.connect() as conn:
            conn.execute(
                """UPDATE mock_portfolio_positions
                SET status=?, exit_price=?, exit_at=?, return_pct=?
                WHERE id=?""",
                (status, exit_price, now, ret_pct, pos_id),
            )
            conn.commit()
    except Exception as exc:
        log.warning("[portfolio] close failed id=%s: %s", pos_id, exc)


# ── Query helpers ─────────────────────────────────────────────────────────────

def get_open_positions(store: Store) -> list[dict]:
    """현재 보유 중인 모의 포지션 목록."""
    try:
        with store.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM mock_portfolio_positions WHERE status='open' ORDER BY entry_at"
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception as exc:
        log.warning("[portfolio] get_open_positions failed: %s", exc)
        return []


def get_portfolio_summary(store: Store, days: int = 30) -> dict:
    """포트폴리오 성과 요약."""
    try:
        since = (datetime.now(UTC) - timedelta(days=days)).isoformat()
        with store.connect() as conn:
            closed = conn.execute(
                """SELECT return_pct, status FROM mock_portfolio_positions
                WHERE status != 'open' AND exit_at > ? ORDER BY exit_at DESC LIMIT 50""",
                (since,),
            ).fetchall()
            open_pos = conn.execute(
                "SELECT * FROM mock_portfolio_positions WHERE status='open'"
            ).fetchall()

        closed_list = [dict(r) for r in closed]
        open_list = [dict(r) for r in open_pos]

        total_closed = len(closed_list)
        wins = sum(1 for r in closed_list if (r.get("return_pct") or 0) > 0)
        avg_ret = (
            sum((r.get("return_pct") or 0) for r in closed_list) / total_closed
            if total_closed > 0 else 0.0
        )
        win_rate = wins / total_closed * 100 if total_closed > 0 else 0.0

        return {
            "open_count": len(open_list),
            "max_positions": MAX_POSITIONS,
            "closed_count": total_closed,
            "win_rate": win_rate,
            "avg_return": avg_ret,
            "open_positions": open_list,
            "recent_closed": closed_list[:5],
        }
    except Exception as exc:
        log.warning("[portfolio] summary failed: %s", exc)
        return {
            "open_count": 0, "max_positions": MAX_POSITIONS,
            "closed_count": 0, "win_rate": 0.0, "avg_return": 0.0,
            "open_positions": [], "recent_closed": [],
        }


# ── Report builder ────────────────────────────────────────────────────────────

def build_portfolio_section(store: Store) -> str:
    """텔레그램용 모의 포트폴리오 섹션."""
    summary = get_portfolio_summary(store)
    open_pos = summary["open_positions"]
    closed = summary["recent_closed"]

    lines: list[str] = []

    # 헤더
    wr = summary["win_rate"]
    avg_r = summary["avg_return"]
    cnt = summary["open_count"]
    mx = summary["max_positions"]
    lines.append(
        f"[보유{cnt}/{mx}]  최근승률 {wr:.0f}%  평균수익 {avg_r:+.1f}%"
    )

    # 현재 보유
    if open_pos:
        for pos in open_pos:
            sym = pos.get("symbol", "")
            name = pos.get("name", sym)
            side = pos.get("side", "LONG")
            entry = pos.get("entry_price") or 0.0
            entry_at_str = pos.get("entry_at", "")
            blind = pos.get("is_institutional_blind_spot", 0)
            edge = " 🎯" if blind else ""

            current = _get_current_price(sym)
            if current and entry > 0:
                if side == "LONG":
                    ret = (current - entry) / entry * 100
                else:
                    ret = (entry - current) / entry * 100
                arrow = "▲" if ret >= 0 else "▼"
            else:
                ret = 0.0
                arrow = "—"

            # 보유기간
            try:
                entry_dt = datetime.fromisoformat(entry_at_str.replace("Z", "+00:00"))
                days = (datetime.now(UTC) - entry_dt.replace(tzinfo=UTC) if entry_dt.tzinfo is None else datetime.now(UTC) - entry_dt).days
                days_str = f"{days}일"
            except Exception:
                days_str = ""

            side_label = "L" if side == "LONG" else "S"
            lines.append(
                f"  {arrow}[{side_label}] {name}({sym.replace('.KS','').replace('.KQ','')}) "
                f"{ret:+.1f}% | {days_str}{edge}"
            )

    # 최근 청산
    if closed:
        lines.append("  ─ 최근 청산 ─")
        for pos in closed[:3]:
            sym = pos.get("symbol", "")
            name = pos.get("name", sym)
            ret = pos.get("return_pct") or 0.0
            status = pos.get("status", "")
            icon = "✅" if ret > 0 else "❌"
            reason = {"closed_target": "목표도달", "closed_stop": "손절", "closed_timeout": "기간만료"}.get(status, "청산")
            lines.append(f"  {icon} {name} {ret:+.1f}% ({reason})")

    return "\n".join(lines)
