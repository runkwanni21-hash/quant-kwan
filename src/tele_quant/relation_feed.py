from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tele_quant.relation_fallback import FallbackLeadLagCandidate

log = logging.getLogger(__name__)

# ── Self-computed mover universe ──────────────────────────────────────────────

_UNIVERSE_US: list[str] = [
    "NVDA", "AAPL", "MSFT", "GOOGL", "META", "AMZN", "TSLA", "AMD",
    "INTC", "QCOM", "AVGO", "MU", "TSM", "ASML", "ARM", "SMCI", "MRVL",
    "ON", "TXN", "LRCX", "KLAC", "AMAT",
    "JPM", "GS", "MS", "BAC",
    "XOM", "CVX", "NEE", "FSLR", "FLNC",
    "PLTR", "ORCL", "CRM", "SNOW",
    "LLY", "UNH", "MRNA",
]

_UNIVERSE_KR: list[str] = [
    "005930.KS", "000660.KS", "035420.KS", "035720.KS",
    "051910.KS", "006400.KS", "373220.KS", "003670.KS",
    "207940.KS", "005380.KS", "000270.KS", "068270.KS",
    "012330.KS", "329180.KS", "096770.KS", "066570.KS",
    "017670.KS", "030200.KS", "086790.KS", "034730.KS",
]

_NAME_MAP: dict[str, str] = {
    "NVDA": "NVIDIA", "AAPL": "Apple", "MSFT": "Microsoft",
    "GOOGL": "Alphabet", "META": "Meta", "AMZN": "Amazon",
    "TSLA": "Tesla", "AMD": "AMD", "INTC": "Intel",
    "QCOM": "Qualcomm", "AVGO": "Broadcom", "MU": "Micron",
    "TSM": "TSMC ADR", "ASML": "ASML", "ARM": "ARM Holdings",
    "SMCI": "Super Micro", "MRVL": "Marvell", "ON": "ON Semi",
    "TXN": "Texas Instruments", "LRCX": "Lam Research",
    "KLAC": "KLA Corp", "AMAT": "Applied Materials",
    "JPM": "JPMorgan", "GS": "Goldman Sachs", "MS": "Morgan Stanley",
    "BAC": "Bank of America", "XOM": "ExxonMobil", "CVX": "Chevron",
    "NEE": "NextEra Energy", "FSLR": "First Solar", "FLNC": "Fluence Energy",
    "PLTR": "Palantir", "ORCL": "Oracle", "CRM": "Salesforce", "SNOW": "Snowflake",
    "LLY": "Eli Lilly", "UNH": "UnitedHealth", "MRNA": "Moderna",
    "005930.KS": "삼성전자", "000660.KS": "SK하이닉스", "035420.KS": "NAVER",
    "035720.KS": "카카오", "051910.KS": "LG화학", "006400.KS": "삼성SDI",
    "373220.KS": "LG에너지솔루션", "003670.KS": "포스코퓨처엠",
    "207940.KS": "삼성바이오로직스", "005380.KS": "현대차",
    "000270.KS": "기아", "068270.KS": "셀트리온", "012330.KS": "현대모비스",
    "329180.KS": "HD현대중공업", "096770.KS": "SK이노베이션",
    "066570.KS": "LG전자", "017670.KS": "SK텔레콤",
    "030200.KS": "KT", "086790.KS": "하나금융지주", "034730.KS": "SK",
}

_SECTOR_MAP: dict[str, str] = {
    "NVDA": "반도체/AI", "AMD": "반도체/AI", "INTC": "반도체", "QCOM": "반도체",
    "AVGO": "반도체", "MU": "반도체", "TSM": "반도체", "ASML": "반도체장비",
    "ARM": "반도체/AI", "SMCI": "서버/AI", "MRVL": "반도체", "ON": "반도체",
    "TXN": "반도체", "LRCX": "반도체장비", "KLAC": "반도체장비", "AMAT": "반도체장비",
    "AAPL": "빅테크", "MSFT": "빅테크/AI", "GOOGL": "빅테크/AI", "META": "소셜/AI",
    "AMZN": "이커머스/클라우드", "TSLA": "전기차/에너지",
    "PLTR": "AI소프트웨어", "ORCL": "클라우드", "CRM": "클라우드", "SNOW": "클라우드",
    "JPM": "금융", "GS": "금융", "MS": "금융", "BAC": "금융",
    "XOM": "에너지", "CVX": "에너지", "NEE": "신재생에너지",
    "FSLR": "태양광", "FLNC": "ESS/에너지",
    "LLY": "바이오/제약", "UNH": "헬스케어", "MRNA": "바이오/제약",
    "005930.KS": "반도체/전자", "000660.KS": "반도체", "035420.KS": "IT서비스",
    "035720.KS": "IT서비스", "051910.KS": "화학/배터리", "006400.KS": "배터리",
    "373220.KS": "배터리", "003670.KS": "배터리소재",
    "207940.KS": "바이오", "005380.KS": "자동차", "000270.KS": "자동차",
    "068270.KS": "바이오", "012330.KS": "자동차부품", "329180.KS": "조선",
    "096770.KS": "에너지/화학", "066570.KS": "전자",
    "017670.KS": "통신", "030200.KS": "통신", "086790.KS": "금융", "034730.KS": "지주/에너지",
}

# Threshold: minimum abs(return_pct) to qualify as a mover
_MOVER_THRESH: dict[str, float] = {"US": 4.0, "KR": 5.0}

_MACRO_ONLY_FORBIDDEN = frozenset({"롱 관심", "숏/매도", "관심 진입", "손절", "목표/매도 관찰"})


@dataclass
class RelationFeedSummary:
    generated_at: str = ""
    asof_date: str = ""
    price_rows: int = 0
    mover_rows: int = 0
    leadlag_rows: int = 0
    status: str = ""
    warnings: list[str] = field(default_factory=list)
    source_project: str = ""
    method: str = ""


@dataclass
class MoverRow:
    asof_date: str
    market: str
    symbol: str
    name: str
    sector: str
    close: float | None
    prev_close: float | None
    return_pct: float
    volume: float | None
    volume_ratio_20d: float | None
    move_type: str  # UP or DOWN


@dataclass
class LeadLagCandidateRow:
    asof_date: str
    source_market: str
    source_symbol: str
    source_name: str
    source_sector: str
    source_move_type: str
    source_return_pct: float
    target_market: str
    target_symbol: str
    target_name: str
    target_sector: str
    relation_type: str
    lag_days: int
    event_count: int
    hit_count: int
    conditional_prob: float
    lift: float
    confidence: str  # high, medium, low
    direction: str  # beneficiary, risk
    note: str


@dataclass
class RelationTargetLiveCheck:
    """Current price/volume/4H status for a relation-feed target symbol."""

    target_symbol: str
    latest_price: float | None = None
    today_return_pct: float | None = None
    volume_ratio: float | None = None
    rsi_4h: float | None = None
    obv_trend_4h: str = ""
    bollinger_4h: str = ""
    expected_direction: str = "UP"  # UP or DOWN
    live_status: str = "DATA_MISSING"  # CONFIRMED / WATCH / MIXED / NOT_CONFIRMED / DATA_MISSING


@dataclass
class RelationFeedData:
    summary: RelationFeedSummary | None = None
    movers: list[MoverRow] = field(default_factory=list)
    leadlag: list[LeadLagCandidateRow] = field(default_factory=list)
    fallback_candidates: list[FallbackLeadLagCandidate] = field(default_factory=list)
    feed_age_hours: float | None = None
    is_stale: bool = False
    load_warnings: list[str] = field(default_factory=list)

    @property
    def available(self) -> bool:
        return self.summary is not None


def _feed_age_hours(generated_at: str) -> float | None:
    if not generated_at:
        return None
    try:
        dt = datetime.fromisoformat(generated_at)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        now = datetime.now(UTC)
        return (now - dt).total_seconds() / 3600.0
    except Exception:
        return None


def _is_recent_asof(asof_date: str, max_days: int = 3) -> bool:
    """Return True if asof_date is within max_days calendar days of today."""
    try:
        d = date.fromisoformat(asof_date)
        return 0 <= (date.today() - d).days <= max_days
    except Exception:
        return False


def _yf_symbol(symbol: str, market: str) -> str:
    if market == "KR":
        return f"{symbol}.KS"
    return symbol


def _fetch_yf_daily_safe(yf_sym: str) -> tuple[float | None, float | None, float | None]:
    """Return (latest_price, today_return_pct, volume_ratio_vs_recent). Never raises."""
    try:
        import yfinance as yf

        df = yf.Ticker(yf_sym).history(period="5d", interval="1d", auto_adjust=True)
        if df is None or df.empty or len(df) < 2:
            return None, None, None
        prev_close = float(df["Close"].iloc[-2])
        last_close = float(df["Close"].iloc[-1])
        ret = (last_close - prev_close) / prev_close * 100 if prev_close > 0 else None
        vol_ratio: float | None = None
        if "Volume" in df.columns:
            vols = df["Volume"].dropna()
            if len(vols) >= 2:
                avg = float(vols.iloc[:-1].mean())
                if avg > 0:
                    vol_ratio = float(vols.iloc[-1]) / avg
        return last_close, ret, vol_ratio
    except Exception:
        return None, None, None


def _expected_direction_from_relation_type(relation_type: str) -> str:
    rt = relation_type.upper()
    if rt in ("UP_LEADS_UP", "DOWN_LEADS_UP"):
        return "UP"
    return "DOWN"


def _judge_live_status(
    expected_direction: str,
    today_return_pct: float | None,
    volume_ratio: float | None,
    obv_trend: str,
) -> str:
    if today_return_pct is None:
        return "DATA_MISSING"
    is_up = expected_direction == "UP"
    if is_up:
        if today_return_pct > 0:
            if volume_ratio is not None and volume_ratio >= 1.0 and obv_trend == "상승":
                return "CONFIRMED"
            return "WATCH"
        if today_return_pct < -0.5:
            return "NOT_CONFIRMED"
        return "MIXED"
    else:
        if today_return_pct < 0:
            if volume_ratio is not None and volume_ratio >= 1.0 and obv_trend == "하락":
                return "CONFIRMED"
            return "WATCH"
        if today_return_pct > 0.5:
            return "NOT_CONFIRMED"
        return "MIXED"


_LIVE_STATUS_LABELS: dict[str, str] = {
    "CONFIRMED": "확인됨",
    "WATCH": "부분 확인 (거래량/4H 추가 확인 필요)",
    "MIXED": "혼조 (통계와 현재가 불일치)",
    "NOT_CONFIRMED": "미확인 (통계와 현재가 반대 방향)",
    "DATA_MISSING": "현재가 확인 불가, 통계만 참고",
}

_LIVE_STATUS_JUDGMENT: dict[str, str] = {
    "CONFIRMED": "조건 충족, 관심 관찰 대상",
    "WATCH": "가격 확인 전 관찰 후보",
    "MIXED": "관찰만 — 통계와 현재가 불일치",
    "NOT_CONFIRMED": "관찰만 — 통계와 현재가 반대",
    "DATA_MISSING": "가격 확인 전 관찰 후보",
}


def _compute_live_movers(settings: Any) -> tuple[list[MoverRow], str]:
    """Scan built-in universe for significant recent movers via yfinance.

    Returns (movers, asof_date). Never raises — returns ([], today) on any error.
    """
    import yfinance as yf

    all_syms = list(_UNIVERSE_US) + list(_UNIVERSE_KR)
    asof_date = str(date.today())

    try:
        data = yf.download(
            all_syms,
            period="5d",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
    except Exception as exc:
        log.warning("[relation_feed] yfinance download failed: %s", exc)
        return [], asof_date

    if data is None or data.empty:
        return [], asof_date

    # Multi-ticker download: MultiIndex columns (metric, symbol)
    try:
        closes = data["Close"] if "Close" in data.columns.get_level_values(0) else data
    except Exception:
        closes = data

    valid = closes.dropna(how="all")
    if valid.empty:
        return [], asof_date

    asof_date = str(valid.index[-1].date())
    movers: list[MoverRow] = []

    for sym in all_syms:
        try:
            if sym not in closes.columns:
                continue
            series = closes[sym].dropna()
            if len(series) < 2:
                continue
            prev_close = float(series.iloc[-2])
            curr_close = float(series.iloc[-1])
            if prev_close <= 0:
                continue
            ret = (curr_close - prev_close) / prev_close * 100
            market = "KR" if sym.endswith((".KS", ".KQ")) else "US"
            thresh = _MOVER_THRESH.get(market, 4.0)
            if abs(ret) < thresh:
                continue
            move_type = "UP" if ret > 0 else "DOWN"
            movers.append(MoverRow(
                asof_date=asof_date,
                market=market,
                symbol=sym,
                name=_NAME_MAP.get(sym, sym),
                sector=_SECTOR_MAP.get(sym, ""),
                close=curr_close,
                prev_close=prev_close,
                return_pct=ret,
                volume=None,
                volume_ratio_20d=None,
                move_type=move_type,
            ))
        except Exception:
            continue

    movers.sort(key=lambda m: -abs(m.return_pct))
    log.info("[relation_feed] computed movers: %d from %d symbols", len(movers), len(all_syms))
    return movers, asof_date


def load_relation_feed(settings: Any) -> RelationFeedData:
    """Compute live relation feed from yfinance. No external dependency. Never raises."""
    if not getattr(settings, "relation_feed_enabled", True):
        return RelationFeedData()

    try:
        movers, asof_date = _compute_live_movers(settings)
    except Exception as exc:
        log.warning("[relation_feed] mover computation failed: %s", exc)
        return RelationFeedData()

    now_str = datetime.now(UTC).isoformat()
    summary = RelationFeedSummary(
        generated_at=now_str,
        asof_date=asof_date,
        price_rows=len(_UNIVERSE_US) + len(_UNIVERSE_KR),
        mover_rows=len(movers),
        leadlag_rows=0,
        status="live",
        source_project="tele_quant_self",
        method="yfinance-scan + correlation lead-lag",
    )

    result = RelationFeedData(
        summary=summary,
        movers=movers,
        is_stale=False,
        feed_age_hours=0.0,
    )
    log.info(
        "[relation_feed] loaded: movers=%d leadlag=%d stale=%s",
        len(result.movers),
        len(result.leadlag),
        result.is_stale,
    )
    return result


def fetch_relation_target_live_checks(
    feed: RelationFeedData,
    settings: Any = None,
) -> dict[str, RelationTargetLiveCheck]:
    """Fetch current price/volume/4H status for all relation-feed targets. Never raises."""
    target_info: dict[str, tuple[str, str]] = {}
    for row in feed.leadlag:
        if row.target_symbol not in target_info:
            ed = _expected_direction_from_relation_type(row.relation_type)
            target_info[row.target_symbol] = (row.target_market, ed)
    for c in feed.fallback_candidates:
        if c.target_symbol not in target_info:
            ed = _expected_direction_from_relation_type(getattr(c, "relation_type", "UP_LEADS_UP"))
            target_info[c.target_symbol] = (getattr(c, "target_market", "US"), ed)

    result: dict[str, RelationTargetLiveCheck] = {}
    for sym, (market, expected_dir) in target_info.items():
        yf_sym = _yf_symbol(sym, market)
        price, ret_pct, vol_ratio = _fetch_yf_daily_safe(yf_sym)

        rsi_4h: float | None = None
        obv_trend = ""
        bb_pos = ""
        if settings is not None:
            try:
                from tele_quant.analysis.intraday import fetch_intraday_4h

                snap = fetch_intraday_4h(yf_sym, settings)
                if snap is not None:
                    rsi_4h = snap.rsi14
                    obv_trend = snap.obv_trend
                    bb_pos = snap.bb_position
            except Exception:
                pass

        status = _judge_live_status(expected_dir, ret_pct, vol_ratio, obv_trend)
        result[sym] = RelationTargetLiveCheck(
            target_symbol=sym,
            latest_price=price,
            today_return_pct=ret_pct,
            volume_ratio=vol_ratio,
            rsi_4h=rsi_4h,
            obv_trend_4h=obv_trend,
            bollinger_4h=bb_pos,
            expected_direction=expected_dir,
            live_status=status,
        )
    return result


def get_all_target_symbols(feed: RelationFeedData) -> set[str]:
    syms = {row.target_symbol for row in feed.leadlag}
    syms.update(c.target_symbol for c in feed.fallback_candidates)
    return syms


def get_relation_boost(
    feed: RelationFeedData | None,
    symbol: str,
    has_telegram_evidence: bool,
    technical_ok: bool,
    live_checks: dict[str, RelationTargetLiveCheck] | None = None,
) -> tuple[float, str]:
    """Return (score_boost, note) for a symbol. boost=0 when conditions not met.

    Boost scale: high=+2, medium=+1.
    Guards: source must be an actual mover (latest_movers.csv), abs(source_return_pct)>0,
    confidence>=medium, note must not be from refill/deeper-empirical.
    When live_checks is provided, only CONFIRMED live status allows a boost.
    """
    if feed is None or not feed.available:
        return 0.0, ""

    # Only boost when telegram evidence AND technical signal both present
    if not has_telegram_evidence or not technical_ok:
        return 0.0, ""

    # Live status guard: when live_checks provided, require CONFIRMED
    if live_checks is not None:
        check = live_checks.get(symbol)
        if check is None or check.live_status != "CONFIRMED":
            return 0.0, ""

    mover_symbols = {m.symbol for m in feed.movers}

    rows = [
        r
        for r in feed.leadlag
        if r.target_symbol == symbol
        and r.source_symbol in mover_symbols
        and abs(r.source_return_pct) > 0
        and not _is_refill_note(r.note)
    ]
    if rows:
        r = rows[0]
        if r.confidence == "high":
            boost = 2.0
        elif r.confidence == "medium":
            boost = 1.0
        else:
            return 0.0, ""
        sign = "+" if r.source_move_type == "UP" else "-"
        src_name = r.source_name or r.source_symbol
        note = (
            f"{src_name} {sign}{abs(r.source_return_pct):.1f}% 후 "
            f"{symbol} {r.lag_days}일 후 동행 사례, "
            f"조건부확률 {r.conditional_prob:.1%}, lift {r.lift:.1f}x"
        )
        return boost, note

    # Fallback: medium only (no high), source must have actual return
    fb_rows = [
        c
        for c in feed.fallback_candidates
        if c.target_symbol == symbol
        and c.confidence == "medium"
        and c.source_symbol in mover_symbols
        and abs(c.source_return_pct) > 0
    ]
    if fb_rows:
        c = fb_rows[0]
        sign = "+" if c.source_move_type == "UP" else "-"
        src_name = c.source_name or c.source_symbol
        note = (
            f"[fallback] {src_name} {sign}{abs(c.source_return_pct):.1f}% 후 "
            f"{symbol} {c.lag_days}일 후 동행 사례, "
            f"조건부확률 {c.conditional_prob:.1%}, lift {c.lift:.1f}x"
        )
        return 1.0, note

    return 0.0, ""


def _is_refill_note(note: str) -> bool:
    """Return True when note originates from refill/deeper-empirical lawbook (not raw mover data)."""
    n = note.lower()
    return "refill" in n or "deeper" in n or "empirical" in n or "lawbook" in n


# ── Korean explanation helpers (used in section rendering and tests) ──────────


def format_probability_explanation(prob: float) -> str:
    """Return a Korean plain-language summary for a conditional probability."""
    pct = prob * 100
    if pct >= 70:
        strength = "강한 반복 패턴"
    elif pct >= 60:
        strength = "중간 수준 반복 패턴"
    else:
        strength = "약한 반복 패턴"
    return f"과거 비슷한 상황에서 target이 같은 방향으로 반응한 비율이 약 {pct:.0f}% ({strength})"


def format_lift_explanation(lift: float, event_count: int = 0) -> str:
    """Return a Korean plain-language summary for a lift value."""
    note = f"평소보다 약 {lift:.1f}배 자주 나타난 후행 반응"
    if event_count > 0 and event_count < 10:
        note += " (표본 수 확인 필요)"
    return note


def format_confidence_explanation(confidence: str) -> str:
    """Return a Korean plain-language confidence label."""
    if confidence == "high":
        return "표본 수와 반복성이 비교적 양호"
    if confidence == "medium":
        return "관찰할 만하지만 현재 가격 확인 필요"
    return "참고만 가능, 리포트 기본 노출 제한"


def _today_watchpoints(is_up: bool) -> str:
    """Return a short Korean checklist of things to verify today."""
    if is_up:
        return "거래량 증가 + 4H RSI 우상향 + OBV 개선 확인"
    return "반등 실패 + 거래량 동반 하락이면 주의"


_RELATION_FEED_DISCLAIMER = (
    "이 섹션은 매수/매도 지시가 아니라, 과거 급등·급락 이후 반복된 후행 반응을 보여주는 "
    "통계적 관찰 목록입니다. 현재 주가·거래량·4H RSI/OBV가 같은 방향으로 확인되지 않으면 "
    "관찰만 합니다."
)


def build_relation_feed_section(
    feed: RelationFeedData,
    watchlist_symbols: set[str] | None = None,
    telegram_symbols: set[str] | None = None,
    macro_only: bool = False,
    max_movers: int = 8,
    max_targets: int = 3,
    settings: Any = None,
    live_checks: dict[str, RelationTargetLiveCheck] | None = None,
) -> str:
    """Build the relation feed digest section."""
    if settings is not None:
        max_movers = int(getattr(settings, "relation_feed_max_movers", max_movers))
        max_targets = int(getattr(settings, "relation_feed_max_targets_per_mover", max_targets))

    watchlist_symbols = watchlist_symbols or set()
    telegram_symbols = telegram_symbols or set()

    lines: list[str] = []

    if not feed.available:
        if feed.load_warnings:
            lines.append("⚡ 후행 관찰 후보")
            lines.append(f"- {feed.load_warnings[0]}")
        return "\n".join(lines)

    summary = feed.summary
    assert summary is not None

    if macro_only:
        lines.append("⚡ 급등·급락 후행 관찰 후보")
        lines.append("- 이번 주말에는 매매 시나리오가 아니라 통계적 관찰 후보만 표시합니다.")
    else:
        lines.append("⚡ 최근 급등·급락 → 후행 관찰 후보")

    lines.append(f"- 기준일: {summary.asof_date}")
    lines.append(
        f"- 데이터: {summary.method or 'yfinance-scan + correlation lead-lag'}"
    )
    fb_count = len(feed.fallback_candidates)
    stats_line = f"- 스캔 종목: {summary.price_rows}개 / 급등락 모버: {len(feed.movers)}개"
    if fb_count:
        stats_line += f" / 상관관계 후보: {fb_count}개"
    lines.append(stats_line)
    if not feed.leadlag and fb_count:
        lines.append(
            "- 가격·상관관계 DB 기반 후행 후보 자체 계산"
        )

    if summary.warnings:
        lines.append(f"- 주의: {', '.join(summary.warnings)}")
    for w in feed.load_warnings:
        lines.append(f"- ⚠️ {w}")

    if not feed.movers and not feed.leadlag:
        lines.append("- 급등·급락 데이터 없음")
        lines.append("- 통계적 후보이며 실제 상승/하락 보장 아님")
        return "\n".join(lines)

    max_each = max(1, max_movers // 2)
    # Only show movers with a real return (abs > 0) — skip +0.0% placeholder rows
    up_movers = [m for m in feed.movers if m.move_type == "UP" and abs(m.return_pct) > 0][
        : min(5, max_each)
    ]
    down_movers = [m for m in feed.movers if m.move_type == "DOWN" and abs(m.return_pct) > 0][
        : min(5, max_each)
    ]

    # source → deduplicated targets map (max 2 per source for readability)
    _display_targets = min(2, max_targets)
    source_targets: dict[str, list[LeadLagCandidateRow]] = {}
    for row in feed.leadlag:
        src = row.source_symbol
        if src not in source_targets:
            source_targets[src] = []
        already = {r.target_symbol for r in source_targets[src]}
        if row.target_symbol not in already and len(source_targets[src]) < _display_targets:
            source_targets[src].append(row)

    # Global cap: max 6 source-target pairs total to keep section concise
    _MAX_TOTAL_PAIRS = 6
    _pair_count = 0
    _no_price_blocked = 0  # 현재가 확인 불가로 접힌 후보 수

    def _render_block(mover: MoverRow, is_up: bool) -> list[str]:
        nonlocal _pair_count, _no_price_blocked
        block: list[str] = []
        if _pair_count >= _MAX_TOTAL_PAIRS:
            return block

        star = "⭐ " if mover.symbol in watchlist_symbols else ""
        sign = "+" if is_up else ""
        name_display = (
            (mover.name or mover.symbol)
            .replace("DIRECTION_WORD_REMOVED", "")
            .replace("  ", " ")
            .strip()
        )
        src_label = (
            f"{name_display} / {mover.symbol}" if name_display != mover.symbol else mover.symbol
        )
        src_sign = f"{sign}{mover.return_pct:.1f}%"

        targets = source_targets.get(mover.symbol, [])
        if not targets:
            return block

        for t in targets:
            if _pair_count >= _MAX_TOTAL_PAIRS:
                break
            _pair_count += 1

            tg_note = " ★텔레그램" if t.target_symbol in telegram_symbols else ""
            wl_t = "⭐ " if t.target_symbol in watchlist_symbols else ""
            tname = t.target_name or t.target_symbol
            tgt_label = (
                f"{tname} / {t.target_symbol}" if tname != t.target_symbol else t.target_symbol
            )
            event_cnt = getattr(t, "event_count", 0)

            if is_up:
                direction_label = "관찰 후보" if macro_only else "후행 관찰 후보"
                meaning = f"과거 {name_display}이(가) 강하게 오른 뒤 {tname}도 뒤따라 움직인 사례가 반복됨"
            else:
                direction_label = "관찰 후보" if macro_only else "약세 전이 관찰 후보"
                meaning = (
                    f"과거 {name_display}이(가) 급락한 뒤 {tname}도 약세로 반응한 사례가 반복됨"
                )

            prob_str = f"{t.conditional_prob:.1%}"
            lift_str = format_lift_explanation(t.lift, event_cnt)
            conf_str = format_confidence_explanation(t.confidence)
            watch_str = _today_watchpoints(is_up)

            # Live check status
            if live_checks is not None:
                chk = live_checks.get(t.target_symbol)
                live_label = _LIVE_STATUS_LABELS.get(
                    chk.live_status if chk else "DATA_MISSING",
                    "현재가 확인 불가",
                )
                judgment = _LIVE_STATUS_JUDGMENT.get(
                    chk.live_status if chk else "DATA_MISSING",
                    "가격 확인 전 관찰 후보",
                )
                if chk and chk.today_return_pct is not None:
                    ret_sign = "+" if chk.today_return_pct > 0 else ""
                    live_label += f" ({ret_sign}{chk.today_return_pct:.1f}%)"
            else:
                live_label = "라이브 확인 미실행 — 통계만 참고"
                judgment = "가격 확인 전 관찰 후보"

            # live_checks가 있으나 가격 미확인인 경우만 접힘 처리
            if live_checks is not None and "확인 불가" in live_label:
                _no_price_blocked += 1
                continue

            block.append(f"{_pair_count}. {star}{src_label} {src_sign}")
            block.append(f"   → {direction_label}: {wl_t}{tgt_label}{tg_note}")
            block.append(f"   - 의미: {meaning}")
            block.append(
                f"   - 통계: 조건부확률 {prob_str} / {lift_str} / 신뢰도 {t.confidence} ({conf_str})"
            )
            block.append(f"   - 현재 확인: {live_label}")
            block.append(f"   - 오늘 볼 것: {watch_str}")
            block.append(f"   - 판정: {judgment}")

        return block

    total_hidden = 0
    if up_movers:
        lines.append("")
        lines.append("🟢 급등 출발점" if not macro_only else "🟢 급등 관찰")
        for m in up_movers:
            lines.extend(_render_block(m, is_up=True))

    if down_movers:
        lines.append("")
        lines.append("🔴 급락 출발점" if not macro_only else "🔴 급락 관찰")
        for m in down_movers:
            lines.extend(_render_block(m, is_up=False))

    # Count hidden candidates beyond the display cap
    all_pairs = sum(
        min(len(targets), _display_targets) for targets in source_targets.values() if targets
    )
    total_hidden = max(0, all_pairs - _pair_count)
    if total_hidden > 0:
        lines.append(f"  (그 외 {total_hidden}개 후보는 숨김)")

    # Fallback section (only when stock feed has no leadlag)
    fb = feed.fallback_candidates
    if not feed.leadlag and fb:
        lines.append("")
        lines.append("🔄 상관관계 기반 후행 후보 (가격 DB 자체 계산)")
        medium = [c for c in fb if c.confidence == "medium"]
        low_top = [c for c in fb if c.confidence == "low"][:3]
        to_show = medium if medium else low_top
        for i, cand in enumerate(to_show, 1):
            is_up = cand.source_move_type == "UP"
            sign = "+" if is_up else ""
            src_name = cand.source_name or cand.source_symbol
            src_label = (
                f"{src_name} / {cand.source_symbol}"
                if src_name != cand.source_symbol
                else cand.source_symbol
            )
            tgt_name = cand.target_name or cand.target_symbol
            tgt_label = (
                f"{tgt_name} / {cand.target_symbol}"
                if tgt_name != cand.target_symbol
                else cand.target_symbol
            )
            wl = "⭐ " if watchlist_symbols and cand.target_symbol in watchlist_symbols else ""
            direction_label = "후행 관찰 후보" if is_up else "약세 전이 관찰 후보"
            if is_up:
                meaning = (
                    f"과거 {src_name}이(가) 강하게 오른 뒤 {tgt_name}도 뒤따라 움직인 사례가 반복됨"
                )
            else:
                meaning = (
                    f"과거 {src_name}이(가) 급락한 뒤 {tgt_name}도 약세로 반응한 사례가 반복됨"
                )
            lift_str = format_lift_explanation(cand.lift, cand.event_count)
            conf_str = format_confidence_explanation(cand.confidence)
            watch_str = _today_watchpoints(is_up)
            if live_checks is not None:
                fb_chk = live_checks.get(cand.target_symbol)
                fb_live_label = _LIVE_STATUS_LABELS.get(
                    fb_chk.live_status if fb_chk else "DATA_MISSING",
                    "현재가 확인 불가",
                )
                fb_judgment = _LIVE_STATUS_JUDGMENT.get(
                    fb_chk.live_status if fb_chk else "DATA_MISSING",
                    "가격 확인 전 관찰 후보",
                )
                if fb_chk and fb_chk.today_return_pct is not None:
                    fb_ret_sign = "+" if fb_chk.today_return_pct > 0 else ""
                    fb_live_label += f" ({fb_ret_sign}{fb_chk.today_return_pct:.1f}%)"
            else:
                fb_live_label = "라이브 확인 미실행 — 통계만 참고"
                fb_judgment = "가격 확인 전 관찰 후보"
            # live_checks가 None이면 오프라인/테스트 모드 — 통계만 표시
            # live_checks가 있으나 가격 미확인인 경우만 접힘 처리
            if live_checks is not None and "확인 불가" in fb_live_label:
                _no_price_blocked += 1
                continue
            lines.append(f"{i}. {src_label} {sign}{cand.source_return_pct:.1f}%")
            lines.append(f"   → {direction_label}: {wl}{tgt_label}")
            lines.append(f"   - 의미: {meaning}")
            lines.append(
                f"   - 통계: 조건부확률 {cand.conditional_prob:.1%} / {lift_str}"
                f" / 신뢰도 {cand.confidence} ({conf_str})"
            )
            lines.append(f"   - 현재 확인: {fb_live_label}")
            lines.append(f"   - 오늘 볼 것: {watch_str}")
            lines.append(f"   - 판정: {fb_judgment}")
            if cand.confidence == "low":
                lines.append("   ⚠️ 낮은 신뢰도 — 가격/거래량 확인 전까지는 참고용")
        if not medium and low_top:
            lines.append("  (medium 후보 없음 — low 신뢰도 상위 3개만 표시)")

    if _no_price_blocked > 0:
        lines.append(f"  (가격 미확인 통계 후보 {_no_price_blocked}개 — 상세 제외, 가격 확인 후 참고)")
    lines.append("")
    lines.append(f"※ {_RELATION_FEED_DISCLAIMER}")

    return "\n".join(lines)
