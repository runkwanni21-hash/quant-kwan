"""Surge Alert Engine — 실시간 장중 급등감지 + 카탈리스트 규명 + 미반영 종목 LONG/SHORT 후보.

검출 흐름:
1. detect_intraday_surges()  — yfinance 5분봉으로 현재가 vs 장 개시가 비교
2. find_catalyst()           — DART/RSS 헤드라인 + 거래량 발산으로 급등 이유 추정
3. find_unpriced_targets()   — supply-chain 룰 적용 후 아직 미반영된 관련 종목 탐색
4. build_surge_report()      — 텔레그램 포맷 보고서 생성

주의: 매수·매도 확정 표현 금지. 기계적 스크리닝 후보이며 실제 투자 판단은 사용자 책임.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tele_quant.db import Store

log = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────

_SURGE_THRESHOLD_KR = 3.0    # KR 기본 급등 임계 (%)
_SURGE_THRESHOLD_US = 3.0    # US 기본 급등 임계 (%)
_VOLUME_SURGE = 1.5           # 거래량 발산 배수
_UNPRICED_GAP_MIN = 2.0       # 미반영 갭 최소 (%)
_CATALYST_CONFIDENCE_HIGH = 0.7
_CATALYST_CONFIDENCE_MED = 0.4
_DEDUP_HOURS = 2              # 동일 종목 알림 중복 방지 시간

# ── Reason keyword mapping (supply_chain_alpha 와 동기화) ─────────────────────

_CATALYST_KEYWORDS: list[tuple[str, list[str], float]] = [
    # (reason_type, keywords, confidence)
    ("earnings_beat",     ["어닝 서프라이즈", "실적 호조", "어닝 비트", "beat", "순이익 증가", "영업이익 상회"], 0.85),
    ("earnings_miss",     ["실적 쇼크", "어닝 미스", "실적 부진", "miss", "예상 하회"], 0.85),
    ("order_contract",    ["수주", "계약", "공급계약", "contract", "award", "order win"], 0.80),
    ("clinical_success",  ["임상 성공", "FDA 승인", "phase 3", "approved", "NDA", "BLA"], 0.90),
    ("clinical_failure",  ["임상 실패", "FDA 거부", "CRL", "임상 중단", "trial failed", "rejected"], 0.90),
    ("ai_capex",          ["AI", "데이터센터", "GPU", "HBM", "capex", "엔비디아", "hyperscaler", "NVDA"], 0.75),
    ("policy_benefit",    ["정책 수혜", "규제 완화", "보조금", "IRA", "CHIPS", "인프라"], 0.75),
    ("policy_risk",       ["규제 강화", "세금", "제재", "금지", "반독점", "tariff", "sanction"], 0.75),
    ("guidance_up",       ["가이던스 상향", "전망 상향", "outlook raised", "raised guidance"], 0.85),
    ("guidance_down",     ["가이던스 하향", "전망 하향", "lowered guidance", "cut guidance"], 0.85),
    ("product_launch",    ["신제품", "출시", "launch", "출품", "공개"], 0.70),
    ("commodity_price",   ["유가", "철강 가격", "구리", "원자재", "commodity", "WTI"], 0.70),
    ("rate_fx_macro",     ["금리", "기준금리", "환율", "달러", "Fed", "FOMC", "인플레"], 0.65),
    ("sector_cycle",      ["업황 회복", "사이클", "수요 회복", "cycle turn"], 0.65),
]

_REASON_KO: dict[str, str] = {
    "earnings_beat":       "실적 서프라이즈",
    "earnings_miss":       "실적 쇼크",
    "order_contract":      "수주·계약",
    "clinical_success":    "임상·승인 성공",
    "clinical_failure":    "임상 실패",
    "ai_capex":            "AI·데이터센터 투자",
    "policy_benefit":      "정책 수혜",
    "policy_risk":         "정책 리스크",
    "guidance_up":         "가이던스 상향",
    "guidance_down":       "가이던스 하향",
    "product_launch":      "신제품 출시",
    "commodity_price":     "원자재 가격",
    "rate_fx_macro":       "금리·환율·매크로",
    "sector_cycle":        "업황 사이클",
    "volume_surge_only":   "거래량 급증(이유 불명)",
}


# ── Data models ───────────────────────────────────────────────────────────────

@dataclass
class SurgeEvent:
    """장중 급등·급락 감지 종목."""
    symbol: str
    name: str
    market: str          # KR | US
    sector: str
    intraday_pct: float  # 장 개시가 대비 현재 등락률
    volume_ratio: float  # 전일 평균 대비 거래량 배수
    price: float
    prev_close: float
    open_price: float
    detected_at: datetime
    catalyst_type: str = "volume_surge_only"
    catalyst_confidence: float = 0.0
    catalyst_ko: str = ""
    news_headline: str = ""
    direction: str = "BULLISH"   # BULLISH | BEARISH


@dataclass
class UnpricedTarget:
    """미반영 갭 관찰 후보 종목."""
    symbol: str
    name: str
    sector: str
    market: str
    relation_type: str   # BENEFICIARY | VICTIM | PEER_MOMENTUM | SUPPLY_CHAIN_COST
    connection: str      # 연결고리 설명
    rule_id: str
    chain_name: str
    source: SurgeEvent
    current_price: float
    intraday_pct: float  # 이 종목의 현재 장중 등락률
    gap_pct: float       # source 급등 - 이미 반영된 % = 잠재 미반영 갭
    side: str            # LONG | SHORT
    score: float
    reason: str
    chain_tier: int = 1


# ── Universe helpers (relation_feed.py 에서 가져옴) ──────────────────────────

def _get_universe_kr() -> list[str]:
    try:
        from tele_quant.relation_feed import _UNIVERSE_KR
        return list(_UNIVERSE_KR)
    except Exception:
        return []


def _get_universe_us() -> list[str]:
    try:
        from tele_quant.relation_feed import _UNIVERSE_US
        return list(_UNIVERSE_US)
    except Exception:
        return []


# ── Intraday surge detection ──────────────────────────────────────────────────

def _fetch_intraday_single(symbol: str, market: str) -> dict | None:
    """yfinance 5분봉으로 장중 등락률 + 거래량 배수를 계산. None if unavailable."""
    try:
        import yfinance as yf

        ticker = yf.Ticker(symbol)
        # 오늘 5분봉
        intra = ticker.history(period="1d", interval="5m", auto_adjust=True)
        if intra is None or intra.empty or len(intra) < 3:
            return None
        # 전일 종가
        hist = ticker.history(period="5d", interval="1d", auto_adjust=True)
        if hist is None or hist.empty:
            return None

        open_price = float(intra["Open"].iloc[0])
        current_price = float(intra["Close"].iloc[-1])
        prev_close = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else float(hist["Close"].iloc[-1])

        if open_price <= 0 or prev_close <= 0:
            return None

        intraday_pct = (current_price - open_price) / open_price * 100

        # 거래량 비교: 오늘 누적 vs 전일 전체
        today_vol = float(intra["Volume"].sum())
        prev_vol = float(hist["Volume"].iloc[-2]) if len(hist) >= 2 else float(hist["Volume"].iloc[-1])
        # 전일 거래량의 같은 시간 비율로 정규화 (장 총 시간 대비 경과 시간)
        total_minutes_kr = 6 * 60 + 30   # 09:00-15:30 = 6.5h
        elapsed = min(len(intra), total_minutes_kr // 5)  # 5분봉 기준
        total_bars = total_minutes_kr // 5
        volume_ratio = (today_vol / max(prev_vol, 1)) * (total_bars / max(elapsed, 1))

        # Fast name lookup
        try:
            info = ticker.fast_info
            name = getattr(info, "exchange_timezone", None) and symbol  # fallback
            name = symbol
            # try real name
            long_name = getattr(ticker, "info", {}).get("longName") or getattr(ticker, "info", {}).get("shortName") or symbol
            name = long_name
        except Exception:
            name = symbol

        return {
            "symbol": symbol,
            "name": name,
            "market": market,
            "intraday_pct": intraday_pct,
            "volume_ratio": volume_ratio,
            "price": current_price,
            "prev_close": prev_close,
            "open_price": open_price,
        }
    except Exception as exc:
        log.debug("[surge] fetch failed %s: %s", symbol, exc)
        return None


def detect_intraday_surges(
    symbols: list[str] | None = None,
    threshold: float = 3.0,
    market: str = "ALL",
    max_workers: int = 12,
) -> list[SurgeEvent]:
    """장중 급등·급락 종목 탐지."""
    if symbols is None:
        kr = _get_universe_kr() if market in ("KR", "ALL") else []
        us = _get_universe_us() if market in ("US", "ALL") else []
        symbols_with_market: list[tuple[str, str]] = [(s, "KR") for s in kr] + [(s, "US") for s in us]
    else:
        symbols_with_market = []
        for s in symbols:
            m = "KR" if s.endswith((".KS", ".KQ")) else "US"
            if market == "ALL" or m == market:
                symbols_with_market.append((s, m))

    surges: list[SurgeEvent] = []
    now = datetime.now(UTC)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {pool.submit(_fetch_intraday_single, sym, mkt): (sym, mkt) for sym, mkt in symbols_with_market}
        for fut in as_completed(futs):
            sym, mkt = futs[fut]
            try:
                result = fut.result()
            except Exception:
                continue
            if result is None:
                continue

            pct = result["intraday_pct"]
            if abs(pct) < threshold:
                continue

            direction = "BULLISH" if pct > 0 else "BEARISH"
            sector = _guess_sector(sym)

            surges.append(SurgeEvent(
                symbol=sym,
                name=result["name"],
                market=mkt,
                sector=sector,
                intraday_pct=pct,
                volume_ratio=result["volume_ratio"],
                price=result["price"],
                prev_close=result["prev_close"],
                open_price=result["open_price"],
                detected_at=now,
                direction=direction,
            ))

    surges.sort(key=lambda e: -abs(e.intraday_pct))
    log.info("[surge] detected market=%s threshold=%.1f count=%d", market, threshold, len(surges))
    return surges


def _guess_sector(symbol: str) -> str:
    """공급망 룰이나 relation_feed 에서 섹터 추정."""
    try:
        from tele_quant.supply_chain_alpha import load_supply_chain_rules
        rules = load_supply_chain_rules()
        for rule in rules:
            src_syms = {s.get("symbol", "") for s in rule.get("source_symbols", [])}
            all_tgts: list[dict] = (
                rule.get("beneficiaries", [])
                + rule.get("victims_on_bearish", [])
                + rule.get("downstream_victims", [])
                + rule.get("downstream_beneficiaries", [])
            )
            all_syms = src_syms | {t.get("symbol", "") for t in all_tgts}
            if symbol in all_syms:
                return rule.get("chain_name", "기타")
    except Exception:
        pass
    return "기타"


# ── Catalyst identification ───────────────────────────────────────────────────

def find_catalyst(
    surge: SurgeEvent,
    store: Store | None = None,
    dart_api_key: str = "",
    timeout: int = 5,
) -> SurgeEvent:
    """급등 이유 추정: DART 공시 + 최근 뉴스 헤드라인 + 거래량 발산 휴리스틱."""
    texts: list[str] = []
    headline = ""

    # 1) RSS/Store 최근 뉴스 (2H 이내)
    if store is not None:
        try:
            since = datetime.now(UTC) - timedelta(hours=2)
            items = store.recent_items(since=since, limit=200)
            base_sym = surge.symbol.replace(".KS", "").replace(".KQ", "")
            for item in items:
                text = (item.get("title") or "") + " " + (item.get("text") or "")[:200]
                if base_sym.upper() in text.upper() or surge.name[:4] in text:
                    texts.append(text)
                    if not headline:
                        headline = item.get("title") or ""
        except Exception:
            pass

    # 2) DART 당일 공시 검색 (KR만, API 키 필요)
    if not texts and surge.market == "KR" and dart_api_key:
        try:
            import httpx

            base_sym = surge.symbol.replace(".KS", "").replace(".KQ", "")
            today = datetime.now(UTC).strftime("%Y%m%d")
            url = (
                f"https://opendart.fss.or.kr/api/list.json"
                f"?crtfc_key={dart_api_key}&corp_code=&bgn_de={today}&end_de={today}"
                f"&pblntf_ty=B&page_count=20"
            )
            resp = httpx.get(url, timeout=timeout)
            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("list", []):
                    if base_sym in item.get("corp_name", ""):
                        title = item.get("report_nm", "")
                        texts.append(title)
                        if not headline:
                            headline = title
        except Exception:
            pass

    # 3) 키워드 매칭으로 카탈리스트 분류
    combined = " ".join(texts)
    best_type = "volume_surge_only"
    best_conf = 0.0
    for cat_type, kws, conf in _CATALYST_KEYWORDS:
        for kw in kws:
            if kw.lower() in combined.lower():
                if conf > best_conf:
                    best_conf = conf
                    best_type = cat_type
                break

    # 4) 거래량 발산 단독이라도 confidence 최소값 부여
    if best_type == "volume_surge_only":
        if surge.volume_ratio >= 3.0:
            best_conf = 0.35
        elif surge.volume_ratio >= 2.0:
            best_conf = 0.25
        else:
            best_conf = 0.15

    surge.catalyst_type = best_type
    surge.catalyst_confidence = best_conf
    surge.catalyst_ko = _REASON_KO.get(best_type, "기타")
    surge.news_headline = headline[:120] if headline else ""
    return surge


# ── Un-priced target finding ──────────────────────────────────────────────────

def _fetch_current_pct(symbol: str) -> float | None:
    """해당 종목의 현재 장중 등락률(시가 대비)."""
    try:
        import yfinance as yf
        intra = yf.Ticker(symbol).history(period="1d", interval="5m", auto_adjust=True)
        if intra is None or intra.empty:
            return None
        open_p = float(intra["Open"].iloc[0])
        close_p = float(intra["Close"].iloc[-1])
        if open_p <= 0:
            return None
        return (close_p - open_p) / open_p * 100
    except Exception:
        return None


def find_unpriced_targets(
    surges: list[SurgeEvent],
    rules: list[dict] | None = None,
    all_prices: dict[str, float] | None = None,
    min_gap: float = _UNPRICED_GAP_MIN,
) -> list[UnpricedTarget]:
    """공급망 룰로 미반영 관련 종목 탐색."""
    if rules is None:
        from tele_quant.supply_chain_alpha import load_supply_chain_rules
        rules = load_supply_chain_rules()

    targets: list[UnpricedTarget] = []
    seen: set[tuple[str, str]] = set()  # (source, target)

    for surge in surges:
        for rule in rules:
            rule_market = rule.get("market", "BOTH")
            if rule_market not in (surge.market, "BOTH", "CROSS"):
                continue

            src_syms = {s.get("symbol", "") for s in rule.get("source_symbols", [])}
            if surge.symbol not in src_syms:
                # sector keyword match
                matched_kw = False
                for kw in rule.get("source_keywords", []):
                    if kw and kw.lower() in surge.name.lower():
                        matched_kw = True
                        break
                if not matched_kw:
                    continue

            # 방향별 후보 목록 선택
            if surge.direction == "BULLISH":
                candidates = rule.get("beneficiaries", [])
                for c in candidates:
                    _add_unpriced_target(
                        targets, seen, c, surge, rule,
                        relation_type="BENEFICIARY", side="LONG", min_gap=min_gap,
                    )
                peer_list = rule.get("peer_symbols", [])
                for c in peer_list:
                    _add_unpriced_target(
                        targets, seen, c, surge, rule,
                        relation_type="PEER_MOMENTUM", side="LONG", min_gap=min_gap,
                    )
            else:  # BEARISH
                candidates = rule.get("victims_on_bearish", [])
                for c in candidates:
                    _add_unpriced_target(
                        targets, seen, c, surge, rule,
                        relation_type="VICTIM", side="SHORT", min_gap=min_gap,
                    )
                supply_victims = rule.get("downstream_victims", [])
                for c in supply_victims:
                    _add_unpriced_target(
                        targets, seen, c, surge, rule,
                        relation_type="SUPPLY_CHAIN_COST", side="SHORT", min_gap=min_gap,
                    )

    targets.sort(key=lambda t: -t.score)
    return targets


def _fetch_current_price(symbol: str) -> float:
    """종목 최근 종가 조회 (테스트 주입 포인트)."""
    try:
        import yfinance as yf
        hist = yf.Ticker(symbol).history(period="2d", interval="1d", auto_adjust=True)
        return float(hist["Close"].iloc[-1]) if not hist.empty else 0.0
    except Exception:
        return 0.0


def _add_unpriced_target(
    targets: list[UnpricedTarget],
    seen: set[tuple[str, str]],
    candidate: dict,
    surge: SurgeEvent,
    rule: dict,
    relation_type: str,
    side: str,
    min_gap: float,
) -> None:
    sym = candidate.get("symbol", "")
    if not sym or sym == surge.symbol:
        return
    key = (surge.symbol, sym)
    if key in seen:
        return
    seen.add(key)

    # 현재 장중 등락률 조회
    current_pct = _fetch_current_pct(sym)
    if current_pct is None:
        current_pct = 0.0

    # 갭 계산: source 급등폭에서 이미 반영된 만큼 차감
    expected_move = abs(surge.intraday_pct) * candidate.get("lag_sensitivity", 0.6)
    gap_pct = expected_move - abs(current_pct)
    if gap_pct < min_gap:
        return

    # 가격 조회
    current_price = _fetch_current_price(sym)

    # 스코어 = 기본 점수 + 갭 보너스 + 카탈리스트 신뢰도 보너스
    base_score = 60.0
    if relation_type in ("BENEFICIARY", "SUPPLY_CHAIN_COST", "VICTIM"):
        base_score = 75.0
    elif relation_type == "LAGGING_BENEFICIARY":
        base_score = 65.0

    score = base_score + min(gap_pct * 1.5, 15.0) + surge.catalyst_confidence * 10

    # 카탈리스트 신뢰도 낮으면 페널티
    if surge.catalyst_type == "volume_surge_only":
        score -= 10.0

    chain_tier_map = {
        "BENEFICIARY": 1, "SUPPLY_CHAIN_COST": 1, "VICTIM": 1,
        "DEMAND_SLOWDOWN": 1, "LAGGING_BENEFICIARY": 2, "PEER_MOMENTUM": 3,
    }

    connection = candidate.get("connection", rule.get("chain_name", ""))
    name = candidate.get("name", sym)
    sector = candidate.get("sector", rule.get("chain_name", "기타"))
    mkt = "KR" if sym.endswith((".KS", ".KQ")) else "US"

    reason_parts = [
        f"{surge.name}({surge.symbol}) {surge.intraday_pct:+.1f}% [{surge.catalyst_ko}]",
        f"→ {connection}",
        f"갭={gap_pct:+.1f}% (기대:{expected_move:.1f}% vs 현재:{current_pct:+.1f}%)",
    ]
    if surge.news_headline:
        reason_parts.append(f"헤드라인: {surge.news_headline[:80]}")

    targets.append(UnpricedTarget(
        symbol=sym,
        name=name,
        sector=sector,
        market=mkt,
        relation_type=relation_type,
        connection=connection,
        rule_id=rule.get("id", ""),
        chain_name=rule.get("chain_name", ""),
        source=surge,
        current_price=current_price,
        intraday_pct=current_pct,
        gap_pct=gap_pct,
        side=side,
        score=score,
        reason=" | ".join(reason_parts),
        chain_tier=chain_tier_map.get(relation_type, 2),
    ))


# ── Report builder ────────────────────────────────────────────────────────────

def build_surge_report(
    surges: list[SurgeEvent],
    targets: list[UnpricedTarget],
    market: str = "ALL",
) -> str:
    if not surges and not targets:
        return ""

    kst_now = datetime.now(UTC).strftime("%m/%d %H:%M") + " UTC"
    lines: list[str] = [f"⚡ 급등감지 리포트 [{market}] — {kst_now}\n"]

    # ── 섹션1: 급등 종목 ─────────────────────────────────────────────────
    if surges:
        lines.append("[ 장중 급등·급락 감지 ]")
        for ev in surges[:8]:
            arrow = "▲" if ev.direction == "BULLISH" else "▼"
            vol_str = f"거래량x{ev.volume_ratio:.1f}" if ev.volume_ratio else ""
            cat_str = f"[{ev.catalyst_ko}]" if ev.catalyst_ko and ev.catalyst_ko != "거래량 급증(이유 불명)" else "[이유 불명]"
            conf_str = f"확신도:{ev.catalyst_confidence:.0%}" if ev.catalyst_confidence > 0 else ""
            line = f"{arrow} {ev.name}({ev.symbol}) {ev.intraday_pct:+.1f}% {cat_str}"
            if vol_str:
                line += f"  {vol_str}"
            if conf_str:
                line += f"  {conf_str}"
            lines.append(line)
            if ev.news_headline:
                lines.append(f"   ↳ {ev.news_headline[:100]}")
        lines.append("")

    # ── 섹션2: 미반영 LONG 후보 ─────────────────────────────────────────
    long_tgts = [t for t in targets if t.side == "LONG"][:6]
    if long_tgts:
        lines.append("[ 미반영 LONG 관찰 후보 — 공개 정보 기반 리서치 보조 ]")
        for t in long_tgts:
            tier_str = f"T{t.chain_tier}" if t.chain_tier else ""
            lines.append(
                f"  ▶ {t.name}({t.symbol}) [{t.market}] 현재{t.intraday_pct:+.1f}%  갭≈{t.gap_pct:+.1f}%  {tier_str}"
            )
            lines.append(f"     {t.reason[:140]}")
        lines.append("")

    # ── 섹션3: 미반영 SHORT 후보 ────────────────────────────────────────
    short_tgts = [t for t in targets if t.side == "SHORT"][:4]
    if short_tgts:
        lines.append("[ 미반영 SHORT 관찰 후보 ]")
        for t in short_tgts:
            tier_str = f"T{t.chain_tier}" if t.chain_tier else ""
            lines.append(
                f"  ▼ {t.name}({t.symbol}) [{t.market}] 현재{t.intraday_pct:+.1f}%  갭≈{t.gap_pct:+.1f}%  {tier_str}"
            )
            lines.append(f"     {t.reason[:140]}")
        lines.append("")

    lines.append("※ 기계적 스크리닝 보조 — 최종 판단은 사용자 책임")
    return "\n".join(lines)


# ── DB deduplication ─────────────────────────────────────────────────────────

def _is_recently_alerted(store: Store, symbol: str, hours: int = _DEDUP_HOURS) -> bool:
    """최근 N시간 내 동일 종목 급등 알림이 있었으면 True."""
    try:
        since = datetime.now(UTC) - timedelta(hours=hours)
        with store.connect() as conn:
            row = conn.execute(
                "SELECT id FROM surge_events WHERE symbol=? AND created_at>? LIMIT 1",
                (symbol, since.isoformat()),
            ).fetchone()
            return row is not None
    except Exception:
        return False


def _save_surge_events(store: Store, surges: list[SurgeEvent], targets: list[UnpricedTarget]) -> None:
    now = datetime.now(UTC).isoformat()
    try:
        with store.connect() as conn:
            for ev in surges:
                conn.execute(
                    """INSERT OR IGNORE INTO surge_events
                    (created_at, symbol, name, market, sector, intraday_pct,
                     volume_ratio, price, prev_close, open_price,
                     catalyst_type, catalyst_confidence, catalyst_ko, news_headline, direction)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        now, ev.symbol, ev.name, ev.market, ev.sector,
                        ev.intraday_pct, ev.volume_ratio, ev.price, ev.prev_close, ev.open_price,
                        ev.catalyst_type, ev.catalyst_confidence, ev.catalyst_ko,
                        ev.news_headline, ev.direction,
                    ),
                )
            for t in targets:
                conn.execute(
                    """INSERT OR IGNORE INTO surge_targets
                    (created_at, source_symbol, target_symbol, target_name,
                     target_market, relation_type, connection, rule_id, chain_name,
                     current_price, intraday_pct, gap_pct, side, score, chain_tier, reason)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        now, t.source.symbol, t.symbol, t.name,
                        t.market, t.relation_type, t.connection, t.rule_id, t.chain_name,
                        t.current_price, t.intraday_pct, t.gap_pct, t.side, t.score, t.chain_tier,
                        t.reason[:400],
                    ),
                )
            conn.commit()
    except Exception as exc:
        log.warning("[surge] DB save failed: %s", exc)


# ── Market hours guard ──────────────────────────────────────────────────────

def is_market_open(market: str) -> bool:
    """UTC 기준으로 시장 개장 여부 확인. 공휴일 미고려."""
    now_utc = datetime.now(UTC)
    weekday = now_utc.weekday()  # 0=Mon, 6=Sun
    if weekday >= 5:  # 주말
        return False
    hour_utc = now_utc.hour + now_utc.minute / 60

    if market == "KR":
        # KST 09:00-15:30 = UTC 00:00-06:30
        return 0.0 <= hour_utc < 6.5
    elif market == "US":
        # ET 09:30-16:00 = UTC 13:30-20:00 (EDT), 14:30-21:00 (EST)
        return 13.5 <= hour_utc < 20.5
    else:  # ALL
        return is_market_open("KR") or is_market_open("US")


# ── Main orchestrator ────────────────────────────────────────────────────────

def run_surge_scan(
    market: str = "ALL",
    threshold: float = 3.0,
    dart_api_key: str = "",
    max_workers: int = 12,
    store: Store | None = None,
    skip_dedup: bool = False,
) -> tuple[list[SurgeEvent], list[UnpricedTarget]]:
    """전체 흐름 오케스트레이터: 탐지 → 카탈리스트 → 미반영 갭 분석."""
    # 1. 급등 탐지
    surges = detect_intraday_surges(threshold=threshold, market=market, max_workers=max_workers)
    if not surges:
        return [], []

    # 2. 중복 알림 제거
    if store is not None and not skip_dedup:
        surges = [s for s in surges if not _is_recently_alerted(store, s.symbol)]
    if not surges:
        log.info("[surge] all surges already alerted recently — skipping")
        return [], []

    # 3. 카탈리스트 파악
    enriched: list[SurgeEvent] = []
    for surge in surges:
        enriched.append(find_catalyst(surge, store=store, dart_api_key=dart_api_key))

    # 4. 미반영 갭 탐색
    from tele_quant.supply_chain_alpha import load_supply_chain_rules
    rules = load_supply_chain_rules()
    targets = find_unpriced_targets(enriched, rules=rules)

    # 5. DB 저장
    if store is not None:
        _save_surge_events(store, enriched, targets)

    return enriched, targets
