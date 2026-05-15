from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from tele_quant.analysis.models import TradeScenario
    from tele_quant.evidence_ranker import RankedEvidencePack
    from tele_quant.models import RunStats

_URL_RE = re.compile(r"https?://\S+|t\.me/\S+", re.IGNORECASE)
_FORBIDDEN_RE = re.compile(r"무조건\s*매수|반드시\s*상승|확정\s*수익|Buy\s*Now", re.IGNORECASE)

_MACRO_POS_KW: frozenset[str] = frozenset(
    [
        "금리인하",
        "고용호조",
        "환율안정",
        "경기회복",
        "무역타결",
        "유가하락",
        "정책호재",
        "고용증가",
        "낙관",
        "완화",
        "피봇",
    ]
)
_MACRO_NEG_KW: frozenset[str] = frozenset(
    [
        "금리인상",
        "금리상승",
        "PCE",
        "CPI",
        "관세",
        "지정학",
        "유가급등",
        "실업증가",
        "인플레이션",
        "긴축",
        "경기침체",
        "무역전쟁",
    ]
)

_MACRO_RATE_KW: frozenset[str] = frozenset(["금리", "FOMC", "연준", "연방준비"])
_MACRO_FX_KW: frozenset[str] = frozenset(["환율", "달러", "원화", "엔화"])
_MACRO_OIL_KW: frozenset[str] = frozenset(["유가", "원유", "WTI", "브렌트"])
_MACRO_EMPLOY_KW: frozenset[str] = frozenset(["고용", "실업", "고용지표"])
_MACRO_PRICE_KW: frozenset[str] = frozenset(["물가", "인플레이션", "CPI", "PCE"])

_SECTOR_KW: dict[str, list[str]] = {
    "AI/반도체": ["AI", "HBM", "반도체", "엔비디아", "NVDA", "TSMC", "GPU", "칩"],
    "바이오": ["바이오", "제약", "임상", "FDA", "CMO", "헬스케어", "신약"],
    "조선/방산": ["조선", "방산", "LNG선", "해양플랜트", "한화에어로", "한화"],
    "2차전지": ["배터리", "2차전지", "리튬", "전고체", "음극재", "양극재", "에코프로"],
    "금융": ["금융", "은행", "보험", "금리", "증권"],
    "자동차/EV": ["현대차", "기아", "EV", "전기차"],
    "원전/에너지": ["원전", "원자력", "두산에너빌", "에너지"],
    "로봇/AI로봇": ["로봇", "협동로봇", "두산로보틱스"],
}

_CATEGORY_KW: dict[str, list[str]] = {
    "세계경제": ["금리", "FOMC", "연준", "관세", "GDP", "환율", "달러", "무역"],
    "기술/AI": ["AI", "반도체", "HBM", "GPU", "NVDA", "엔비디아", "칩", "데이터센터"],
    "바이오": ["바이오", "제약", "임상", "FDA", "CMO", "헬스케어", "신약", "의약"],
    "정책/규제": ["규제", "제재", "법안", "정책", "행정", "허가", "승인"],
    "산업/에너지": ["조선", "방산", "LNG", "원전", "원자력", "유가", "에너지", "배터리"],
    "소비": ["소비", "소매", "유통", "리테일", "명품", "여행", "항공"],
}

_CHECKPOINT_KW: list[tuple[str, str]] = [
    ("FOMC", "FOMC 일정"),
    ("CPI", "CPI 발표"),
    ("PCE", "PCE 발표"),
    ("실적", "실적 발표"),
    ("환율", "환율 동향"),
    ("유가", "유가 흐름"),
    ("고용", "고용 지표"),
    ("GDP", "GDP 발표"),
    ("물가", "물가 지표"),
]


def _strip_urls(text: str) -> str:
    return _URL_RE.sub("", text).strip()


def _one_sentence(text: str, max_len: int = 80) -> str:
    from tele_quant.headline_cleaner import clean_source_header

    text = clean_source_header(text)
    text = _strip_urls(text).replace("\n", " ").strip()
    text = _FORBIDDEN_RE.sub("", text).strip()
    if len(text) > max_len:
        cut = text[:max_len].rsplit(" ", 1)[0]
        text = (cut or text[:max_len]) + "…"
    return text


def _hours_label(h: float) -> str:
    return f"{int(h)}시간" if h == int(h) else f"{h:.1f}시간"


def _current_hour(timezone: str = "Asia/Seoul") -> int:
    try:
        return datetime.now(ZoneInfo(timezone)).hour
    except Exception:
        return datetime.now(UTC).hour


def _detect_strong_sectors(pack: RankedEvidencePack) -> list[str]:
    all_text = " ".join(c.headline + " " + c.summary_hint for c in pack.positive_stock + pack.macro)
    return [sector for sector, kws in _SECTOR_KW.items() if any(kw in all_text for kw in kws)]


def _detect_weak_sectors(pack: RankedEvidencePack) -> list[str]:
    all_text = " ".join(c.headline + " " + c.summary_hint for c in pack.negative_stock)
    return [sector for sector, kws in _SECTOR_KW.items() if any(kw in all_text for kw in kws)]


def _time_focus_label(hour: int) -> str:
    """시간대별 리포트 초점 문구."""
    if 6 <= hour < 9:
        return "아침 브리핑: 전일 미국장 + 한국장 개장 전 체크 / 환율·금리·유가"
    elif 9 <= hour < 13:
        return "오전장 점검: 한국장 오전 수급 / 특징주 / 새 리포트"
    elif 13 <= hour < 16:
        return "장마감 전 점검: 한국장 마감 전 수급 / 당일 강한 섹터 / 종가 기준"
    elif 16 <= hour < 21:
        return "저녁 점검: 한국장 마감 정리 / 유럽장·미국장 프리마켓 / 다음날 체크"
    elif 21 <= hour < 24:
        return "미국장 개장 체크: 개장 전 뉴스 / 빅테크·반도체·ETF / 프리마켓 급등락"
    else:
        return "미국장 중반 체크: 미국장 흐름 / 금리·달러·유가 / 익일 한국장 영향"


def _build_macro_rate_summary(pack: RankedEvidencePack) -> list[str]:
    """금리/환율/유가/고용/물가 초보자 친화적 요약."""
    mac_text = " ".join(c.headline + " " + c.summary_hint for c in pack.macro)
    items: list[str] = []

    if any(kw in mac_text for kw in _MACRO_RATE_KW):
        if any(kw in mac_text for kw in ("금리인하", "피봇", "완화", "인하 기대", "금리 인하")):
            detail = "인하 기대 → 성장주·위험자산에 우호"
        elif any(kw in mac_text for kw in ("금리인상", "긴축", "금리 상승", "금리인상")):
            detail = "상승 압력 → 성장주 밸류에이션 부담"
        else:
            detail = "이슈 감지 → 성장주 밸류에이션 체크"
        items.append(f"금리: {detail}")

    if any(kw in mac_text for kw in _MACRO_FX_KW):
        if any(kw in mac_text for kw in ("원화 강세", "달러 약세", "원화강세")):
            detail = "원화 강세 → 외국인 수급 개선 가능"
        elif any(kw in mac_text for kw in ("원화 약세", "달러 강세", "원화약세", "달러강세")):
            detail = "원화 약세/달러 강세 → 외국인 수급과 반도체/자동차 환율 효과 체크"
        else:
            detail = "변동 이슈 감지 → 외국인 수급·수출 기업 영향 체크"
        items.append(f"환율: {detail}")

    if any(kw in mac_text for kw in _MACRO_OIL_KW):
        if any(kw in mac_text for kw in ("유가 상승", "유가급등", "원유 상승", "WTI 상승")):
            detail = "상승 이슈 → 항공/화학 비용 부담, 에너지주 수혜 가능"
        elif any(kw in mac_text for kw in ("유가 하락", "유가급락", "원유 하락", "WTI 하락")):
            detail = "하락 이슈 → 항공/화학 비용 감소, 에너지주 부담"
        else:
            detail = "변동 이슈 감지 → 항공·화학·에너지 영향 체크"
        items.append(f"유가: {detail}")

    if any(kw in mac_text for kw in _MACRO_EMPLOY_KW):
        if any(kw in mac_text for kw in ("고용호조", "고용증가", "고용 강", "비농업", "NFP")):
            detail = "강함 → 금리 인하 기대 약화 가능"
        elif any(kw in mac_text for kw in ("실업 증가", "고용 둔화", "고용 약", "실업률 상승")):
            detail = "약함 → 금리 인하 기대 강화 가능"
        else:
            detail = "지표 대기 → 금리 인하 기대 변화 가능"
        items.append(f"고용: {detail}")

    if any(kw in mac_text for kw in _MACRO_PRICE_KW):
        if any(kw in mac_text for kw in ("물가 상승", "CPI 상회", "PCE 상회", "인플레이션 상승")):
            detail = "상승 이슈 → 연준 긴축 우려, 성장주 부담"
        elif any(kw in mac_text for kw in ("물가 하락", "CPI 하회", "PCE 하회", "디플레이션")):
            detail = "하락 이슈 → 연준 완화 기대"
        else:
            detail = "CPI/PCE 경계 → 연준 정책 민감"
        items.append(f"물가: {detail}")

    return items


def _safe_headline(cluster: Any) -> str:
    """Get a clean, forbidden-expression-free headline for display."""
    from tele_quant.headline_cleaner import summarize_issue_for_display

    raw = summarize_issue_for_display(cluster)
    if raw == "핵심 내용 추가 확인 필요":
        raw = _one_sentence(cluster.headline)
    else:
        raw = _FORBIDDEN_RE.sub("", raw).strip()
    return raw


def _compute_sector_sentiments(pack: RankedEvidencePack) -> dict[str, dict]:
    """Compute per-sector sentiment data from ranked evidence pack.

    Returns dict[sector_name → {score, bullish, bearish, novelty, events, sources, confidence}]
    where score is 0-100 (50 = neutral, >50 = bullish, <50 = bearish).
    """
    result: dict[str, dict] = {}
    all_clusters = pack.positive_stock + pack.negative_stock + pack.macro

    for sector, kws in _CATEGORY_KW.items():
        bull = sum(
            1
            for c in pack.positive_stock
            if any(kw in (c.headline + " " + c.summary_hint) for kw in kws)
        )
        bear = sum(
            1
            for c in pack.negative_stock
            if any(kw in (c.headline + " " + c.summary_hint) for kw in kws)
        )
        total = bull + bear
        if total == 0:
            continue
        score = (bull / total) * 100.0
        sources = sum(
            c.source_count
            for c in all_clusters
            if any(kw in (c.headline + " " + c.summary_hint) for kw in kws)
        )
        novelty = sum(
            1
            for c in all_clusters
            if c.source_count == 1 and any(kw in (c.headline + " " + c.summary_hint) for kw in kws)
        )
        events = [
            c.headline[:50]
            for c in (pack.positive_stock + pack.negative_stock)
            if any(kw in (c.headline + " " + c.summary_hint) for kw in kws)
        ][:3]
        confidence = "high" if total >= 3 else "medium" if total == 2 else "low"
        result[sector] = {
            "score": round(score, 1),
            "bullish": bull,
            "bearish": bear,
            "novelty": novelty,
            "events": events,
            "sources": sources,
            "confidence": confidence,
        }
    return result


def _build_sentiment_radar_section(
    pack: RankedEvidencePack,
    prev_sector_sentiments: dict[str, dict] | None = None,
) -> str:
    """Build 🧭 4시간 시장 감성 레이더 section text."""
    pos_cnt = len(pack.positive_stock)
    neg_cnt = len(pack.negative_stock)
    total_cnt = pos_cnt + neg_cnt
    overall_score = 50.0 if total_cnt == 0 else (pos_cnt / total_cnt) * 100.0
    net_score = pos_cnt - neg_cnt  # net positive count

    if overall_score >= 65:
        mood_label, mood_icon = "긍정 우세", "🟢"
    elif overall_score >= 55:
        mood_label, mood_icon = "소폭 긍정", "🟡"
    elif overall_score >= 45:
        mood_label, mood_icon = "중립 혼조", "⚪"
    elif overall_score >= 35:
        mood_label, mood_icon = "소폭 부정", "🟠"
    else:
        mood_label, mood_icon = "부정 우세", "🔴"

    net_str = f"+{net_score}" if net_score > 0 else str(net_score)
    lines = [
        "4.5 🧭 시장 감성 레이더",
        f"{mood_icon} 전체 감성: {mood_label}  점수 {overall_score:.0f}/100  순감성 {net_str}",
    ]

    sector_data = _compute_sector_sentiments(pack)
    if sector_data:
        lines.append("섹터별:")
        for sector, data in sorted(sector_data.items(), key=lambda x: -x[1]["score"]):
            bull, bear = data["bullish"], data["bearish"]
            sc = data["score"]
            icon = "⬆" if sc >= 60 else "⬇" if sc <= 40 else "➡"
            label = "강세" if sc >= 60 else "약세" if sc <= 40 else "중립"
            conf = " (저신뢰)" if data["confidence"] == "low" else ""
            lines.append(f"  {icon} {sector}: {label} (호재 {bull}건 / 악재 {bear}건){conf}")

    # 주요동력 / 부담 — top 2 positive / negative cluster headlines
    drivers = [_one_sentence(c.headline, max_len=45) for c in pack.positive_stock[:2] if c.headline]
    burdens = [_one_sentence(c.headline, max_len=45) for c in pack.negative_stock[:2] if c.headline]
    if drivers:
        lines.append(f"  주요동력: {' / '.join(drivers)}")
    if burdens:
        lines.append(f"  주요부담: {' / '.join(burdens)}")

    # Sentiment delta vs previous — rising/cooling sectors
    if prev_sector_sentiments:
        rising: list[str] = []
        cooling: list[str] = []
        for sector, data in sector_data.items():
            prev = prev_sector_sentiments.get(sector)
            if prev:
                delta = data["score"] - prev["score"]
                if delta >= 10:
                    rising.append(f"{sector} ↑{delta:.0f}점")
                elif delta <= -10:
                    cooling.append(f"{sector} ↓{abs(delta):.0f}점")
        if rising:
            lines.append(f"  🔥 상승섹터: {', '.join(rising[:3])}")
        if cooling:
            lines.append(f"  🧊 냉각섹터: {', '.join(cooling[:3])}")

    return "\n".join(lines)


def _build_category_news_section(pack: RankedEvidencePack) -> str:
    """카테고리별 주요 뉴스 요약 (반도체/AI/바이오/세계경제 등)."""
    all_clusters = pack.positive_stock + pack.negative_stock + pack.macro
    if not all_clusters:
        return ""

    # Group clusters into categories using _CATEGORY_KW
    cat_hits: dict[str, list[tuple[str, str]]] = {cat: [] for cat in _CATEGORY_KW}
    seen_headlines: set[str] = set()

    for cluster in all_clusters:
        text = (cluster.headline + " " + cluster.summary_hint).lower()
        pol_icon = "🟢" if cluster.polarity == "positive" else ("🔴" if cluster.polarity == "negative" else "⚪")
        headline = _one_sentence(cluster.headline, max_len=50)
        if not headline or headline in seen_headlines:
            continue
        for cat, kws in _CATEGORY_KW.items():
            if any(kw.lower() in text for kw in kws):
                if len(cat_hits[cat]) < 2:
                    cat_hits[cat].append((pol_icon, headline))
                    seen_headlines.add(headline)
                break

    active_cats = [(cat, hits) for cat, hits in cat_hits.items() if hits]
    if not active_cats:
        return ""

    lines = ["4.4 📰 카테고리별 주요 뉴스"]
    for cat, hits in active_cats:
        lines.append(f"  [{cat}]")
        for icon, hl in hits:
            lines.append(f"    {icon} {hl}")
    return "\n".join(lines)


def _build_beginner_action_section(pack: RankedEvidencePack) -> str:
    """초보자 행동 가이드: 시장 분위기 + 섹터 신호등 + 지금 할 일."""
    pos = len(pack.positive_stock)
    neg = len(pack.negative_stock)
    total = pos + neg

    if total == 0:
        mood_icon, mood_label = "🟡", "중립 관망"
    elif pos >= neg * 2:
        mood_icon, mood_label = "🟢", "전반 긍정 — 선별적 관심 가능"
    elif neg >= pos * 2:
        mood_icon, mood_label = "🔴", "전반 부정 — 비중 축소 또는 관망"
    else:
        mood_icon, mood_label = "🟡", "혼조 — 종목별 선별 접근"

    strong = _detect_strong_sectors(pack)
    weak = _detect_weak_sectors(pack)
    mixed = [s for s in strong if s in weak]
    strong_only = [s for s in strong if s not in weak]
    weak_only = [s for s in weak if s not in strong]

    lines = [
        "📌 초보자 행동 가이드",
        f"시장 분위기: {mood_icon} {mood_label}",
    ]

    if strong_only:
        lines.append(f"🟢 관심 섹터: {' / '.join(strong_only[:3])}")
    if mixed:
        lines.append(f"🟡 혼조 섹터: {' / '.join(mixed[:2])}")
    if weak_only:
        lines.append(f"🔴 약세 섹터: {' / '.join(weak_only[:2])}")

    # 핵심 이슈 1개
    top_pos = next(
        (c for c in pack.positive_stock if c.headline and len(c.headline) > 5), None
    )
    top_neg = next(
        (c for c in pack.negative_stock if c.headline and len(c.headline) > 5), None
    )
    if top_pos:
        lines.append(f"지금 눈여겨볼 것: {_one_sentence(top_pos.headline, 70)}")
    if top_neg:
        lines.append(f"지금 조심할 것: {_one_sentence(top_neg.headline, 70)}")

    lines.append(
        "※ 이 가이드는 공개 정보 기반 참고용이며 투자 결정은 본인 책임입니다."
    )
    return "\n".join(lines)


def build_macro_digest(
    pack: RankedEvidencePack,
    market_snapshot: list[dict[str, Any]],
    stats: RunStats,
    hours: float,
    watchlist_cfg: Any = None,
    timezone: str = "Asia/Seoul",
    macro_only: bool = False,
    relation_feed: Any = None,
    scenarios: Any = None,
    prev_sector_sentiments: dict[str, dict] | None = None,
    market_narrative: str = "",
    external_data: dict[str, Any] | None = None,
    settings: Any = None,
) -> str:
    """4시간 투자 브리핑 형태의 deterministic digest를 생성한다."""

    label = _hours_label(hours)
    selected = pack.total_count - pack.dropped_count
    hour = _current_hour(timezone)

    if macro_only:
        title = "🧠 Tele Quant 주말 매크로 브리핑"
        mode_notice = "주말 모드: 종목 롱/숏 시나리오는 중단하고 매크로·섹터 흐름만 추적"
    else:
        title = f"🧠 Tele Quant {label} 투자 브리핑"
        mode_notice = ""

    lines: list[str] = [
        title,
        (
            f"수집: 텔레그램 {stats.telegram_items}건 · 네이버 {stats.report_items}건"
            f" · 증거묶음 {pack.total_count}개 → 선별 {selected}개"
        ),
    ]
    if mode_notice:
        lines.append(f"📌 {mode_notice}")
    lines.append(f"🕒 이번 리포트 초점: {_time_focus_label(hour)}")
    lines.append("")

    # 0-A. 초보자 행동 가이드 (항상 표시)
    try:
        beginner_section = _build_beginner_action_section(pack)
        lines.append(beginner_section)
        lines.append("")
    except Exception:
        pass

    # 0-B. AI가 읽은 4시간 뉴스 (fast mode에서 Ollama 전처리 결과)
    if market_narrative:
        lines.append("📰 AI가 읽은 4시간 뉴스")
        for nl in market_narrative.splitlines():
            if nl.strip():
                lines.append(nl)
        lines.append("")

    # 0-C. 시장 심리 & 실시간 매크로 지표
    if external_data:
        try:
            from tele_quant.external_indicators import (
                format_energy_line,
                format_exchange_rate_line,
                format_fear_greed_line,
                format_fred_lines,
            )

            indicator_lines: list[str] = []

            # 공포탐욕지수
            fg = external_data.get("fear_greed")
            if fg:
                indicator_lines.append(f"공포탐욕지수: {format_fear_greed_line(fg)}")

            # FRED / yfinance 매크로 (금리, DXY, VIX 등)
            fred = external_data.get("fred") or {}
            indicator_lines.extend(format_fred_lines(fred))

            # ECOS 한국은행
            ecos = external_data.get("ecos") or {}
            if ecos:
                try:
                    from tele_quant.ecos_client import format_ecos_lines

                    indicator_lines.extend(format_ecos_lines(ecos))
                except Exception:
                    pass

            # EIA 에너지 가격
            energy = external_data.get("energy") or {}
            if energy:
                e_line = format_energy_line(energy)
                if e_line:
                    indicator_lines.append(f"에너지: {e_line}")

            # ECB 금리
            ecb_rate = external_data.get("ecb_rate")
            if ecb_rate is not None:
                indicator_lines.append(f"ECB 예금금리: {ecb_rate:.2f}%")

            # Frankfurter 환율 (yfinance 대비 cross-check)
            fx_rates = external_data.get("exchange_rates") or {}
            if fx_rates:
                fx_line = format_exchange_rate_line(fx_rates)
                if fx_line:
                    indicator_lines.append(f"환율(실시간): {fx_line}")

            if indicator_lines:
                lines.append("📊 시장 심리 & 실시간 지표")
                for il in indicator_lines:
                    lines.append(f"- {il}")
                lines.append("")
        except Exception:
            pass

    # 0-D. 경제 캘린더 (향후 주요 일정)
    if settings is not None:
        try:
            from tele_quant.economic_calendar import build_calendar_section

            _lookahead = getattr(settings, "economic_calendar_lookahead_days", 14)
            _cal_section = build_calendar_section(settings, lookahead_days=_lookahead)
            if _cal_section:
                lines.append(_cal_section)
                lines.append("")
        except Exception:
            pass

    # 1. 한 줄 결론
    pos_cnt, neg_cnt, mac_cnt = len(pack.positive_stock), len(pack.negative_stock), len(pack.macro)
    if pos_cnt > neg_cnt * 2:
        mood = "전반적 호재 우세, 리스크 모니터링 병행 권장"
    elif neg_cnt > pos_cnt * 2:
        mood = "전반적 악재 우세, 비중 축소·관망 우선"
    elif mac_cnt > 5:
        mood = "매크로 변수 다수 집중, 방향성 확인 대기"
    else:
        mood = "호재·악재 혼재, 선별적 접근 권장"

    lines += ["1️⃣ 한 줄 결론", f"- {mood}", ""]

    # 2. 직전 리포트 대비 변화 (현재는 텍스트 기반 간략 표시)
    lines.append("2️⃣ 🔁 직전 리포트 대비 변화")
    new_pos_headlines = [_one_sentence(c.headline) for c in pack.positive_stock[:3] if c.headline]
    new_neg_headlines = [_one_sentence(c.headline) for c in pack.negative_stock[:3] if c.headline]

    if new_pos_headlines:
        lines.append(f"- 새로 뜬 호재: {new_pos_headlines[0]}")
        for h in new_pos_headlines[1:]:
            lines.append(f"  +{h}")
    else:
        lines.append("- 새로 뜬 호재: 없음")

    if new_neg_headlines:
        lines.append(f"- 새로 뜬 악재: {new_neg_headlines[0]}")
        for h in new_neg_headlines[1:]:
            lines.append(f"  +{h}")
    else:
        lines.append("- 새로 뜬 악재: 없음")

    # 반복 이슈 감지 (tickers가 2건 이상인 클러스터)
    repeat_issues = [
        _one_sentence(c.headline)
        for c in (pack.positive_stock + pack.negative_stock)
        if c.source_count >= 2 and c.headline
    ]
    if repeat_issues:
        lines.append(f"- 계속 반복되는 이슈: {repeat_issues[0]}")
    lines.append("")

    # 3. 매크로 온도
    lines.append("3️⃣ 🌍 매크로 온도")

    rate_summary = _build_macro_rate_summary(pack)
    if rate_summary:
        for item in rate_summary:
            lines.append(f"  - {item}")
    else:
        lines.append("  - 특이 매크로 없음")

    good_macro: list[str] = []
    bad_macro: list[str] = []

    for c in pack.macro[:8]:
        text = c.headline + " " + c.summary_hint
        line = _safe_headline(c)
        if not line:
            continue
        if any(kw in text for kw in _MACRO_POS_KW):
            good_macro.append(line)
        elif any(kw in text for kw in _MACRO_NEG_KW):
            bad_macro.append(line)

    if good_macro:
        lines.append("  호재 매크로:")
        for item in good_macro[:2]:
            lines.append(f"  + {item}")
    if bad_macro:
        lines.append("  위험 매크로:")
        for item in bad_macro[:2]:
            lines.append(f"  - {item}")
    lines.append("")

    # 4. 섹터 온도판
    lines.append("4️⃣ 📊 섹터 온도판")
    strong_sectors = _detect_strong_sectors(pack)
    weak_sectors = _detect_weak_sectors(pack)
    all_sectors = set(_SECTOR_KW.keys())
    mixed_sectors = [s for s in all_sectors if s in strong_sectors and s in weak_sectors]
    strong_only = [s for s in strong_sectors if s not in weak_sectors]
    weak_only = [s for s in weak_sectors if s not in strong_sectors]

    if strong_only:
        lines.append(f"  강세: {', '.join(strong_only[:4])}")
    if mixed_sectors:
        lines.append(f"  혼조: {', '.join(mixed_sectors[:3])}")
    if weak_only:
        lines.append(f"  약세: {', '.join(weak_only[:3])}")
    if not strong_only and not weak_only and not mixed_sectors:
        lines.append("  - 특정 섹터 신호 없음")
    lines.append("")

    # 4.4. 카테고리별 주요 뉴스
    try:
        cat_news = _build_category_news_section(pack)
        if cat_news:
            lines.append(cat_news)
            lines.append("")
    except Exception:
        pass

    # 4.5. 시장 감성 레이더
    try:
        radar = _build_sentiment_radar_section(pack, prev_sector_sentiments)
        lines.append(radar)
        lines.append("")
    except Exception:
        pass

    # 4.6. 급등·급락 → 후행 후보 (자체 계산 relation feed)
    if relation_feed is not None:
        try:
            from tele_quant.relation_feed import build_relation_feed_section

            watchlist_syms: set[str] = set()
            if watchlist_cfg is not None:
                try:
                    from tele_quant.watchlist import is_watchlist_symbol

                    for grp in watchlist_cfg.groups.values():
                        for sym in grp.symbols:
                            watchlist_syms.add(sym)
                except Exception:
                    pass

            rf_section = build_relation_feed_section(
                relation_feed,
                watchlist_symbols=watchlist_syms,
                macro_only=macro_only,
            )
            if rf_section:
                lines.append(rf_section)
                lines.append("")
        except Exception as _rf_exc:
            import logging as _log

            _log.getLogger(__name__).warning("[digest] relation_feed section failed: %s", _rf_exc)

    # 5. 내 관심종목 변화 (watchlist 연동)
    lines.append("5️⃣ ⭐ 내 관심종목 변화")
    if watchlist_cfg is not None:
        try:
            from tele_quant.watchlist import is_watchlist_symbol

            wl_hits_pos: list[str] = []
            wl_hits_neg: list[str] = []
            wl_hits_watch: list[str] = []
            wl_hits_hot: list[str] = []
            wl_hits_weak: list[str] = []
            all_clusters = pack.positive_stock + pack.negative_stock + pack.macro

            for c in all_clusters:
                for ticker in c.tickers[:3]:
                    if is_watchlist_symbol(ticker, watchlist_cfg):
                        name = f"{ticker}"
                        headline = _one_sentence(c.headline, max_len=50)
                        entry = f"{name}: {headline}"
                        if c.polarity == "positive" and entry not in wl_hits_pos:
                            wl_hits_pos.append(entry)
                        elif c.polarity == "negative" and entry not in wl_hits_neg:
                            wl_hits_neg.append(entry)
                        elif entry not in wl_hits_watch:
                            wl_hits_watch.append(entry)

            # scenarios 기반 RSI 신호 (과열/약화)
            if scenarios:
                import contextlib

                _rsi_re = re.compile(r"RSI\w*:\s*([\d.]+)")

                for s in scenarios:
                    if not s.is_watchlist:
                        continue
                    name_sym = f"{s.name or s.symbol} / {s.symbol}"
                    # chart RSI (일봉)
                    chart_rsi: float | None = None
                    for line in s.chart_summary.splitlines():
                        m = _rsi_re.search(line)
                        if m:
                            with contextlib.suppress(ValueError):
                                chart_rsi = float(m.group(1))
                            break
                    # 4H RSI (intraday)
                    intraday_rsi: float | None = None
                    for line in s.intraday_4h_summary.splitlines():
                        m = _rsi_re.search(line)
                        if m:
                            with contextlib.suppress(ValueError):
                                intraday_rsi = float(m.group(1))
                            break

                    if chart_rsi is not None and chart_rsi >= 85:
                        entry = f"{name_sym} (RSI {chart_rsi:.0f} 과열)"
                        if entry not in wl_hits_hot:
                            wl_hits_hot.append(entry)
                    elif intraday_rsi is not None and intraday_rsi <= 30:
                        entry = f"{name_sym} (4H RSI {intraday_rsi:.0f} 약세)"
                        if entry not in wl_hits_weak:
                            wl_hits_weak.append(entry)
                    elif s.side == "LONG" and s.reasons_up:
                        entry = f"{name_sym}: 새로 강해짐 — {s.reasons_up[0][:50]}"
                        if entry not in wl_hits_pos:
                            wl_hits_pos.append(entry)

            if wl_hits_pos:
                for item in wl_hits_pos[:3]:
                    lines.append(f"  호재: {item}")
            if wl_hits_neg:
                for item in wl_hits_neg[:3]:
                    lines.append(f"  악재: {item}")
            if wl_hits_hot:
                for item in wl_hits_hot[:2]:
                    lines.append(f"  과열 주의: {item}")
            if wl_hits_weak:
                for item in wl_hits_weak[:2]:
                    lines.append(f"  약화/주의: {item}")
            if wl_hits_watch:
                for item in wl_hits_watch[:3]:
                    lines.append(f"  관망: {item}")
            if not any([wl_hits_pos, wl_hits_neg, wl_hits_hot, wl_hits_weak, wl_hits_watch]):
                lines.append("  - 이번 구간 관심종목 특이 언급 없음")
        except Exception:
            lines.append("  - 관심종목 정보 없음")
    else:
        lines.append("  - watchlist 비활성화")
    lines.append("")

    # 6. 새로 뜬 호재
    if pack.positive_stock:
        lines.append("6️⃣ 🆕 새로 뜬 호재")
        seen: set[str] = set()
        for c in pack.positive_stock[:5]:
            headline = _safe_headline(c)
            if headline in seen or not headline:
                continue
            seen.add(headline)
            ticker_part = f" ({', '.join(c.tickers[:2])})" if c.tickers else ""
            src_part = f" / 출처 {c.source_count}건" if c.source_count > 1 else ""
            lines.append(f"- {headline}{ticker_part}{src_part}")
        lines.append("")

    # 7. 새로 뜬 악재
    if pack.negative_stock:
        lines.append("7️⃣ 🆕 새로 뜬 악재")
        seen = set()
        for c in pack.negative_stock[:5]:
            headline = _safe_headline(c)
            if headline in seen or not headline:
                continue
            seen.add(headline)
            ticker_part = f" ({', '.join(c.tickers[:2])})" if c.tickers else ""
            src_part = f" / 출처 {c.source_count}건" if c.source_count > 1 else ""
            lines.append(f"- {headline}{ticker_part}{src_part}")
        lines.append("")

    # 8. 다음 72시간 체크포인트
    mac_text = " ".join(c.headline for c in pack.macro)
    checkpoints: list[str] = []
    for kw, label_kr in _CHECKPOINT_KW:
        if kw in mac_text:
            checkpoints.append(label_kr)

    lines.append("8️⃣ 📅 다음 72시간 체크포인트")
    if checkpoints:
        for cp in checkpoints[:6]:
            lines.append(f"- {cp}")
    else:
        lines.append("- 특별한 예정 이벤트 없음 (지속 모니터링)")
    lines.append("")

    lines += [
        "─" * 30,
        "공개 정보 기반 개인 리서치 보조용이며 투자 판단 책임은 사용자에게 있음.",
    ]
    return "\n".join(lines).strip()


# ---------------------------------------------------------------------------
# Polish guard: Ollama polish 전후 검증
# ---------------------------------------------------------------------------

_TICKER_RE = re.compile(r"\b([A-Z]{1,5}(?:\.[A-Z]{2})?|\d{6}\.[A-Z]{2})\b")
_FORBIDDEN_POLISH_RE = re.compile(
    r"무조건\s*매수|반드시\s*상승|확정\s*수익|Buy\s*Now|확실한\s*수익|100%\s*상승",
    re.IGNORECASE,
)


def _extract_tickers(text: str) -> frozenset[str]:
    """텍스트에서 티커 목록 추출."""
    return frozenset(_TICKER_RE.findall(text))


def _numbers_changed_too_much(original: str, polished: str, threshold: float = 0.25) -> bool:
    """숫자가 과도하게 바뀌었는지 확인."""
    import re as _re

    orig_nums = _re.findall(r"\d+(?:\.\d+)?", original)
    pol_nums = _re.findall(r"\d+(?:\.\d+)?", polished)
    if not orig_nums:
        return False
    changed = sum(1 for a, b in zip(orig_nums, pol_nums, strict=False) if a != b)
    return changed / len(orig_nums) > threshold


def apply_polish_guard(original: str, polished: str) -> str:
    """
    polish 후 검증:
    - 티커 목록이 달라지면 원본 사용
    - 금지 표현이 있으면 원본 사용
    - 숫자가 25% 이상 바뀌면 원본 사용
    """
    import logging

    log = logging.getLogger(__name__)

    orig_tickers = _extract_tickers(original)
    pol_tickers = _extract_tickers(polished)

    if orig_tickers and orig_tickers != pol_tickers:
        log.warning(
            "[polish-guard] tickers changed %s → %s, using original", orig_tickers, pol_tickers
        )
        return original

    if _FORBIDDEN_POLISH_RE.search(polished):
        log.warning("[polish-guard] forbidden expression in polish, using original")
        return original

    if _numbers_changed_too_much(original, polished):
        log.warning("[polish-guard] numbers changed too much, using original")
        return original

    return polished


def build_tech_scan_section(tech_scan_rows: list[dict], max_stocks: int = 6) -> str:
    """주목 종목 기술 스캔 섹션 생성.

    시나리오 임계점 미달이어도 호재·악재 언급 종목의 4H/3D RSI·OBV·BB를 표시.
    """
    if not tech_scan_rows:
        return ""

    # positive → negative 순, 각 최대 4/3개
    positives = [r for r in tech_scan_rows if r.get("sentiment") in ("positive",)][:4]
    negatives = [r for r in tech_scan_rows if r.get("sentiment") in ("negative",)][:3]
    rows = (positives + negatives)[:max_stocks]
    if not rows:
        rows = tech_scan_rows[:max_stocks]

    lines: list[str] = ["📈 주목 종목 기술 스캔"]
    lines.append("(직접증거 게이트 미달 — 관찰 참고용, 매수·매도 지시 아님)")

    for row in rows:
        sym: str = row.get("symbol", "")
        name: str = row.get("name", sym)
        sentiment: str = row.get("sentiment", "neutral")
        score: float = row.get("score", 0.0)
        catalysts: list[str] = row.get("catalysts", [])
        risks: list[str] = row.get("risks", [])
        snap = row.get("snap_4h")
        tech = row.get("technical")

        is_kr = sym.endswith(".KS") or sym.endswith(".KQ")
        _fmt_price = (lambda v: f"{v:,.0f}") if is_kr else (lambda v: f"{v:.2f}")

        sentiment_alpha: float | None = row.get("sentiment_alpha")
        direct_ev: int = row.get("direct_ev", 0)
        icon = "🟢" if sentiment == "positive" else ("🔴" if sentiment == "negative" else "⚪")
        alpha_s = f"  α{sentiment_alpha:.0f}" if sentiment_alpha is not None else ""  # noqa: RUF001
        ev_s = f"  직증{direct_ev}" if direct_ev > 0 else ""
        lines.append(f"\n{icon} {name} ({sym})  점수 {score:.0f}{alpha_s}{ev_s}")

        # 4H봉
        if snap is not None:
            rsi_s = f"RSI {snap.rsi14:.1f}" if snap.rsi14 is not None else "RSI N/A"
            obv_s = f"OBV {snap.obv_trend}" if snap.obv_trend else ""
            if snap.bb_upper is not None and snap.bb_middle is not None and snap.bb_lower is not None:
                bb_s = (
                    f"BB {snap.bb_position}"
                    f" (상{_fmt_price(snap.bb_upper)}/중{_fmt_price(snap.bb_middle)}/하{_fmt_price(snap.bb_lower)})"
                )
            else:
                bb_s = f"BB {snap.bb_position}" if snap.bb_position else ""
            parts_4h = [p for p in [rsi_s, bb_s, obv_s] if p]
            lines.append(f"   4H봉: {' / '.join(parts_4h)}")

            # 해석 힌트
            hint = ""
            if snap.rsi14 is not None:
                if snap.rsi14 >= 70 and snap.bb_position == "상단돌파":
                    hint = "단기 과열 — 눌림 대기"
                elif snap.rsi14 <= 35:
                    hint = "과매도 구간 — 반등 가능성 주시"
                elif snap.rsi14 >= 55 and snap.obv_trend == "상승" and snap.bb_position in ("중단~상단", "상단돌파"):
                    hint = "모멘텀 유효 — 중단 지지 확인"
                elif snap.obv_trend == "하락" and snap.bb_position in ("하단~중단", "하단이탈"):
                    hint = "수급 약화 + BB 하단권 — 관망"
            if hint:
                lines.append(f"   해석: {hint}")
        else:
            lines.append("   4H봉: 데이터 없음")

        # 3일봉
        if tech is not None and tech.close is not None:
            rsi3_s = f"RSI {tech.rsi14:.1f}" if tech.rsi14 is not None else "RSI N/A"
            obv3_s = f"OBV {tech.obv_trend}" if tech.obv_trend not in ("데이터 부족", "") else ""
            bb3_s = ""
            if tech.bb_upper is not None and tech.bb_middle is not None and tech.bb_lower is not None:
                bb3_s = (
                    f"BB {tech.bb_position}"
                    f" (상{_fmt_price(tech.bb_upper)}/중{_fmt_price(tech.bb_middle)}/하{_fmt_price(tech.bb_lower)})"
                )
            elif tech.bb_position not in ("데이터 부족", ""):
                bb3_s = f"BB {tech.bb_position}"
            close_s = f"종가 {_fmt_price(tech.close)}"
            parts_3d = [p for p in [close_s, rsi3_s, bb3_s, obv3_s] if p]
            lines.append(f"   3일봉: {' / '.join(parts_3d)}")

        # 촉매 / 리스크
        if catalysts:
            lines.append(f"   촉매: {catalysts[0][:80]}")
        if risks and sentiment == "negative":
            lines.append(f"   위험: {risks[0][:80]}")

    lines.append("")
    return "\n".join(lines)


def build_long_short_report(
    scenarios: list[TradeScenario],
    ranked_pack: RankedEvidencePack | None,
    market_verify_results: dict[str, Any],
    compact: bool = False,
    compact_max_longs: int = 5,
    compact_max_shorts: int = 2,
    compact_max_watch: int = 8,
    compact_max_reasons: int = 2,
) -> str:
    """Build deterministic long/short report. Delegates to format_analysis_report."""
    from tele_quant.analysis.report import format_analysis_report

    return format_analysis_report(
        scenarios,
        compact=compact,
        compact_max_longs=compact_max_longs,
        compact_max_shorts=compact_max_shorts,
        compact_max_watch=compact_max_watch,
        compact_max_reasons=compact_max_reasons,
    )
