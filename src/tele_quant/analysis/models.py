from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class StockCandidate:
    symbol: str
    name: str | None
    market: str  # KR, US, UNKNOWN
    mentions: int
    sentiment: str  # positive, negative, mixed, neutral
    catalysts: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    source_titles: list[str] = field(default_factory=list)
    # Count of contexts where symbol/name appears as subject (not as broker attribution)
    direct_evidence_count: int = 0
    # Multi-factor sentiment quality score (0-100)
    sentiment_alpha_score: float = 0.0


@dataclass
class TechnicalSnapshot:
    symbol: str
    close: float | None = None
    change_pct_1d: float | None = None
    change_pct_5d: float | None = None
    change_pct_20d: float | None = None
    sma20: float | None = None
    sma60: float | None = None
    sma120: float | None = None
    rsi14: float | None = None
    macd: float | None = None
    macd_signal: float | None = None
    atr14: float | None = None
    volume_ratio_20d: float | None = None
    support: float | None = None
    resistance: float | None = None
    trend_label: str = "데이터 부족"
    obv: float | None = None
    obv_trend: str = "데이터 부족"
    bb_upper: float | None = None
    bb_middle: float | None = None
    bb_lower: float | None = None
    bb_position: str = "데이터 부족"
    candle_label: str = "보통"


@dataclass
class FundamentalSnapshot:
    symbol: str
    market_cap: float | None = None
    trailing_pe: float | None = None
    forward_pe: float | None = None
    price_to_book: float | None = None
    roe: float | None = None
    debt_to_equity: float | None = None
    operating_margin: float | None = None
    revenue_growth: float | None = None
    dividend_yield: float | None = None
    valuation_label: str = "데이터 부족"


@dataclass
class ScoreCard:
    """5개 컴포넌트로 구성된 투자 점수카드."""

    evidence_score: float  # 0-30: 뉴스/리포트 증거 품질
    technical_score: float  # 0-30: 추세·MACD·OBV 방향성
    valuation_score: float  # 0-20: PER·PBR·ROE·마진
    macro_risk_score: float  # 0-10: 리스크 항목 + 매크로 영향
    timing_score: float  # 0-10: RSI 위치·볼린저·캔들·거래량 타이밍
    final_score: float  # 0-100: 최종 (캡 적용)
    grade: str  # 강한 관심·관심·관망·제외/주의
    sentiment_alpha_score: float = 0.0  # 0-100: 감성 알파 점수 (새 공식 사용 시)

    def display(self) -> str:
        """리포트용 한 줄 요약 문자열."""
        if self.sentiment_alpha_score > 0:
            return (
                f"감성α {self.sentiment_alpha_score:.0f} / "
                f"기술 {self.technical_score:.0f} / "
                f"가치 {self.valuation_score:.0f} / "
                f"리스크 {self.macro_risk_score:.0f} / "
                f"타이밍 {self.timing_score:.0f}"
            )
        return (
            f"증거 {self.evidence_score:.0f} / "
            f"기술 {self.technical_score:.0f} / "
            f"가치 {self.valuation_score:.0f} / "
            f"리스크 {self.macro_risk_score:.0f} / "
            f"타이밍 {self.timing_score:.0f}"
        )


@dataclass
class TradeScenario:
    symbol: str
    name: str | None
    direction: str  # bullish, bearish, neutral
    score: float
    grade: str
    entry_zone: str
    stop_loss: str
    take_profit: str
    invalidation: str
    reasons_up: list[str] = field(default_factory=list)
    reasons_down: list[str] = field(default_factory=list)
    technical_summary: str = ""
    fundamental_summary: str = ""
    chart_summary: str = ""
    risk_notes: list[str] = field(default_factory=list)
    side: str = "WATCH"  # LONG, SHORT, WATCH
    confidence: str = "medium"  # high, medium, low
    evidence_score: float = 0.0
    technical_score: float = 0.0
    valuation_score: float = 0.0
    macro_risk_score: float = 0.0
    timing_score: float = 0.0
    opportunity_score: float = 0.0
    risk_score_val: float = 0.0
    # Watchlist / provider fields
    is_watchlist: bool = False
    watchlist_group: str = ""
    verify_summary: str = ""
    beginner_hint: str = ""
    intraday_4h_summary: str = ""
    relation_feed_note: str = ""
    # 4H 기술지표 (intraday snapshot에서 추출, scenario_history 저장용)
    rsi_4h: float | None = None
    obv_4h: str = ""
    bollinger_4h: str = ""
    # 3D 기술지표 (daily technical snapshot에서 추출)
    rsi_3d: float | None = None
    obv_3d: str = ""
    bollinger_3d: str = ""
    # Evidence 품질
    direct_evidence_count: int = 0
    evidence_summary: str = ""
    signal_price_basis: str = "yfinance"
