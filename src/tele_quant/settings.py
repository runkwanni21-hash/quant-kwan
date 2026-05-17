from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _csv(value: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v).strip().lstrip("@") for v in value if str(v).strip()]
    return [part.strip().lstrip("@") for part in str(value).split(",") if part.strip()]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env.local",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    telegram_api_id: int | None = None
    telegram_api_hash: str | None = None
    telegram_phone: str | None = None
    telegram_session_path: Path = Path("./data/tele_quant.session")

    telegram_include_all_channels: bool = False
    telegram_source_chats: str = ""

    fetch_lookback_hours: float = 4.0
    digest_interval_hours: float = 4.0
    max_messages_per_chat: int = 80
    max_items_for_digest: int = 70
    min_text_chars: int = 20

    telegram_send_mode: str = Field(default="user", pattern="^(user|bot)$")
    telegram_target_chat: str = "me"
    telegram_bot_token: str | None = None
    telegram_bot_target_chat_id: str | None = None

    # 수신 봇 (inbound_bot.py) — 미설정 시 위 두 값으로 fallback
    telegram_inbound_bot_token: str | None = None   # 별도 봇 토큰 (없으면 telegram_bot_token 사용)
    telegram_inbound_allowed_ids: str = ""           # 허용 chat_id 콤마 구분, 비면 bot_target_chat_id 사용

    ollama_host: str = "http://127.0.0.1:11434"
    ollama_chat_model: str = "qwen3:8b"
    ollama_embed_model: str = "qwen3-embedding:0.6b"
    ollama_temperature: float = 0.15
    ollama_num_ctx: int = 32768
    ollama_timeout_seconds: float = 180.0

    fuzzy_dedupe_threshold: int = 92
    embedding_dedupe: bool = True
    embedding_dedupe_threshold: float = 0.88
    embedding_max_items: int = 220

    yfinance_enabled: bool = True
    yfinance_symbols: str = "^GSPC,^IXIC,^DJI,^VIX,^TNX,DX-Y.NYB,KRW=X,^KS11,^KQ11,005930.KS,000660.KS,NVDA,MSFT,AAPL,TSLA,BTC-USD"

    naver_reports_enabled: bool = True
    naver_report_categories: str = "company,industry,economy,market"
    naver_reports_per_category: int = 8
    naver_download_pdfs: bool = False
    naver_max_pdfs_per_run: int = 3
    naver_pdf_max_pages: int = 2
    naver_pdf_max_chars: int = 2500

    sqlite_path: Path = Path("./data/tele_quant.sqlite")

    ticker_aliases_path: str = "config/ticker_aliases.yml"

    # Analysis
    analysis_enabled: bool = True
    analysis_top_candidates: int = 24
    analysis_max_symbols: int = 40
    analysis_min_score_to_send: float = 55.0
    analysis_market_data_period: str = "6mo"
    analysis_market_data_interval: str = "1d"

    # Digest quality (map-reduce)
    digest_chunk_size: int = 25
    digest_chunk_summaries: bool = True
    digest_quality_mode: str = "deep"

    # Self-loop filter: 봇 결과물이 다시 수집되지 않도록
    telegram_exclude_chats: str = ""
    drop_self_generated_messages: bool = True
    self_generated_markers: str = (
        "Tele Quant,관심 진입,손절/무효화,목표/매도 관찰,"
        "공개 정보 기반 개인 리서치,매수/매도 추천이 아님"
    )

    # Source quality filter
    source_quality_enabled: bool = True
    source_quality_min_score: int = 2
    source_quality_strict_mode: bool = False

    # Evidence clusters (Ollama 입력 압축)
    evidence_max_clusters: int = 80
    evidence_max_macro_clusters: int = 25
    evidence_max_positive_stock_clusters: int = 35
    evidence_max_negative_stock_clusters: int = 35
    evidence_min_cluster_score: float = 2.0

    # Digest mode: fast / deep / no_llm
    digest_mode: str = "fast"
    ollama_final_timeout_seconds: float = 180.0
    ollama_polish_only: bool = True
    ollama_max_evidence_for_prompt: int = 28
    ollama_max_macro_evidence: int = 8
    ollama_max_positive_evidence: int = 10
    ollama_max_negative_evidence: int = 10
    deterministic_fallback: bool = True

    # Run safety limits
    run_max_seconds: float = 900.0
    ollama_call_max_seconds: float = 180.0
    analysis_max_tickers: int = 24
    yfinance_batch_size: int = 20

    # External env / provider
    external_env_path: str = "/mnt/c/Users/runkw/Downloads/.env.local"
    provider_verify_enabled: bool = True

    # Timezone for report focus labels
    timezone: str = "Asia/Seoul"

    # Local CSV data
    local_data_enabled: bool = True
    event_price_csv_path: str = "data/external/event_price_1000d.csv"
    correlation_csv_path: str = "data/external/stock_correlation_1000d.csv"
    correlation_expansion_enabled: bool = True
    correlation_min_value: float = 0.45
    correlation_max_peers_per_symbol: int = 5

    # Sector quota
    sector_quota_enabled: bool = True
    sector_quota_max_per_sector: int = 3
    sector_quota_overflow_count: int = 2

    # 4H intraday technical
    intraday_tech_enabled: bool = True
    intraday_interval: str = "60m"
    intraday_resample: str = "4h"
    intraday_period: str = "60d"

    # Report limits
    report_max_longs: int = 8
    report_max_shorts: int = 5
    report_max_watch: int = 12
    report_show_coverage: bool = True

    # Compact scenario mode (긴 진입/손절/목표 블록 제거, 최대 5L/2S/8W)
    report_compact_scenarios: bool = True
    report_compact_max_longs: int = 5
    report_compact_max_shorts: int = 2
    report_compact_max_watch: int = 8
    report_compact_max_reasons: int = 2
    report_reason_max_lines: int = 2
    report_hide_raw_links: bool = True
    report_hide_broker_headers: bool = True

    # 4H 브리핑 섹션별 최대 항목 수 (compact 모드)
    report_max_macro_items: int = 5
    report_max_tech_items: int = 5
    report_max_bio_items: int = 4
    report_max_policy_items: int = 4
    report_max_bullish_tickers: int = 6
    report_max_bearish_tickers: int = 4
    report_max_pair_watch: int = 4

    # Watchlist
    watchlist_enabled: bool = True
    watchlist_path: str = "config/watchlist.yml"

    # Weekly report
    # Smart reader (4H 전처리 패스) 타임아웃 / 샘플 크기
    ollama_narrative_timeout_seconds: float = 300.0
    ollama_stock_summary_timeout_seconds: float = 90.0
    narrative_max_items: int = 80

    weekly_enabled: bool = True
    weekly_lookback_days: int = 7
    weekly_mode: str = "deep_polish"
    weekly_ollama_timeout_seconds: float = 600.0
    weekly_max_reports: int = 60
    weekly_send_day: str = "SUN"
    weekly_send_hour: int = 23

    # Weekend macro-only mode (토~일 사이 종목분석 없이 매크로만 취합)
    weekend_macro_only: bool = True
    weekend_macro_only_start: str = "SAT 07:00"
    weekend_macro_only_end: str = "SUN 23:00"
    weekly_performance_review: bool = True

    # External indicators
    fear_greed_enabled: bool = True
    fear_greed_timeout_seconds: float = 10.0
    fred_enabled: bool = True
    fred_api_key: str = ""
    fred_series: str = "FEDFUNDS,DGS10,DGS2,UNRATE,T10YIE"
    fred_timeout_seconds: float = 12.0
    google_trends_enabled: bool = True
    google_trends_timeout_seconds: float = 25.0
    google_trends_max_keywords: int = 5

    # EIA 에너지 (미국 에너지부)
    eia_enabled: bool = True
    eia_api_key: str = ""
    eia_timeout_seconds: float = 10.0

    # ECB 유럽중앙은행
    ecb_enabled: bool = True
    ecb_timeout_seconds: float = 10.0

    # Frankfurter 실시간 환율
    frankfurter_enabled: bool = True
    frankfurter_timeout_seconds: float = 8.0

    # ECOS 한국은행 경제통계
    ecos_enabled: bool = True
    ecos_api_key: str = ""
    ecos_series: str = "722Y001,731Y003,901Y009"
    ecos_timeout_seconds: float = 12.0

    # KOSIS 통계청
    kosis_enabled: bool = False
    kosis_api_key: str = ""

    # RSS 뉴스 수집
    rss_enabled: bool = True
    google_news_rss_enabled: bool = True
    google_news_rss_max_per_symbol: int = 5
    google_news_rss_max_symbols: int = 4
    prnewswire_rss_enabled: bool = True
    globenewswire_rss_enabled: bool = True
    businesswire_rss_enabled: bool = True
    rss_max_items_per_source: int = 8
    rss_timeout_seconds: float = 10.0

    # SEC EDGAR 8-K 공시
    sec_enabled: bool = True
    sec_user_agent: str = "tele-quant/1.0 contact:tele-quant@example.com"
    sec_8k_lookback_days: int = 3
    sec_max_items_per_symbol: int = 2
    sec_timeout_seconds: float = 10.0
    sec_rate_limit_per_sec: int = 8

    # OpenDART (한국 공시)
    opendart_enabled: bool = True
    opendart_api_key: str = ""
    opendart_lookback_days: int = 3
    opendart_max_per_symbol: int = 3
    opendart_timeout_seconds: float = 10.0
    opendart_rate_limit_per_sec: int = 5

    # Finnhub (미국 주식 뉴스 + 경제 캘린더)
    finnhub_enabled: bool = True
    finnhub_api_key: str = ""
    finnhub_lookback_days: int = 2
    finnhub_max_per_symbol: int = 5
    finnhub_max_symbols: int = 8
    finnhub_timeout_seconds: float = 10.0
    finnhub_rate_limit_per_sec: int = 10

    # 경제 캘린더
    economic_calendar_lookahead_days: int = 14

    # Relation feed (yfinance 자체 계산 — 외부 피드 의존 없음)
    relation_feed_enabled: bool = True
    relation_feed_min_confidence: str = "medium"
    relation_feed_max_movers: int = 8
    relation_feed_max_targets_per_mover: int = 3

    # Relation fallback (stock feed에 leadlag 없을 때 자체 계산)
    relation_fallback_enabled: bool = True
    relation_fallback_when_empty: bool = True
    relation_fallback_max_sources: int = 8
    relation_fallback_peers_per_source: int = 20
    relation_fallback_lags: str = "1,2,3"
    relation_fallback_min_event_count: int = 5
    relation_fallback_min_probability: float = 0.50
    relation_fallback_min_lift: float = 1.05
    relation_fallback_max_results: int = 10
    relation_fallback_cache_enabled: bool = True
    relation_fallback_cache_ttl_hours: float = 24.0

    # GPTPRO research DB (lead-lag 통계 후보)
    research_db_enabled: bool = True
    research_db_path: str = "/home/kwanni/project/stock-relation-ai/GPTPRO"
    research_package_path: str = "data/research/GPTPRO"
    research_leadlag_enabled: bool = True
    research_top_pairs_limit: int = 200
    research_min_reliability: str = "promising_research_candidate"
    research_allow_caution: bool = False

    # Live Pair Watch (선행·후행 페어 관찰 엔진)
    live_pair_watch_enabled: bool = True
    live_pair_watch_interval: str = "1h"
    live_pair_watch_period: str = "60d"
    live_pair_watch_refresh_hours: float = 4.0
    live_pair_watch_max_sources: int = 30
    live_pair_watch_max_targets: int = 40
    live_pair_watch_min_source_move_pct: float = 2.5
    live_pair_watch_min_source_volume_ratio: float = 1.2
    live_pair_watch_target_lag_window_hours: str = "4,8,24"
    live_pair_watch_max_report_items: int = 10
    live_pair_watch_min_confidence: str = "medium"
    pair_watch_universe_path: str = "config/pair_watch_universe.yml"
    pair_watch_rules_path: str = "config/pair_watch_rules.yml"

    @field_validator(
        "telegram_api_id",
        "telegram_bot_token",
        "telegram_bot_target_chat_id",
        "telegram_api_hash",
        "telegram_phone",
        mode="before",
    )
    @classmethod
    def empty_str_to_none(cls, value: Any) -> Any:
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator("intraday_period", mode="before")
    @classmethod
    def ensure_intraday_period_min(cls, value: Any) -> Any:
        """INTRADAY_PERIOD가 30d 미만이면 자동으로 60d로 보정한다.

        5d나 7d는 4H 캔들 수가 부족해 RSI/볼린저를 계산할 수 없어 경고 없이 보정.
        """
        if isinstance(value, str):
            m = re.match(r"^(\d+)d$", value.strip(), re.IGNORECASE)
            if m and int(m.group(1)) < 30:
                return "60d"
        return value

    @property
    def source_chats(self) -> list[str]:
        return _csv(self.telegram_source_chats)

    @property
    def exclude_chats(self) -> list[str]:
        return _csv(self.telegram_exclude_chats)

    @property
    def self_markers(self) -> list[str]:
        return _csv(self.self_generated_markers)

    @property
    def symbols(self) -> list[str]:
        return _csv(self.yfinance_symbols)

    @property
    def naver_categories(self) -> list[str]:
        return _csv(self.naver_report_categories)

    def ensure_runtime_dirs(self) -> None:
        self.telegram_session_path.parent.mkdir(parents=True, exist_ok=True)
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    def validate_minimum(self) -> list[str]:
        issues: list[str] = []
        if self.telegram_api_id is None:
            issues.append("TELEGRAM_API_ID가 비어 있습니다.")
        if not self.telegram_api_hash:
            issues.append("TELEGRAM_API_HASH가 비어 있습니다.")
        if self.telegram_send_mode == "bot":
            if not self.telegram_bot_token:
                issues.append("bot 모드인데 TELEGRAM_BOT_TOKEN이 비어 있습니다.")
            if not self.telegram_bot_target_chat_id:
                issues.append("bot 모드인데 TELEGRAM_BOT_TARGET_CHAT_ID가 비어 있습니다.")
        if not self.telegram_include_all_channels and not self.source_chats:
            issues.append(
                "TELEGRAM_INCLUDE_ALL_CHANNELS=false인데 TELEGRAM_SOURCE_CHATS가 비어 있습니다."
            )
        return issues
