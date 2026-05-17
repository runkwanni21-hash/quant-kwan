from __future__ import annotations

import contextlib
import json
import sqlite3
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from tele_quant.models import RawItem, RunReport, parse_dt, utc_now
from tele_quant.textutil import content_hash

SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL,
    source_name TEXT NOT NULL,
    external_id TEXT NOT NULL,
    published_at TEXT NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    text TEXT NOT NULL,
    url TEXT,
    content_hash TEXT NOT NULL,
    meta_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    UNIQUE(source_type, external_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_items_published_at ON raw_items(published_at);
CREATE INDEX IF NOT EXISTS idx_raw_items_content_hash ON raw_items(content_hash);

CREATE TABLE IF NOT EXISTS digests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    period_hours REAL NOT NULL,
    digest_text TEXT NOT NULL,
    stats_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS run_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    period_hours REAL NOT NULL,
    mode TEXT NOT NULL DEFAULT 'unknown',
    digest_text TEXT NOT NULL,
    analysis_text TEXT,
    stats_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_run_reports_created_at ON run_reports(created_at);

CREATE TABLE IF NOT EXISTS scenario_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    report_id INTEGER,
    symbol TEXT NOT NULL,
    name TEXT,
    side TEXT NOT NULL,
    score REAL NOT NULL,
    confidence TEXT,
    entry_zone TEXT,
    stop_loss TEXT,
    target TEXT,
    close_price_at_report REAL,
    sector TEXT,
    source_mode TEXT,
    report_text_hash TEXT
);

CREATE INDEX IF NOT EXISTS idx_scenario_history_created_at ON scenario_history(created_at);
CREATE INDEX IF NOT EXISTS idx_scenario_history_symbol ON scenario_history(symbol);

CREATE TABLE IF NOT EXISTS mover_chain_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    asof_date TEXT NOT NULL,
    source_symbol TEXT NOT NULL,
    source_return_pct REAL,
    source_move_type TEXT,
    target_symbol TEXT NOT NULL,
    relation_type TEXT,
    lag_days INTEGER,
    conditional_prob REAL,
    lift REAL,
    confidence TEXT,
    target_price_at_signal REAL,
    target_price_at_review REAL,
    outcome_return_pct REAL,
    hit INTEGER,
    UNIQUE(asof_date, source_symbol, target_symbol, lag_days)
);

CREATE INDEX IF NOT EXISTS idx_mover_chain_created_at ON mover_chain_history(created_at);
CREATE INDEX IF NOT EXISTS idx_mover_chain_asof_date ON mover_chain_history(asof_date);

CREATE TABLE IF NOT EXISTS pair_watch_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    source_symbol TEXT NOT NULL,
    source_name TEXT,
    source_market TEXT,
    source_sector TEXT,
    source_return_4h REAL,
    source_return_1d REAL,
    source_volume_ratio REAL,
    target_symbol TEXT NOT NULL,
    target_name TEXT,
    target_market TEXT,
    target_sector TEXT,
    target_return_at_signal REAL,
    target_price_at_signal REAL,
    target_price_at_review REAL,
    expected_direction TEXT,
    pair_score REAL,
    confidence TEXT,
    gap_type TEXT,
    outcome_return_pct REAL,
    hit INTEGER,
    status TEXT DEFAULT 'pending',
    UNIQUE(created_at, source_symbol, target_symbol)
);

CREATE INDEX IF NOT EXISTS idx_pair_watch_created_at ON pair_watch_history(created_at);
CREATE INDEX IF NOT EXISTS idx_pair_watch_target ON pair_watch_history(target_symbol);

CREATE TABLE IF NOT EXISTS sentiment_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    report_id INTEGER,
    sector TEXT NOT NULL,
    sentiment_score REAL NOT NULL,
    bullish_count INTEGER NOT NULL DEFAULT 0,
    bearish_count INTEGER NOT NULL DEFAULT 0,
    novelty_count INTEGER NOT NULL DEFAULT 0,
    top_events_json TEXT NOT NULL DEFAULT '[]',
    source_count INTEGER NOT NULL DEFAULT 0,
    confidence TEXT NOT NULL DEFAULT 'medium'
);

CREATE INDEX IF NOT EXISTS idx_sentiment_history_created_at ON sentiment_history(created_at);
CREATE INDEX IF NOT EXISTS idx_sentiment_history_sector ON sentiment_history(sector);

CREATE TABLE IF NOT EXISTS narrative_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    report_id INTEGER,
    hours REAL NOT NULL DEFAULT 4.0,
    macro_summary TEXT NOT NULL DEFAULT '',
    key_events_json TEXT NOT NULL DEFAULT '[]',
    bullish_json TEXT NOT NULL DEFAULT '[]',
    bearish_json TEXT NOT NULL DEFAULT '[]',
    risks_json TEXT NOT NULL DEFAULT '[]',
    raw_item_count INTEGER NOT NULL DEFAULT 0,
    filtered_noise INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_narrative_history_created_at ON narrative_history(created_at);

CREATE TABLE IF NOT EXISTS fear_greed_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    report_id INTEGER,
    score REAL NOT NULL,
    rating TEXT NOT NULL DEFAULT '',
    rating_ko TEXT NOT NULL DEFAULT '',
    previous_close REAL,
    previous_1_week REAL,
    previous_1_month REAL
);
CREATE INDEX IF NOT EXISTS idx_fear_greed_history_created_at ON fear_greed_history(created_at);

CREATE TABLE IF NOT EXISTS daily_alpha_picks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    session TEXT NOT NULL,
    market TEXT NOT NULL,
    side TEXT NOT NULL,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    final_score REAL NOT NULL,
    sentiment_score REAL,
    value_score REAL,
    technical_4h_score REAL,
    technical_3d_score REAL,
    volume_score REAL,
    catalyst_score REAL,
    pair_watch_score REAL,
    risk_penalty REAL,
    style TEXT,
    valuation_reason TEXT,
    sentiment_reason TEXT,
    technical_reason TEXT,
    catalyst_reason TEXT,
    entry_zone TEXT,
    invalidation_level TEXT,
    target_zone TEXT,
    signal_price REAL,
    signal_price_source TEXT,
    evidence_count INTEGER DEFAULT 0,
    direct_evidence_count INTEGER DEFAULT 0,
    sector TEXT,
    rank INTEGER DEFAULT 0,
    sent INTEGER DEFAULT 0,
    price_at_review REAL,
    outcome_return_pct REAL,
    hit INTEGER,
    status TEXT DEFAULT 'pending',
    UNIQUE(session, market, side, symbol, created_at)
);

CREATE INDEX IF NOT EXISTS idx_daily_alpha_created_at ON daily_alpha_picks(created_at);
CREATE INDEX IF NOT EXISTS idx_daily_alpha_symbol ON daily_alpha_picks(symbol);

CREATE TABLE IF NOT EXISTS order_backlog_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    symbol TEXT NOT NULL,
    market TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT '',
    event_date TEXT NOT NULL,
    amount_ok_krw REAL,
    amount_usd_million REAL,
    client TEXT NOT NULL DEFAULT '',
    contract_type TEXT NOT NULL DEFAULT '',
    chain_tier INTEGER NOT NULL DEFAULT 1,
    raw_title TEXT NOT NULL DEFAULT '',
    raw_amount_text TEXT NOT NULL DEFAULT '',
    backlog_tier TEXT NOT NULL DEFAULT 'LOW',
    UNIQUE(symbol, event_date, raw_title)
);

CREATE INDEX IF NOT EXISTS idx_backlog_symbol ON order_backlog_events(symbol);
CREATE INDEX IF NOT EXISTS idx_backlog_created_at ON order_backlog_events(created_at);
"""

# Columns added after initial schema — applied via ALTER TABLE in _init
_COLUMN_MIGRATIONS: list[str] = [
    "ALTER TABLE mover_chain_history ADD COLUMN report_id INTEGER",
    "ALTER TABLE mover_chain_history ADD COLUMN source_name TEXT",
    "ALTER TABLE mover_chain_history ADD COLUMN target_name TEXT",
    "ALTER TABLE mover_chain_history ADD COLUMN target_market TEXT",
    "ALTER TABLE mover_chain_history ADD COLUMN direction TEXT",
    "ALTER TABLE mover_chain_history ADD COLUMN live_status TEXT",
    "ALTER TABLE mover_chain_history ADD COLUMN note TEXT",
    # scenario_history 확장 (4H/3D 기술지표 + sent 플래그)
    "ALTER TABLE scenario_history ADD COLUMN sent INTEGER DEFAULT 0",
    "ALTER TABLE scenario_history ADD COLUMN rsi_4h REAL",
    "ALTER TABLE scenario_history ADD COLUMN obv_4h TEXT",
    "ALTER TABLE scenario_history ADD COLUMN bollinger_4h TEXT",
    "ALTER TABLE scenario_history ADD COLUMN rsi_3d REAL",
    "ALTER TABLE scenario_history ADD COLUMN obv_3d TEXT",
    "ALTER TABLE scenario_history ADD COLUMN bollinger_3d TEXT",
    "ALTER TABLE scenario_history ADD COLUMN direct_evidence_count INTEGER",
    "ALTER TABLE scenario_history ADD COLUMN signal_price_basis TEXT",
    "ALTER TABLE scenario_history ADD COLUMN evidence_summary TEXT",
    # signal_price: close_price_at_report의 정식 alias — 성과 평가 가격 컬럼명 통일
    "ALTER TABLE scenario_history ADD COLUMN signal_price REAL",
    # daily_alpha_picks: spillover engine 컬럼
    "ALTER TABLE daily_alpha_picks ADD COLUMN source_symbol TEXT",
    "ALTER TABLE daily_alpha_picks ADD COLUMN source_name TEXT",
    "ALTER TABLE daily_alpha_picks ADD COLUMN source_return REAL",
    "ALTER TABLE daily_alpha_picks ADD COLUMN relation_type TEXT",
    "ALTER TABLE daily_alpha_picks ADD COLUMN rule_id TEXT",
    "ALTER TABLE daily_alpha_picks ADD COLUMN spillover_score REAL",
    # daily_alpha_picks: v2 품질 게이트 컬럼
    "ALTER TABLE daily_alpha_picks ADD COLUMN source_reason_type TEXT",
    "ALTER TABLE daily_alpha_picks ADD COLUMN style_detail TEXT",
    "ALTER TABLE daily_alpha_picks ADD COLUMN is_speculative INTEGER DEFAULT 0",
    # daily_alpha_picks: 목표가 알림 컬럼
    "ALTER TABLE daily_alpha_picks ADD COLUMN target_price REAL",
    "ALTER TABLE daily_alpha_picks ADD COLUMN invalidation_price REAL",
    "ALTER TABLE daily_alpha_picks ADD COLUMN alert_sent INTEGER DEFAULT 0",
    # daily_alpha_picks: 시나리오 알파 엔진 컬럼 (v3)
    "ALTER TABLE daily_alpha_picks ADD COLUMN scenario_type TEXT",
    "ALTER TABLE daily_alpha_picks ADD COLUMN scenario_score REAL",
    "ALTER TABLE daily_alpha_picks ADD COLUMN reason_quality REAL",
    "ALTER TABLE daily_alpha_picks ADD COLUMN source_reason TEXT",
    "ALTER TABLE daily_alpha_picks ADD COLUMN relation_path TEXT",
    "ALTER TABLE daily_alpha_picks ADD COLUMN data_quality TEXT",
    # pair_watch_history: dedupe + sent 플래그 + review 가격 메타 컬럼
    "ALTER TABLE pair_watch_history ADD COLUMN sent INTEGER DEFAULT 0",
    "ALTER TABLE pair_watch_history ADD COLUMN save_mode TEXT DEFAULT ''",
    "ALTER TABLE pair_watch_history ADD COLUMN dedupe_key TEXT",
    "ALTER TABLE pair_watch_history ADD COLUMN first_seen_at TEXT",
    "ALTER TABLE pair_watch_history ADD COLUMN last_seen_at TEXT",
    "ALTER TABLE pair_watch_history ADD COLUMN seen_count INTEGER DEFAULT 1",
    "ALTER TABLE pair_watch_history ADD COLUMN review_price_updated_at TEXT",
    "ALTER TABLE pair_watch_history ADD COLUMN archived INTEGER DEFAULT 0",
    "ALTER TABLE pair_watch_history ADD COLUMN legacy_missing_price INTEGER DEFAULT 0",
    "ALTER TABLE pair_watch_history ADD COLUMN relation_type TEXT",
    # backfill 출처 추적 컬럼
    "ALTER TABLE pair_watch_history ADD COLUMN backfill_source TEXT DEFAULT ''",
    "ALTER TABLE pair_watch_history ADD COLUMN backfill_status TEXT DEFAULT ''",
    # order_backlog_events v2: DART 원문 파싱 + SEC EDGAR 강화 컬럼
    "ALTER TABLE order_backlog_events ADD COLUMN rcept_no TEXT DEFAULT ''",
    "ALTER TABLE order_backlog_events ADD COLUMN filing_url TEXT DEFAULT ''",
    "ALTER TABLE order_backlog_events ADD COLUMN corp_name TEXT DEFAULT ''",
    "ALTER TABLE order_backlog_events ADD COLUMN amount_ratio_to_revenue REAL",
    "ALTER TABLE order_backlog_events ADD COLUMN contract_start TEXT DEFAULT ''",
    "ALTER TABLE order_backlog_events ADD COLUMN contract_end TEXT DEFAULT ''",
    "ALTER TABLE order_backlog_events ADD COLUMN parsed_confidence TEXT DEFAULT 'LOW'",
    "ALTER TABLE order_backlog_events ADD COLUMN is_amendment INTEGER DEFAULT 0",
    "ALTER TABLE order_backlog_events ADD COLUMN is_cancellation INTEGER DEFAULT 0",
    "ALTER TABLE order_backlog_events ADD COLUMN cik TEXT DEFAULT ''",
    "ALTER TABLE order_backlog_events ADD COLUMN accession_no TEXT DEFAULT ''",
    "ALTER TABLE order_backlog_events ADD COLUMN source_raw_hash TEXT DEFAULT ''",
]

# 기존 DB 백필: signal_price 컬럼 추가 후 close_price_at_report 값 복사
_BACKFILL_SQL = (
    "UPDATE scenario_history "
    "SET signal_price = close_price_at_report "
    "WHERE signal_price IS NULL AND close_price_at_report IS NOT NULL"
)


def _safe_float_db(v: object) -> float | None:
    try:
        return float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _is_refill_note_db(note: str) -> bool:
    n = note.lower()
    return "refill" in n or "deeper" in n or "empirical" in n or "lawbook" in n


def _fetch_signal_price_safe(symbol: str, market: str) -> float | None:
    if not symbol:
        return None
    try:
        import yfinance as yf

        yf_sym = f"{symbol}.KS" if (market or "").upper() == "KR" else symbol
        df = yf.Ticker(yf_sym).history(period="2d", interval="1d", auto_adjust=True)
        if df is None or df.empty:
            return None
        return float(df["Close"].iloc[-1])
    except Exception:
        return None


class Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            for _sql in _COLUMN_MIGRATIONS:
                with contextlib.suppress(sqlite3.OperationalError):
                    conn.execute(_sql)
            # Backfill signal_price from close_price_at_report for legacy rows
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(_BACKFILL_SQL)
            conn.commit()

    def insert_items(self, items: Iterable[RawItem]) -> list[RawItem]:
        inserted: list[RawItem] = []
        now = utc_now().isoformat()
        with self.connect() as conn:
            for item in items:
                try:
                    conn.execute(
                        """
                        INSERT INTO raw_items
                        (source_type, source_name, external_id, published_at, title, text, url, content_hash, meta_json, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            item.source_type,
                            item.source_name,
                            item.external_id,
                            item.published_at.isoformat(),
                            item.title,
                            item.text,
                            item.url,
                            content_hash(item.compact_text),
                            json.dumps(item.meta, ensure_ascii=False),
                            now,
                        ),
                    )
                    inserted.append(item)
                except sqlite3.IntegrityError:
                    continue
            conn.commit()
        return inserted

    def recent_items(self, since: datetime, limit: int = 2000) -> list[RawItem]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM raw_items
                WHERE published_at >= ?
                ORDER BY published_at DESC
                LIMIT ?
                """,
                (since.isoformat(), limit),
            ).fetchall()
        return [self._row_to_item(row) for row in rows]

    def recent_hashes(self, since: datetime) -> set[str]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT content_hash FROM raw_items WHERE published_at >= ?",
                (since.isoformat(),),
            ).fetchall()
        return {str(row["content_hash"]) for row in rows}

    def save_digest(self, digest_text: str, period_hours: float, stats: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO digests (created_at, period_hours, digest_text, stats_json)
                VALUES (?, ?, ?, ?)
                """,
                (
                    utc_now().isoformat(),
                    period_hours,
                    digest_text,
                    json.dumps(stats, ensure_ascii=False),
                ),
            )
            conn.commit()

    def save_run_report(
        self,
        digest: str,
        analysis: str | None,
        period_hours: float,
        mode: str,
        stats: dict[str, Any] | None,
    ) -> int:
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO run_reports (created_at, period_hours, mode, digest_text, analysis_text, stats_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    utc_now().isoformat(),
                    period_hours,
                    mode,
                    digest,
                    analysis,
                    json.dumps(stats or {}, ensure_ascii=False),
                ),
            )
            conn.commit()
            return cur.lastrowid or 0

    def recent_run_reports(
        self,
        since: datetime,
        until: datetime | None = None,
        limit: int = 100,
    ) -> list[RunReport]:
        with self.connect() as conn:
            if until is not None:
                rows = conn.execute(
                    """
                    SELECT * FROM run_reports
                    WHERE created_at >= ? AND created_at <= ?
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (since.isoformat(), until.isoformat(), limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM run_reports
                    WHERE created_at >= ?
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (since.isoformat(), limit),
                ).fetchall()
        return [self._row_to_run_report(row) for row in rows]

    def save_scenarios(
        self,
        report_id: int | None,
        scenarios: list[Any],
        mode: str = "",
        close_map: dict[str, float] | None = None,
        sector_map: dict[str, str] | None = None,
        sent: bool = False,
    ) -> None:
        """Save scenarios to scenario_history.

        Only stores records when sent=True (real sent reports) so that
        no-send preview runs don't pollute the weekly performance review.
        Pass sent=True only when the report was actually sent to Telegram.
        """
        if not scenarios:
            return
        # Gate: skip saving for preview/no-send runs — they don't count for performance
        if not sent:
            return
        now = utc_now().isoformat()
        close_map = close_map or {}
        sector_map = sector_map or {}

        # Prefetch prices for LONG/SHORT scenarios missing from close_map
        for s in scenarios:
            symbol = getattr(s, "symbol", "")
            side = getattr(s, "side", "WATCH")
            if side in ("LONG", "SHORT") and symbol and close_map.get(symbol) is None:
                market = getattr(s, "market", "") or (
                    "KR" if symbol.endswith((".KS", ".KQ")) else "US"
                )
                price = _fetch_signal_price_safe(symbol, market)
                if price is not None:
                    close_map[symbol] = price

        with self.connect() as conn:
            for s in scenarios:
                symbol = getattr(s, "symbol", "")
                price_val = close_map.get(symbol)
                conn.execute(
                    """
                    INSERT INTO scenario_history
                    (created_at, report_id, symbol, name, side, score, confidence,
                     entry_zone, stop_loss, target, close_price_at_report, sector, source_mode,
                     sent, rsi_4h, obv_4h, bollinger_4h, rsi_3d, obv_3d, bollinger_3d,
                     direct_evidence_count, signal_price_basis, evidence_summary, signal_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        now,
                        report_id,
                        symbol,
                        getattr(s, "name", None),
                        getattr(s, "side", "WATCH"),
                        float(getattr(s, "score", 0)),
                        getattr(s, "confidence", None),
                        getattr(s, "entry_zone", None),
                        getattr(s, "stop_loss", None),
                        getattr(s, "take_profit", None),
                        price_val,
                        sector_map.get(symbol),
                        mode or None,
                        1 if sent else 0,
                        getattr(s, "rsi_4h", None),
                        getattr(s, "obv_4h", None) or None,
                        getattr(s, "bollinger_4h", None) or None,
                        getattr(s, "rsi_3d", None),
                        getattr(s, "obv_3d", None) or None,
                        getattr(s, "bollinger_3d", None) or None,
                        getattr(s, "direct_evidence_count", None),
                        getattr(s, "signal_price_basis", None) or None,
                        getattr(s, "evidence_summary", None) or None,
                        price_val,  # signal_price = close_price_at_report alias
                    ),
                )
            conn.commit()

    def recent_scenarios(
        self,
        since: datetime,
        until: datetime | None = None,
        side: str | None = None,
        min_score: float = 0.0,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        clauses = ["created_at >= ?"]
        params: list[Any] = [since.isoformat()]
        if until is not None:
            clauses.append("created_at <= ?")
            params.append(until.isoformat())
        if side:
            clauses.append("side = ?")
            params.append(side)
        if min_score > 0:
            clauses.append("score >= ?")
            params.append(min_score)
        params.append(limit)
        sql = f"SELECT * FROM scenario_history WHERE {' AND '.join(clauses)} ORDER BY created_at DESC LIMIT ?"
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def load_signal_performance(
        self,
        since: datetime,
        until: datetime | None = None,
        side: str | None = None,
        min_score: float = 80.0,
        sent_only: bool = True,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """Load scenario_history rows for weekly LONG/SHORT performance review.

        Returns first crossing per (symbol, side) within the window — same symbol
        appearing multiple times counts only once (first 80+ crossing is the signal).
        """
        clauses = ["created_at >= ?", "score >= ?"]
        params: list[Any] = [since.isoformat(), min_score]
        if until is not None:
            clauses.append("created_at <= ?")
            params.append(until.isoformat())
        if side:
            clauses.append("side = ?")
            params.append(side)
        if sent_only:
            clauses.append("sent = 1")
        params.append(limit)
        sql = (
            f"SELECT * FROM scenario_history WHERE {' AND '.join(clauses)}"
            " ORDER BY created_at ASC LIMIT ?"
        )
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()

        # Deduplicate: keep first crossing per (symbol, side)
        seen: set[tuple[str, str]] = set()
        result: list[dict[str, Any]] = []
        for row in rows:
            key = (str(row["symbol"]), str(row["side"]))
            if key not in seen:
                seen.add(key)
                result.append(dict(row))
        return result

    def save_sentiment_history(
        self,
        report_id: int | None,
        sector: str,
        sentiment_score: float,
        bullish_count: int = 0,
        bearish_count: int = 0,
        novelty_count: int = 0,
        top_events_json: str = "[]",
        source_count: int = 0,
        confidence: str = "medium",
    ) -> None:
        now = utc_now().isoformat()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO sentiment_history
                (created_at, report_id, sector, sentiment_score, bullish_count, bearish_count,
                 novelty_count, top_events_json, source_count, confidence)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    report_id,
                    sector,
                    sentiment_score,
                    bullish_count,
                    bearish_count,
                    novelty_count,
                    top_events_json,
                    source_count,
                    confidence,
                ),
            )
            conn.commit()

    def recent_sentiment_history(
        self,
        since: datetime,
        sector: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        clauses = ["created_at >= ?"]
        params: list[Any] = [since.isoformat()]
        if sector:
            clauses.append("sector = ?")
            params.append(sector)
        params.append(limit)
        sql = (
            f"SELECT * FROM sentiment_history WHERE {' AND '.join(clauses)}"
            " ORDER BY created_at DESC LIMIT ?"
        )
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def save_narrative(
        self,
        result: Any,  # SmartReaderResult
        report_id: int | None = None,
        hours: float = 4.0,
    ) -> None:
        """Ollama smart_read 결과를 narrative_history에 저장."""
        import json as _json

        now = utc_now().isoformat()
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO narrative_history
                   (created_at, report_id, hours,
                    macro_summary, key_events_json, bullish_json, bearish_json,
                    risks_json, raw_item_count, filtered_noise)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    now,
                    report_id,
                    hours,
                    getattr(result, "macro_summary", "") or "",
                    _json.dumps(getattr(result, "key_events", []) or [], ensure_ascii=False),
                    _json.dumps(getattr(result, "bullish_items", []) or [], ensure_ascii=False),
                    _json.dumps(getattr(result, "bearish_items", []) or [], ensure_ascii=False),
                    _json.dumps(getattr(result, "risks", []) or [], ensure_ascii=False),
                    getattr(result, "raw_item_count", 0),
                    getattr(result, "filtered_noise", 0),
                ),
            )

    def recent_narratives(
        self,
        since: datetime,
        limit: int = 40,
    ) -> list[dict[str, Any]]:
        """최근 narrative_history 조회. 주간 리포트 등에서 사용."""
        import json as _json

        sql = (
            "SELECT * FROM narrative_history WHERE created_at >= ?"
            " ORDER BY created_at DESC LIMIT ?"
        )
        with self.connect() as conn:
            rows = conn.execute(sql, [since.isoformat(), limit]).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            for col in ("key_events_json", "bullish_json", "bearish_json", "risks_json"):
                try:
                    d[col] = _json.loads(d.get(col) or "[]")
                except Exception:
                    d[col] = []
            result.append(d)
        return result

    def save_fear_greed(
        self,
        data: dict[str, Any],
        report_id: int | None = None,
    ) -> None:
        """Fear & Greed 수치를 fear_greed_history에 저장."""
        sql = """
            INSERT INTO fear_greed_history
                (created_at, report_id, score, rating, rating_ko,
                 previous_close, previous_1_week, previous_1_month)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        with self.connect() as conn:
            conn.execute(
                sql,
                (
                    utc_now().isoformat(),
                    report_id,
                    float(data.get("score") or 0),
                    str(data.get("rating") or ""),
                    str(data.get("rating_ko") or ""),
                    _safe_float_db(data.get("previous_close")),
                    _safe_float_db(data.get("previous_1_week")),
                    _safe_float_db(data.get("previous_1_month")),
                ),
            )

    def recent_fear_greed(
        self,
        since: datetime,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """최근 fear_greed_history 조회 (DESC order)."""
        sql = (
            "SELECT * FROM fear_greed_history WHERE created_at >= ?"
            " ORDER BY created_at DESC LIMIT ?"
        )
        with self.connect() as conn:
            rows = conn.execute(sql, [since.isoformat(), limit]).fetchall()
        return [dict(row) for row in rows]

    def save_mover_chain(self, relation_feed: Any, report_id: int | None = None) -> int:
        """Save lead-lag rows from relation feed. Returns count of inserted rows.

        Filters: skips zero-return sources, refill/deeper notes, empty target symbols.
        Fetches target_price_at_signal via yfinance (batch-cached per symbol).
        """
        try:
            leadlag = getattr(relation_feed, "leadlag", [])
        except Exception:
            return 0
        if not leadlag:
            return 0

        # Batch-fetch target prices before opening DB connection
        price_cache: dict[str, float | None] = {}
        for row in leadlag:
            src_ret = getattr(row, "source_return_pct", None)
            if src_ret is None or src_ret == 0.0:
                continue
            note = getattr(row, "note", "") or ""
            if _is_refill_note_db(note):
                continue
            tgt_sym = getattr(row, "target_symbol", "") or ""
            if not tgt_sym:
                continue
            if tgt_sym not in price_cache:
                tgt_market = getattr(row, "target_market", "") or ""
                price_cache[tgt_sym] = _fetch_signal_price_safe(tgt_sym, tgt_market)

        now = utc_now().isoformat()
        inserted = 0
        with self.connect() as conn:
            for row in leadlag:
                src_ret = getattr(row, "source_return_pct", None)
                if src_ret is None or src_ret == 0.0:
                    continue
                note = getattr(row, "note", "") or ""
                if _is_refill_note_db(note):
                    continue
                tgt_sym = getattr(row, "target_symbol", "") or ""
                if not tgt_sym:
                    continue
                signal_price = price_cache.get(tgt_sym)
                try:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO mover_chain_history
                        (created_at, asof_date, report_id,
                         source_symbol, source_name, source_return_pct, source_move_type,
                         target_symbol, target_name, target_market,
                         relation_type, direction, lag_days,
                         conditional_prob, lift, confidence,
                         live_status, target_price_at_signal, note)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            now,
                            getattr(row, "asof_date", ""),
                            report_id,
                            getattr(row, "source_symbol", ""),
                            getattr(row, "source_name", None),
                            src_ret,
                            getattr(row, "source_move_type", None),
                            tgt_sym,
                            getattr(row, "target_name", None),
                            getattr(row, "target_market", None),
                            getattr(row, "relation_type", None),
                            getattr(row, "direction", None),
                            getattr(row, "lag_days", None),
                            getattr(row, "conditional_prob", None),
                            getattr(row, "lift", None),
                            getattr(row, "confidence", None),
                            "DATA_MISSING" if signal_price is None else "SIGNAL_SAVED",
                            signal_price,
                            note or None,
                        ),
                    )
                    inserted += conn.execute("SELECT changes()").fetchone()[0]
                except sqlite3.IntegrityError:
                    continue
            conn.commit()
        return inserted

    def recent_mover_chain_signals(
        self,
        since: datetime,
        until: datetime | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Query mover_chain_history rows for weekly review.

        Filters out zero-return sources and refill/empirical notes.
        """
        clauses = [
            "created_at >= ?",
            "source_return_pct IS NOT NULL",
            "source_return_pct != 0.0",
            "target_symbol != ''",
            "(note IS NULL OR ("
            "note NOT LIKE '%refill%' AND note NOT LIKE '%deeper%' AND "
            "note NOT LIKE '%empirical%' AND note NOT LIKE '%lawbook%'))",
        ]
        params: list[Any] = [since.isoformat()]
        if until is not None:
            clauses.append("created_at <= ?")
            params.append(until.isoformat())
        params.append(limit)
        sql = (
            f"SELECT * FROM mover_chain_history WHERE {' AND '.join(clauses)}"
            " ORDER BY created_at DESC LIMIT ?"
        )
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def update_mover_chain_review(
        self,
        row_id: int,
        target_price_at_review: float,
        outcome_return_pct: float,
        hit: int,
    ) -> None:
        """Update review results for a mover_chain_history row."""
        with self.connect() as conn:
            conn.execute(
                """UPDATE mover_chain_history
                   SET target_price_at_review = ?, outcome_return_pct = ?, hit = ?
                   WHERE id = ?""",
                (target_price_at_review, outcome_return_pct, hit, row_id),
            )
            conn.commit()

    def save_pair_watch_signals(
        self,
        signals: list[Any],
        sent: bool = False,
        save_mode: str = "",
    ) -> int:
        """Persist LivePairSignal list to pair_watch_history using dedupe_key upsert.

        Same source-target-direction-relation per KST date → single representative row.
        Returns the number of rows inserted (new) or upserted (seen_count incremented).
        """
        if not signals:
            return 0
        now = utc_now()
        now_iso = now.isoformat()
        sent_int = 1 if sent else 0
        affected = 0
        with self.connect() as conn:
            for sig in signals:
                src_sym = getattr(sig, "source_symbol", "")
                tgt_sym = getattr(sig, "target_symbol", "")
                exp_dir = getattr(sig, "expected_direction", "UP")
                rel_type = getattr(sig, "relation_type", "") or ""
                # KST date for dedupe key
                _kst = timezone(timedelta(hours=9))
                signal_date = now.astimezone(_kst).strftime("%Y-%m-%d")
                dedupe_key = f"{src_sym}|{tgt_sym}|{exp_dir}|{rel_type}|{signal_date}"

                # Check if row with this dedupe_key already exists
                existing = conn.execute(
                    "SELECT id, seen_count, target_price_at_signal, first_seen_at FROM pair_watch_history WHERE dedupe_key = ? AND archived = 0 LIMIT 1",
                    (dedupe_key,),
                ).fetchone()

                tgt_price = getattr(sig, "target_price_at_signal", None)
                pair_score = getattr(sig, "pair_score", None)

                if existing:
                    new_count = (existing["seen_count"] or 1) + 1
                    # Fill missing target_price_at_signal from first valid value
                    fill_price = existing["target_price_at_signal"]
                    if fill_price is None and tgt_price is not None:
                        fill_price = tgt_price
                    conn.execute(
                        """UPDATE pair_watch_history
                           SET last_seen_at = ?, seen_count = ?,
                               target_price_at_signal = COALESCE(target_price_at_signal, ?),
                               pair_score = CASE WHEN ? > COALESCE(pair_score, 0) THEN ? ELSE pair_score END,
                               sent = MAX(sent, ?)
                           WHERE id = ?""",
                        (
                            now_iso,
                            new_count,
                            tgt_price,
                            pair_score,
                            pair_score,
                            sent_int,
                            existing["id"],
                        ),
                    )
                    affected += 1
                else:
                    try:
                        conn.execute(
                            """
                            INSERT INTO pair_watch_history
                            (created_at, source_symbol, source_name, source_market, source_sector,
                             source_return_4h, source_return_1d, source_volume_ratio,
                             target_symbol, target_name, target_market, target_sector,
                             target_return_at_signal, target_price_at_signal,
                             expected_direction, relation_type, pair_score, confidence,
                             gap_type, status, sent, save_mode,
                             dedupe_key, first_seen_at, last_seen_at, seen_count,
                             archived, legacy_missing_price)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                now_iso,
                                src_sym,
                                getattr(sig, "source_name", None),
                                getattr(sig, "source_market", None),
                                getattr(sig, "source_sector", None),
                                getattr(sig, "source_return_4h", None),
                                getattr(sig, "source_return_1d", None),
                                getattr(sig, "source_volume_ratio", None),
                                tgt_sym,
                                getattr(sig, "target_name", None),
                                getattr(sig, "target_market", None),
                                getattr(sig, "target_sector", None),
                                getattr(sig, "target_return_4h", None)
                                or getattr(sig, "target_return_1d", None),
                                tgt_price,
                                exp_dir,
                                rel_type,
                                pair_score,
                                getattr(sig, "confidence", None),
                                getattr(sig, "gap_type", None),
                                "pending",
                                sent_int,
                                save_mode,
                                dedupe_key,
                                now_iso,
                                now_iso,
                                1,
                                0,
                                1 if tgt_price is None else 0,
                            ),
                        )
                        affected += conn.execute("SELECT changes()").fetchone()[0]
                    except sqlite3.IntegrityError:
                        pass
            conn.commit()
        return affected

    def recent_pair_watch_signals(
        self,
        since: datetime,
        until: datetime | None = None,
        limit: int = 500,
        sent_only: bool = False,
        exclude_archived: bool = True,
    ) -> list[dict[str, Any]]:
        """Query pair_watch_history.

        sent_only=True: only rows with sent=1 (post-fix scheduled sends).
        exclude_archived=True: skip rows marked archived=1 by cleanup.
        """
        clauses = ["created_at >= ?"]
        params: list[Any] = [since.isoformat()]
        if until is not None:
            clauses.append("created_at <= ?")
            params.append(until.isoformat())
        if sent_only:
            clauses.append("sent = 1")
        if exclude_archived:
            clauses.append("(archived IS NULL OR archived = 0)")
        params.append(limit)
        sql = (
            f"SELECT * FROM pair_watch_history WHERE {' AND '.join(clauses)}"
            " ORDER BY created_at DESC LIMIT ?"
        )
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def pair_watch_cleanup_stats(self) -> dict[str, Any]:
        """Return cleanup statistics without modifying DB."""
        with self.connect() as conn:
            total = conn.execute(
                "SELECT COUNT(*) FROM pair_watch_history WHERE (archived IS NULL OR archived = 0)"
            ).fetchone()[0]
            price_missing = conn.execute(
                "SELECT COUNT(*) FROM pair_watch_history WHERE target_price_at_signal IS NULL AND (archived IS NULL OR archived = 0)"
            ).fetchone()[0]
            unverified = conn.execute(
                "SELECT COUNT(*) FROM pair_watch_history WHERE backfill_status='unverified_legacy_backfill' AND (archived IS NULL OR archived = 0)"
            ).fetchone()[0]
            dup_groups = conn.execute(
                """SELECT COUNT(*) FROM (
                    SELECT dedupe_key FROM pair_watch_history
                    WHERE dedupe_key IS NOT NULL AND (archived IS NULL OR archived = 0)
                    GROUP BY dedupe_key HAVING COUNT(*) > 1
                )"""
            ).fetchone()[0]
            dup_rows = conn.execute(
                """SELECT COUNT(*) FROM (
                    SELECT id FROM pair_watch_history
                    WHERE dedupe_key IS NOT NULL AND (archived IS NULL OR archived = 0)
                    AND id NOT IN (
                        SELECT MIN(id) FROM pair_watch_history
                        WHERE dedupe_key IS NOT NULL AND (archived IS NULL OR archived = 0)
                        GROUP BY dedupe_key
                    )
                )"""
            ).fetchone()[0]
        return {
            "total_active": total,
            "duplicate_groups": dup_groups,
            "duplicate_rows_to_archive": dup_rows,
            "price_missing": price_missing,
            "unverified_legacy": unverified,
        }

    def pair_watch_cleanup_apply(self) -> dict[str, Any]:
        """Archive duplicates, mark legacy rows, backfill with historical prices."""
        archived = 0
        legacy_marked = 0

        with self.connect() as conn:
            # Step 0: mark ALL existing rows without backfill_source as unverified_legacy_backfill.
            # New rows (saved with sent=True going forward) will have backfill_source=''.
            # Legacy rows cannot be trusted — their price source is unknown.
            conn.execute(
                """UPDATE pair_watch_history
                   SET backfill_status = 'unverified_legacy_backfill'
                   WHERE (backfill_source IS NULL OR backfill_source = '')
                     AND backfill_status != 'unverified_legacy_backfill'
                     AND (archived IS NULL OR archived = 0)"""
            )

            # Step 1: archive duplicate dedupe_key groups — keep MIN(id) as representative
            dup_ids = conn.execute(
                """SELECT id FROM pair_watch_history
                   WHERE dedupe_key IS NOT NULL AND (archived IS NULL OR archived = 0)
                   AND id NOT IN (
                       SELECT MIN(id) FROM pair_watch_history
                       WHERE dedupe_key IS NOT NULL AND (archived IS NULL OR archived = 0)
                       GROUP BY dedupe_key
                   )"""
            ).fetchall()
            if dup_ids:
                placeholders = ",".join("?" * len(dup_ids))
                ids = [r[0] for r in dup_ids]
                conn.execute(
                    f"UPDATE pair_watch_history SET archived = 1 WHERE id IN ({placeholders})",
                    ids,
                )
                archived = len(ids)

            # Step 2: update representative row seen_count/first_seen/last_seen
            rep_rows = conn.execute(
                """SELECT MIN(id) as rep_id, dedupe_key,
                          MIN(first_seen_at) as fst, MAX(last_seen_at) as lst, COUNT(*) as cnt
                   FROM pair_watch_history
                   WHERE dedupe_key IS NOT NULL
                   GROUP BY dedupe_key HAVING COUNT(*) > 1"""
            ).fetchall()
            for r in rep_rows:
                conn.execute(
                    """UPDATE pair_watch_history
                       SET first_seen_at = ?, last_seen_at = ?, seen_count = ?
                       WHERE id = ?""",
                    (r["fst"], r["lst"], r["cnt"], r["rep_id"]),
                )

            # Step 3: mark rows still missing price
            conn.execute(
                """UPDATE pair_watch_history SET legacy_missing_price = 1
                   WHERE target_price_at_signal IS NULL AND (archived IS NULL OR archived = 0)"""
            )
            legacy_marked = conn.execute("SELECT changes()").fetchone()[0]
            conn.commit()

        # Step 4: historical backfill — replace unverified prices with date-accurate prices
        exact_cnt, nearest_cnt, failed_cnt = self._backfill_pair_watch_prices_historical()

        with self.connect() as conn:
            unverified_remaining = conn.execute(
                "SELECT COUNT(*) FROM pair_watch_history WHERE backfill_status='unverified_legacy_backfill' AND (archived IS NULL OR archived = 0)"
            ).fetchone()[0]

        return {
            "archived": archived,
            "legacy_marked": legacy_marked,
            "exact_backfilled": exact_cnt,
            "nearest_backfilled": nearest_cnt,
            "failed_backfill": failed_cnt,
            "unverified_remaining": unverified_remaining,
        }

    @staticmethod
    def _fetch_historical_close(
        yf_sym: str, signal_date_str: str
    ) -> tuple[float | None, str]:
        """Fetch historical close at or near signal_date_str ('YYYY-MM-DD').

        Returns (price, source) where source is:
          'exact_date_close' | 'nearest_trading_day_close' | 'failed_no_price'
        """
        try:
            from datetime import date as _date

            import pandas as pd
            import yfinance as yf

            signal_date = _date.fromisoformat(signal_date_str)
            end_date = signal_date + timedelta(days=8)

            df = yf.Ticker(yf_sym).history(
                start=signal_date_str,
                end=end_date.isoformat(),
                interval="1d",
                auto_adjust=True,
            )
            if df is None or df.empty:
                return None, "failed_no_price"

            # Normalize index to plain date objects
            idx = df.index
            if hasattr(idx, "tz") and idx.tz is not None:
                idx = idx.tz_localize(None)
            df.index = pd.to_datetime(idx).date

            # Try exact date
            if signal_date in df.index:
                price = float(df.loc[signal_date, "Close"])
                return price, "exact_date_close"

            # Nearest date on or after signal date
            after = sorted(d for d in df.index if d >= signal_date)
            if after:
                price = float(df.loc[after[0], "Close"])
                return price, "nearest_trading_day_close"

            return None, "failed_no_price"
        except Exception:
            return None, "failed_no_price"

    def _backfill_pair_watch_prices_historical(self) -> tuple[int, int, int]:
        """Backfill target_price_at_signal using the signal date's historical close.

        Targets: rows with target_price_at_signal IS NULL OR backfill_status='unverified_legacy_backfill'
        Returns (exact_count, nearest_count, failed_count).
        """
        with self.connect() as conn:
            rows = conn.execute(
                """SELECT id, target_symbol, target_market, created_at
                   FROM pair_watch_history
                   WHERE (target_price_at_signal IS NULL
                          OR backfill_status = 'unverified_legacy_backfill')
                     AND (archived IS NULL OR archived = 0)
                   ORDER BY created_at DESC LIMIT 600"""
            ).fetchall()

        if not rows:
            return 0, 0, 0

        # Build deduplicated fetch plan: (yf_sym, signal_date_str) → (price, source)
        _kst = timezone(timedelta(hours=9))
        plan: dict[tuple, tuple[float | None, str]] = {}

        sym_date_to_ids: dict[tuple, list[int]] = {}
        for row in rows:
            sym = row["target_symbol"] or ""
            mkt = row["target_market"] or "US"
            if not sym:
                continue
            yf_sym = (
                f"{sym}.KS"
                if mkt.upper() == "KR" and not sym.endswith((".KS", ".KQ"))
                else sym
            )
            try:
                dt = datetime.fromisoformat(row["created_at"])
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.UTC)
                signal_date_str = dt.astimezone(_kst).strftime("%Y-%m-%d")
            except Exception:
                signal_date_str = (row["created_at"] or "")[:10]

            key = (yf_sym, signal_date_str)
            sym_date_to_ids.setdefault(key, []).append(row["id"])

        # Fetch once per unique (symbol, date)
        for key in sym_date_to_ids:
            yf_sym, signal_date_str = key
            if key not in plan:
                plan[key] = self._fetch_historical_close(yf_sym, signal_date_str)

        # Apply results
        exact_cnt = nearest_cnt = failed_cnt = 0
        for (yf_sym, signal_date_str), (price, source) in plan.items():
            ids = sym_date_to_ids[(yf_sym, signal_date_str)]
            with self.connect() as conn:
                for row_id in ids:
                    if price is not None and source in ("exact_date_close", "nearest_trading_day_close"):
                        conn.execute(
                            """UPDATE pair_watch_history
                               SET target_price_at_signal = ?,
                                   backfill_source = ?,
                                   backfill_status = '',
                                   legacy_missing_price = 0
                               WHERE id = ?""",
                            (price, source, row_id),
                        )
                        if source == "exact_date_close":
                            exact_cnt += 1
                        else:
                            nearest_cnt += 1
                    else:
                        conn.execute(
                            """UPDATE pair_watch_history
                               SET backfill_source = 'failed_no_price',
                                   legacy_missing_price = 1
                               WHERE id = ?""",
                            (row_id,),
                        )
                        failed_cnt += 1
                conn.commit()

        return exact_cnt, nearest_cnt, failed_cnt

    def update_pair_watch_review(
        self,
        row_id: int,
        target_price_at_review: float,
        outcome_return_pct: float,
        hit: int,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """UPDATE pair_watch_history
                   SET target_price_at_review = ?, outcome_return_pct = ?, hit = ?, status = 'reviewed'
                   WHERE id = ?""",
                (target_price_at_review, outcome_return_pct, hit, row_id),
            )
            conn.commit()

    def _row_to_run_report(self, row: sqlite3.Row) -> RunReport:
        created_at = parse_dt(row["created_at"]) or utc_now()
        try:
            stats = json.loads(row["stats_json"] or "{}")
        except json.JSONDecodeError:
            stats = {}
        return RunReport(
            id=row["id"],
            created_at=created_at,
            digest=row["digest_text"] or "",
            analysis=row["analysis_text"],
            period_hours=row["period_hours"],
            mode=row["mode"] or "unknown",
            stats=stats,
        )

    def save_daily_alpha_picks(
        self,
        picks: list[Any],
        session: str,
        market: str,
    ) -> int:
        """Insert daily alpha picks. Skips duplicates (same session/market/side/symbol/date).
        Only call when send=True. Returns number of new rows inserted."""
        from tele_quant.daily_alpha import DailyAlphaPick

        now = utc_now().isoformat()
        today_prefix = now[:10]  # YYYY-MM-DD
        inserted = 0
        with self.connect() as conn:
            for pick in picks:
                if not isinstance(pick, DailyAlphaPick):
                    continue
                try:
                    conn.execute(
                        """
                        INSERT INTO daily_alpha_picks
                        (created_at, session, market, side, symbol, name, final_score,
                         sentiment_score, value_score, technical_4h_score, technical_3d_score,
                         volume_score, catalyst_score, pair_watch_score, risk_penalty,
                         style, valuation_reason, sentiment_reason, technical_reason,
                         catalyst_reason, entry_zone, invalidation_level, target_zone,
                         signal_price, signal_price_source, evidence_count,
                         direct_evidence_count, sector, rank, sent, status,
                         source_symbol, source_name, source_return,
                         relation_type, rule_id, spillover_score,
                         source_reason_type, style_detail, is_speculative,
                         target_price, invalidation_price, alert_sent,
                         scenario_type, scenario_score, reason_quality,
                         source_reason, relation_path, data_quality)
                        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                               ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'pending', ?, ?, ?, ?, ?, ?,
                               ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?
                        WHERE NOT EXISTS (
                            SELECT 1 FROM daily_alpha_picks
                            WHERE session=? AND market=? AND side=? AND symbol=?
                              AND created_at LIKE ?
                        )
                        """,
                        (
                            now, session, market, pick.side, pick.symbol,
                            pick.name, pick.final_score,
                            pick.sentiment_score, pick.value_score,
                            pick.technical_4h_score, pick.technical_3d_score,
                            pick.volume_score, pick.catalyst_score,
                            pick.pair_watch_score, pick.risk_penalty,
                            pick.style, pick.valuation_reason, pick.sentiment_reason,
                            pick.technical_reason, pick.catalyst_reason,
                            pick.entry_zone, pick.invalidation_level, pick.target_zone,
                            pick.signal_price, pick.signal_price_source,
                            pick.evidence_count, pick.direct_evidence_count,
                            pick.sector, pick.rank,
                            # Spillover fields
                            getattr(pick, "source_symbol", "") or "",
                            getattr(pick, "source_name", "") or "",
                            getattr(pick, "source_return", 0.0) or 0.0,
                            getattr(pick, "relation_type", "") or "",
                            getattr(pick, "rule_id", "") or "",
                            getattr(pick, "spillover_score", 0.0) or 0.0,
                            # v2 quality fields
                            getattr(pick, "source_reason_type", "") or "",
                            getattr(pick, "style_detail", "") or "",
                            1 if getattr(pick, "is_speculative", False) else 0,
                            # price alert fields
                            getattr(pick, "target_price", None),
                            getattr(pick, "invalidation_price", None),
                            # scenario alpha v3 fields
                            getattr(pick, "scenario_type", "") or "",
                            getattr(pick, "scenario_score", 0.0) or 0.0,
                            getattr(pick, "reason_quality", 50.0) or 50.0,
                            getattr(pick, "source_reason", "") or "",
                            getattr(pick, "relation_path", "") or "",
                            getattr(pick, "data_quality", "medium") or "medium",
                            # WHERE NOT EXISTS params
                            session, market, pick.side, pick.symbol,
                            f"{today_prefix}%",
                        ),
                    )
                    inserted += conn.execute("SELECT changes()").fetchone()[0]
                except sqlite3.IntegrityError:
                    pass
            conn.commit()
        return inserted

    def recent_daily_alpha_picks(
        self,
        since: datetime,
        market: str | None = None,
        side: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        clauses = ["created_at >= ?"]
        params: list[Any] = [since.isoformat()]
        if market:
            clauses.append("market = ?")
            params.append(market)
        if side:
            clauses.append("side = ?")
            params.append(side)
        params.append(limit)
        sql = (
            f"SELECT * FROM daily_alpha_picks WHERE {' AND '.join(clauses)}"
            " ORDER BY created_at DESC LIMIT ?"
        )
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def mark_alert_sent(self, row_id: int, alert_type: int) -> None:
        """alert_sent 컬럼 업데이트. alert_type: 1=목표가도달 2=무효화이탈."""
        with self.connect() as conn:
            conn.execute(
                "UPDATE daily_alpha_picks SET alert_sent=? WHERE id=?",
                (alert_type, row_id),
            )
            conn.commit()

    def get_active_picks_for_alert(self, since: datetime, market: str | None = None) -> list[dict]:
        """alert_sent=0 이고 target_price/invalidation_price가 있는 활성 picks 반환."""
        clauses = ["created_at >= ?", "alert_sent = 0",
                   "target_price IS NOT NULL", "invalidation_price IS NOT NULL"]
        params: list[Any] = [since.isoformat()]
        if market:
            clauses.append("market = ?")
            params.append(market)
        sql = (
            f"SELECT * FROM daily_alpha_picks WHERE {' AND '.join(clauses)}"
            " ORDER BY created_at DESC"
        )
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def update_daily_alpha_review(
        self,
        row_id: int,
        price_at_review: float,
        outcome_return_pct: float,
        hit: int,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """UPDATE daily_alpha_picks
                   SET price_at_review=?, outcome_return_pct=?, hit=?, status='reviewed'
                   WHERE id=?""",
                (price_at_review, outcome_return_pct, hit, row_id),
            )
            conn.commit()

    # ── Order Backlog ─────────────────────────────────────────────────────────

    def insert_backlog_events(self, events: list[Any]) -> int:
        """BacklogEvent 리스트를 order_backlog_events에 upsert. 저장 건수 반환."""
        from tele_quant.models import utc_now as _utcnow
        now = _utcnow().isoformat()
        count = 0
        with self.connect() as conn:
            for ev in events:
                try:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO order_backlog_events
                        (created_at, symbol, market, source, event_date,
                         amount_ok_krw, amount_usd_million, client, contract_type,
                         chain_tier, raw_title, raw_amount_text, backlog_tier,
                         rcept_no, filing_url, corp_name, amount_ratio_to_revenue,
                         contract_start, contract_end, parsed_confidence,
                         is_amendment, is_cancellation, cik, accession_no, source_raw_hash)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            now,
                            ev.symbol,
                            ev.market,
                            ev.source,
                            ev.event_date.isoformat(),
                            ev.amount_ok_krw,
                            ev.amount_usd_million,
                            ev.client,
                            ev.contract_type,
                            ev.chain_tier,
                            ev.raw_title,
                            ev.raw_amount_text,
                            ev.backlog_tier,
                            getattr(ev, "rcept_no", ""),
                            getattr(ev, "filing_url", ""),
                            getattr(ev, "corp_name", ""),
                            getattr(ev, "amount_ratio_to_revenue", None),
                            getattr(ev, "contract_start", ""),
                            getattr(ev, "contract_end", ""),
                            getattr(ev, "parsed_confidence", "LOW"),
                            int(getattr(ev, "is_amendment", False)),
                            int(getattr(ev, "is_cancellation", False)),
                            getattr(ev, "cik", ""),
                            getattr(ev, "accession_no", ""),
                            getattr(ev, "source_raw_hash", ""),
                        ),
                    )
                    count += conn.execute("SELECT changes()").fetchone()[0]
                except sqlite3.OperationalError:
                    pass
            conn.commit()
        return count

    def recent_backlog_events(self, symbol: str, days: int = 60) -> list[dict]:
        """최근 N일 이내 특정 심볼의 수주잔고 이벤트 반환."""
        since = (datetime.now(UTC) - timedelta(days=days)).isoformat()
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM order_backlog_events WHERE symbol=? AND created_at>=?"
                " ORDER BY created_at DESC",
                (symbol, since),
            ).fetchall()
        return [dict(r) for r in rows]

    def recent_all_backlog_events(self, days: int = 7) -> list[dict]:
        """최근 N일 전체 수주잔고 이벤트 (금액 큰 순)."""
        since = (datetime.now(UTC) - timedelta(days=days)).isoformat()
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM order_backlog_events WHERE created_at>=?"
                " ORDER BY amount_ok_krw DESC NULLS LAST",
                (since,),
            ).fetchall()
        return [dict(r) for r in rows]

    def _row_to_item(self, row: sqlite3.Row) -> RawItem:
        published_at = parse_dt(row["published_at"]) or utc_now()
        try:
            meta = json.loads(row["meta_json"] or "{}")
        except json.JSONDecodeError:
            meta = {}
        return RawItem(
            source_type=row["source_type"],
            source_name=row["source_name"],
            external_id=row["external_id"],
            published_at=published_at,
            title=row["title"] or "",
            text=row["text"] or "",
            url=row["url"],
            meta=meta,
        )
