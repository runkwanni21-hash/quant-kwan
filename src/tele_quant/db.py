from __future__ import annotations

import contextlib
import json
import sqlite3
from collections.abc import Iterable
from datetime import datetime
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

    def save_pair_watch_signals(self, signals: list[Any]) -> int:
        """Persist LivePairSignal list to pair_watch_history. Returns inserted count.

        Uses target_price_at_signal from each signal object (populated by compute_signals).
        """
        if not signals:
            return 0
        now = utc_now().isoformat()
        inserted = 0
        with self.connect() as conn:
            for sig in signals:
                try:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO pair_watch_history
                        (created_at, source_symbol, source_name, source_market, source_sector,
                         source_return_4h, source_return_1d, source_volume_ratio,
                         target_symbol, target_name, target_market, target_sector,
                         target_return_at_signal, target_price_at_signal,
                         expected_direction, pair_score, confidence, gap_type, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            now,
                            getattr(sig, "source_symbol", ""),
                            getattr(sig, "source_name", None),
                            getattr(sig, "source_market", None),
                            getattr(sig, "source_sector", None),
                            getattr(sig, "source_return_4h", None),
                            getattr(sig, "source_return_1d", None),
                            getattr(sig, "source_volume_ratio", None),
                            getattr(sig, "target_symbol", ""),
                            getattr(sig, "target_name", None),
                            getattr(sig, "target_market", None),
                            getattr(sig, "target_sector", None),
                            getattr(sig, "target_return_4h", None)
                            or getattr(sig, "target_return_1d", None),
                            getattr(sig, "target_price_at_signal", None),
                            getattr(sig, "expected_direction", "UP"),
                            getattr(sig, "pair_score", None),
                            getattr(sig, "confidence", None),
                            getattr(sig, "gap_type", None),
                            "pending",
                        ),
                    )
                    inserted += conn.execute("SELECT changes()").fetchone()[0]
                except sqlite3.IntegrityError:
                    continue
            conn.commit()
        return inserted

    def recent_pair_watch_signals(
        self,
        since: datetime,
        until: datetime | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Query pair_watch_history for weekly review."""
        clauses = ["created_at >= ?"]
        params: list[Any] = [since.isoformat()]
        if until is not None:
            clauses.append("created_at <= ?")
            params.append(until.isoformat())
        params.append(limit)
        sql = (
            f"SELECT * FROM pair_watch_history WHERE {' AND '.join(clauses)}"
            " ORDER BY created_at DESC LIMIT ?"
        )
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

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
                         relation_type, rule_id, spillover_score)
                        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                               ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'pending', ?, ?, ?, ?, ?, ?
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
