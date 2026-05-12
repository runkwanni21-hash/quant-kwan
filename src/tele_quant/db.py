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
]


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
    ) -> None:
        if not scenarios:
            return
        now = utc_now().isoformat()
        close_map = close_map or {}
        sector_map = sector_map or {}

        # Prefetch prices for LONG scenarios missing from close_map (fallback for weekly perf)
        for s in scenarios:
            symbol = getattr(s, "symbol", "")
            side = getattr(s, "side", "WATCH")
            if side == "LONG" and symbol and close_map.get(symbol) is None:
                market = getattr(s, "market", "") or (
                    "KR" if symbol.endswith((".KS", ".KQ")) else "US"
                )
                price = _fetch_signal_price_safe(symbol, market)
                if price is not None:
                    close_map[symbol] = price

        with self.connect() as conn:
            for s in scenarios:
                symbol = getattr(s, "symbol", "")
                conn.execute(
                    """
                    INSERT INTO scenario_history
                    (created_at, report_id, symbol, name, side, score, confidence,
                     entry_zone, stop_loss, target, close_price_at_report, sector, source_mode)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        close_map.get(symbol),
                        sector_map.get(symbol),
                        mode or None,
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
        """Persist LivePairSignal list to pair_watch_history. Returns inserted count."""
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
                            None,  # target_price_at_signal fetched separately if needed
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
