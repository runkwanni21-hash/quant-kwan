from __future__ import annotations

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
"""


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

    def save_mover_chain(self, relation_feed: Any) -> int:
        """Save lead-lag rows from relation feed. Returns count of inserted rows."""
        try:
            leadlag = getattr(relation_feed, "leadlag", [])
        except Exception:
            return 0
        if not leadlag:
            return 0
        now = utc_now().isoformat()
        inserted = 0
        with self.connect() as conn:
            for row in leadlag:
                try:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO mover_chain_history
                        (created_at, asof_date, source_symbol, source_return_pct,
                         source_move_type, target_symbol, relation_type, lag_days,
                         conditional_prob, lift, confidence)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            now,
                            getattr(row, "asof_date", ""),
                            getattr(row, "source_symbol", ""),
                            getattr(row, "source_return_pct", None),
                            getattr(row, "source_move_type", None),
                            getattr(row, "target_symbol", ""),
                            getattr(row, "relation_type", None),
                            getattr(row, "lag_days", None),
                            getattr(row, "conditional_prob", None),
                            getattr(row, "lift", None),
                            getattr(row, "confidence", None),
                        ),
                    )
                    inserted += conn.execute("SELECT changes()").fetchone()[0]
                except sqlite3.IntegrityError:
                    continue
            conn.commit()
        return inserted

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
