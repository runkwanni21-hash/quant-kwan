"""Relation Graph — 관계 엣지 그래프 관리 및 내보내기. 공개 정보 기반 리서치 보조 목적."""

from __future__ import annotations

import csv
import json
import logging
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tele_quant.db import Store
    from tele_quant.relation_miner import RelationEdge

log = logging.getLogger(__name__)

_DISCLAIMER = (
    "상관관계는 인과관계가 아님. "
    "공개 정보 기반 리서치 보조. "
    "투자 판단 책임은 사용자에게 있음."
)

_YAML_HEADER = (
    "# 공개 정보 기반 리서치 보조. 투자 판단 책임은 사용자에게 있음.\n"
    "# 상관관계는 인과관계가 아님.\n"
)

_CONFIDENCE_VALUES = {"HIGH", "MEDIUM", "LOW"}


def _edge_to_yaml_dict(edge: dict[str, Any]) -> dict[str, Any]:
    """Convert a DB row dict to YAML-friendly dict.

    Converts integer boolean fields to Python bool.
    Preserves None as null (YAML null).
    Numeric fields remain as-is.
    """
    result: dict[str, Any] = {}
    bool_cols = {"active"}
    for key, value in edge.items():
        if key in bool_cols:
            if value is None:
                result[key] = None
            else:
                result[key] = bool(int(value))
        else:
            result[key] = value
    return result


class RelationGraph:
    """In-memory graph of relation edges with DB persistence and export helpers."""

    def __init__(self) -> None:
        self._edges: list[dict[str, Any]] = []

    # ── Mutation ──────────────────────────────────────────────────────────────

    def add_edges(self, edges: list[RelationEdge]) -> int:
        """Add RelationEdge objects to the graph. Returns count added."""
        count = 0
        for edge in edges:
            try:
                d = asdict(edge) if hasattr(edge, "__dataclass_fields__") else dict(edge)
                self._edges.append(d)
                count += 1
            except Exception as exc:
                log.warning("[relation_graph] add_edges failed for edge %r: %s", edge, exc)
        return count

    # ── Query ─────────────────────────────────────────────────────────────────

    def get_edges(
        self,
        active_only: bool = True,
        min_score: float = 0.0,
        confidence: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Filter edges.

        Args:
            active_only: If True, only return edges where active != 0/False.
            min_score: Minimum relation_score (inclusive).
            confidence: Allowed confidence values. None = all.

        Returns:
            List of matching edge dicts.
        """
        allowed_conf: set[str] | None = (
            {c.upper() for c in confidence} if confidence is not None else None
        )
        result: list[dict[str, Any]] = []
        for edge in self._edges:
            if active_only:
                active_val = edge.get("active")
                # None means the field wasn't in the source dataclass → treat as active=True
                if active_val is not None and not bool(active_val):
                    continue
            score = edge.get("relation_score")
            if score is not None and float(score) < min_score:
                continue
            if allowed_conf is not None:
                conf = (edge.get("confidence") or "").upper()
                if conf not in allowed_conf:
                    continue
            result.append(edge)
        return result

    def get_targets_for_source(
        self,
        source_symbol: str,
        active_only: bool = True,
    ) -> list[dict[str, Any]]:
        """Return all edges where source_symbol matches."""
        return [
            e
            for e in self.get_edges(active_only=active_only)
            if e.get("source_symbol") == source_symbol
        ]

    def get_sources_for_target(
        self,
        target_symbol: str,
        active_only: bool = True,
    ) -> list[dict[str, Any]]:
        """Return all edges where target_symbol matches."""
        return [
            e
            for e in self.get_edges(active_only=active_only)
            if e.get("target_symbol") == target_symbol
        ]

    # ── Stats ─────────────────────────────────────────────────────────────────

    def stats(self) -> dict[str, Any]:
        """Return aggregate counts by confidence, relation_type, and market."""
        by_conf: dict[str, int] = {}
        by_rel: dict[str, int] = {}
        by_market: dict[str, int] = {}

        for edge in self._edges:
            conf = (edge.get("confidence") or "UNKNOWN").upper()
            by_conf[conf] = by_conf.get(conf, 0) + 1

            rel = (edge.get("relation_type") or "UNKNOWN").upper()
            by_rel[rel] = by_rel.get(rel, 0) + 1

            src_mkt = (edge.get("source_market") or "UNKNOWN").upper()
            by_market[src_mkt] = by_market.get(src_mkt, 0) + 1

        active_count = sum(
            1 for e in self._edges if e.get("active") and bool(e["active"])
        )

        return {
            "total": len(self._edges),
            "active": active_count,
            "by_confidence": by_conf,
            "by_relation_type": by_rel,
            "by_source_market": by_market,
        }

    # ── Export ────────────────────────────────────────────────────────────────

    def export_csv(self, path: Path) -> int:
        """Write all edges to CSV. Returns row count written."""
        if not self._edges:
            return 0
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            fieldnames = list(self._edges[0].keys())
            with path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(self._edges)
            log.debug("[relation_graph] exported %d rows to %s", len(self._edges), path)
            return len(self._edges)
        except Exception as exc:
            log.warning("[relation_graph] export_csv failed: %s", exc)
            return 0

    def export_yaml(self, path: Path) -> int:
        """Write edges to YAML. Returns count written."""
        if not self._edges:
            return 0
        try:
            import yaml  # type: ignore[import-untyped]
        except ImportError:
            log.warning("[relation_graph] pyyaml not installed; falling back to manual YAML")
            yaml = None  # type: ignore[assignment]

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            now_iso = datetime.now(UTC).isoformat()
            edges_out = [_edge_to_yaml_dict(e) for e in self._edges]

            data = {
                "generated_at": now_iso,
                "total_edges": len(edges_out),
                "edges": edges_out,
            }

            with path.open("w", encoding="utf-8") as fh:
                fh.write(_YAML_HEADER)
                if yaml is not None:
                    yaml.dump(
                        data,
                        fh,
                        allow_unicode=True,
                        default_flow_style=False,
                        sort_keys=False,
                    )
                else:
                    # Minimal manual YAML serialisation (fallback)
                    fh.write(f"generated_at: \"{now_iso}\"\n")
                    fh.write(f"total_edges: {len(edges_out)}\n")
                    fh.write("edges:\n")
                    for edge in edges_out:
                        fh.write(f"  - {json.dumps(edge, ensure_ascii=False)}\n")

            log.debug("[relation_graph] export_yaml: %d edges → %s", len(edges_out), path)
            return len(edges_out)
        except Exception as exc:
            log.warning("[relation_graph] export_yaml failed: %s", exc)
            return 0

    # ── DB persistence ────────────────────────────────────────────────────────

    def load_from_db(self, store: Store) -> int:
        """Load relation_edges from DB via store.get_all_relation_edges(). Returns count loaded."""
        try:
            rows = store.get_all_relation_edges()
            self._edges = list(rows)
            log.debug("[relation_graph] loaded %d edges from DB", len(self._edges))
            return len(self._edges)
        except Exception as exc:
            log.warning("[relation_graph] load_from_db failed: %s", exc)
            return 0

    def save_to_db(self, store: Store) -> tuple[int, int]:
        """Persist current edges to DB via store.upsert_relation_edges().

        Returns:
            (inserted, updated) counts.
        """
        if not self._edges:
            return 0, 0
        try:
            inserted, updated = store.upsert_relation_edges(self._edges)
            log.debug("[relation_graph] save_to_db: inserted=%d updated=%d", inserted, updated)
            return inserted, updated
        except Exception as exc:
            log.warning("[relation_graph] save_to_db failed: %s", exc)
            return 0, 0


# ── Public report builder ─────────────────────────────────────────────────────


def build_relation_report(store: Store, top_n: int = 30) -> str:
    """Build a text report of the top relation edges.

    Args:
        store: Store instance with get_all_relation_edges().
        top_n: Maximum number of edges to list.

    Returns:
        Formatted report string, or "" if no edges.
    """
    try:
        edges = store.get_all_relation_edges()
    except Exception as exc:
        log.warning("[relation_graph] build_relation_report: DB load failed: %s", exc)
        return ""

    if not edges:
        return ""

    # Counts by confidence
    conf_counts: dict[str, int] = {}
    for e in edges:
        conf = (e.get("confidence") or "UNKNOWN").upper()
        conf_counts[conf] = conf_counts.get(conf, 0) + 1

    high = conf_counts.get("HIGH", 0)
    medium = conf_counts.get("MEDIUM", 0)
    low = conf_counts.get("LOW", 0)

    lines: list[str] = [
        f"관계 엣지 그래프 (총 {len(edges)}개)",
        f"HIGH={high} / MEDIUM={medium} / LOW={low}",
        "",
        f"Top {top_n} (score 기준):",
    ]

    # Sort by relation_score descending, nulls last
    def _score_key(e: dict[str, Any]) -> float:
        v = e.get("relation_score")
        return float(v) if v is not None else 0.0

    top = sorted(edges, key=_score_key, reverse=True)[:top_n]

    for e in top:
        src = e.get("source_symbol") or e.get("source") or "?"
        src_name = e.get("source_name") or src
        tgt = e.get("target_symbol") or e.get("target") or "?"
        tgt_name = e.get("target_name") or tgt
        rel_type = (e.get("relation_type") or "?").upper()
        direction = (e.get("direction") or "?").upper()
        conf = (e.get("confidence") or "?").upper()
        score_val = e.get("relation_score")
        score_str = f"{float(score_val):.1f}" if score_val is not None else "?"
        lines.append(
            f"{src}({src_name}) → {tgt}({tgt_name})"
            f" [{rel_type}, {direction}, {conf}, score={score_str}]"
        )

    lines.append("")
    lines.append(_DISCLAIMER)
    return "\n".join(lines)


__all__ = ["RelationGraph", "build_relation_report"]
