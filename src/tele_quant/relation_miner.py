"""Relation Miner — 급등주 수혜/피해 관계 엣지 생성 엔진.

상관관계는 인과관계가 아님.
공개 정보 기반 리서치 보조 목적이며 투자 판단 책임은 사용자에게 있음.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tele_quant.top_mover_miner import TopMover

log = logging.getLogger(__name__)

DISCLAIMER = (
    "상관관계는 인과관계가 아님. "
    "공개 정보 기반 리서치 보조. "
    "투자 판단 책임은 사용자에게 있음."
)


# ── Enum types ────────────────────────────────────────────────────────────────


class RelationType(StrEnum):
    BENEFICIARY = "BENEFICIARY"
    VICTIM = "VICTIM"
    SUPPLIER = "SUPPLIER"
    CUSTOMER = "CUSTOMER"
    PEER_MOMENTUM = "PEER_MOMENTUM"
    COMPETITOR = "COMPETITOR"
    INPUT_COST_BENEFIT = "INPUT_COST_BENEFIT"
    INPUT_COST_VICTIM = "INPUT_COST_VICTIM"
    DEMAND_SPILLOVER = "DEMAND_SPILLOVER"
    POLICY_BENEFIT = "POLICY_BENEFIT"
    RATE_SENSITIVE = "RATE_SENSITIVE"
    COMMODITY_SENSITIVE = "COMMODITY_SENSITIVE"
    FX_SENSITIVE = "FX_SENSITIVE"
    BACKLOG_SPILLOVER = "BACKLOG_SPILLOVER"
    AI_CAPEX_SPILLOVER = "AI_CAPEX_SPILLOVER"
    DEFENSE_GEOPOLITICS = "DEFENSE_GEOPOLITICS"
    BIO_CLINICAL_READTHROUGH = "BIO_CLINICAL_READTHROUGH"


class Direction(StrEnum):
    UP_LEADS_UP = "UP_LEADS_UP"
    UP_LEADS_DOWN = "UP_LEADS_DOWN"
    DOWN_LEADS_DOWN = "DOWN_LEADS_DOWN"
    DOWN_LEADS_UP = "DOWN_LEADS_UP"


# ── Data model ────────────────────────────────────────────────────────────────


@dataclass
class RelationEdge:
    """급등주 → 수혜/피해 관계 엣지."""

    source_symbol: str
    source_name: str
    source_market: str
    source_sector: str
    target_symbol: str
    target_name: str
    target_market: str
    target_sector: str
    relation_type: str           # RelationType value
    direction: str               # Direction value
    expected_lag_hours: int
    confidence: str              # "HIGH", "MEDIUM", "LOW", "INACTIVE"
    relation_score: float
    evidence_type: str           # "rule", "price_corr", "web_news", "disclosure", "sector"
    evidence_url: str
    evidence_title: str
    evidence_summary: str
    rule_id: str
    source_return_3m_pct: float
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    # Victim descriptions must avoid "무조건 하락" language.
    # Use "상대적 비용 압박 리스크" or "수요 전환 리스크" in summaries.


# ── Score formula ─────────────────────────────────────────────────────────────


def compute_relation_score(
    rule_match: float,
    web_ev: float,
    lag_corr: float,
    event_study: float,
    sector_chain: float,
    liquidity_qual: float,
    ev_freshness: float,
) -> float:
    """Compute composite relation score (0-100).

    Weights:
        rule_match    0.25
        web_ev        0.20
        lag_corr      0.20
        event_study   0.15
        sector_chain  0.10
        liquidity_qual 0.05
        ev_freshness  0.05
    """
    score = (
        rule_match * 0.25
        + web_ev * 0.20
        + lag_corr * 0.20
        + event_study * 0.15
        + sector_chain * 0.10
        + liquidity_qual * 0.05
        + ev_freshness * 0.05
    )
    return min(100.0, max(0.0, score))


def classify_confidence(score: float) -> str:
    """Map a relation score to a confidence label."""
    if score >= 85:
        return "HIGH"
    if score >= 70:
        return "MEDIUM"
    if score >= 50:
        return "LOW"
    return "INACTIVE"


# ── YAML loading helpers ──────────────────────────────────────────────────────


def _load_yaml_safe(path: Path) -> list[dict[str, Any]]:
    """Load a YAML file and return its list contents. Returns [] on any error."""
    try:
        import yaml  # type: ignore[import-untyped]

        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        if isinstance(data, dict):
            return data.get("rules", data.get("cycles", []))
        if isinstance(data, list):
            return data
        return []
    except Exception as exc:
        log.warning("[relation_miner] YAML load failed %s: %s", path, exc)
        return []


@lru_cache(maxsize=8)
def _load_yaml_cached(path_str: str) -> tuple[dict[str, Any], ...]:
    """Cached YAML load; returns tuple (immutable) for lru_cache compatibility."""
    rules = _load_yaml_safe(Path(path_str))
    return tuple(rules)  # type: ignore[return-value]


# ── Config path resolution ────────────────────────────────────────────────────


def _default_config_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "config"


# ── Universe helpers ──────────────────────────────────────────────────────────


def _get_universe() -> list[str]:
    try:
        from tele_quant.relation_feed import _UNIVERSE_KR, _UNIVERSE_US

        return list(_UNIVERSE_US) + list(_UNIVERSE_KR)
    except ImportError:
        return []


# ── Main class ────────────────────────────────────────────────────────────────


class RelationMiner:
    """급등주로부터 수혜/피해 관계 엣지를 생성하는 엔진."""

    def __init__(
        self,
        supply_chain_path: Path | str | None = None,
        pair_watch_path: Path | str | None = None,
        sector_cycle_path: Path | str | None = None,
    ) -> None:
        cfg = _default_config_dir()
        self._supply_chain_path = Path(supply_chain_path or cfg / "supply_chain_rules.yml")
        self._pair_watch_path = Path(pair_watch_path or cfg / "pair_watch_rules.yml")
        self._sector_cycle_path = Path(sector_cycle_path or cfg / "sector_cycle_rules.yml")

        self._supply_rules: list[dict[str, Any]] | None = None
        self._pair_rules: list[dict[str, Any]] | None = None
        self._sector_rules: list[dict[str, Any]] | None = None

    # ── Rule loaders ──────────────────────────────────────────────────────────

    def _load_supply_rules(self) -> list[dict[str, Any]]:
        if self._supply_rules is None:
            self._supply_rules = list(
                _load_yaml_cached(str(self._supply_chain_path))
            )
            log.info("[relation_miner] loaded supply rules: %d", len(self._supply_rules))
        return self._supply_rules

    def _load_pair_rules(self) -> list[dict[str, Any]]:
        if self._pair_rules is None:
            self._pair_rules = list(
                _load_yaml_cached(str(self._pair_watch_path))
            )
            log.info("[relation_miner] loaded pair rules: %d", len(self._pair_rules))
        return self._pair_rules

    def _load_sector_rules(self) -> list[dict[str, Any]]:
        if self._sector_rules is None:
            self._sector_rules = list(
                _load_yaml_cached(str(self._sector_cycle_path))
            )
            log.info("[relation_miner] loaded sector cycle rules: %d", len(self._sector_rules))
        return self._sector_rules

    # ── Edge builder ──────────────────────────────────────────────────────────

    def _make_edge(
        self,
        source_symbol: str,
        source_name: str,
        source_market: str,
        source_sector: str,
        source_return_3m_pct: float,
        target_symbol: str,
        target_name: str,
        target_market: str,
        target_sector: str,
        relation_type: str,
        direction: str,
        expected_lag_hours: int,
        rule_match: float,
        web_ev: float,
        lag_corr: float,
        event_study: float,
        sector_chain: float,
        liquidity_qual: float,
        ev_freshness: float,
        evidence_type: str,
        evidence_url: str,
        evidence_title: str,
        evidence_summary: str,
        rule_id: str,
    ) -> RelationEdge:
        score = compute_relation_score(
            rule_match, web_ev, lag_corr, event_study,
            sector_chain, liquidity_qual, ev_freshness,
        )
        confidence = classify_confidence(score)
        return RelationEdge(
            source_symbol=source_symbol,
            source_name=source_name,
            source_market=source_market,
            source_sector=source_sector,
            target_symbol=target_symbol,
            target_name=target_name,
            target_market=target_market,
            target_sector=target_sector,
            relation_type=relation_type,
            direction=direction,
            expected_lag_hours=expected_lag_hours,
            confidence=confidence,
            relation_score=score,
            evidence_type=evidence_type,
            evidence_url=evidence_url,
            evidence_title=evidence_title,
            evidence_summary=evidence_summary,
            rule_id=rule_id,
            source_return_3m_pct=source_return_3m_pct,
        )

    # ── mine_from_rules ───────────────────────────────────────────────────────

    def mine_from_rules(
        self,
        source_symbol: str,
        source_return_3m_pct: float,
        source_market: str,
    ) -> list[RelationEdge]:
        """Generate edges from supply_chain_rules and pair_watch_rules."""
        edges: list[RelationEdge] = []
        created_at = datetime.now(UTC)

        try:
            from tele_quant.relation_feed import _NAME_MAP, _SECTOR_MAP
            src_name = _NAME_MAP.get(source_symbol, source_symbol)
            src_sector = _SECTOR_MAP.get(source_symbol, "")
        except ImportError:
            src_name = source_symbol
            src_sector = ""

        # ── Supply chain rules ────────────────────────────────────────────────
        try:
            supply_rules = self._load_supply_rules()
            for rule in supply_rules:
                rule_id = rule.get("id", "")
                rule_market = rule.get("market", "BOTH")
                if rule_market not in (source_market, "BOTH", "CROSS"):
                    continue

                source_syms = {
                    s.get("symbol", "") for s in rule.get("source_symbols", [])
                }
                keyword_match = False
                for kw in rule.get("source_keywords", []):
                    if kw and kw.lower() in src_name.lower():
                        keyword_match = True
                        break

                is_direct_hit = source_symbol in source_syms
                if not is_direct_hit and not keyword_match:
                    continue

                rule_match_score = 90.0 if is_direct_hit else 70.0

                # Beneficiaries (UP_LEADS_UP)
                for ben in rule.get("beneficiaries", []):
                    rel_type = ben.get("relation_type", RelationType.BENEFICIARY)
                    connection = ben.get("connection", "")
                    sector = ben.get("sector", "")
                    for sym_entry in ben.get("symbols", []):
                        tsym = sym_entry.get("symbol", "")
                        tname = sym_entry.get("name", "")
                        if not tsym or tsym == source_symbol:
                            continue
                        tmarket = "KR" if tsym.endswith((".KS", ".KQ")) else "US"
                        lag_hours = 24 if tmarket == source_market else 4
                        edge = self._make_edge(
                            source_symbol=source_symbol,
                            source_name=src_name,
                            source_market=source_market,
                            source_sector=src_sector,
                            source_return_3m_pct=source_return_3m_pct,
                            target_symbol=tsym,
                            target_name=tname,
                            target_market=tmarket,
                            target_sector=sector,
                            relation_type=rel_type,
                            direction=Direction.UP_LEADS_UP,
                            expected_lag_hours=lag_hours,
                            rule_match=rule_match_score,
                            web_ev=0.0,
                            lag_corr=0.0,
                            event_study=0.0,
                            sector_chain=80.0,
                            liquidity_qual=50.0,
                            ev_freshness=50.0,
                            evidence_type="rule",
                            evidence_url="",
                            evidence_title=rule.get("chain_name", ""),
                            evidence_summary=connection,
                            rule_id=rule_id,
                        )
                        edge.created_at = created_at
                        edges.append(edge)

                # Victims on bearish (UP_LEADS_DOWN for cost pass-through)
                for vic in rule.get("victims_on_bearish", []):
                    rel_type = vic.get("relation_type", RelationType.VICTIM)
                    connection = vic.get("connection", "")
                    sector = vic.get("sector", "")
                    for sym_entry in vic.get("symbols", []):
                        tsym = sym_entry.get("symbol", "")
                        tname = sym_entry.get("name", "")
                        if not tsym or tsym == source_symbol:
                            continue
                        tmarket = "KR" if tsym.endswith((".KS", ".KQ")) else "US"
                        lag_hours = 24 if tmarket == source_market else 4
                        # Victim descriptions: 상대적 비용 압박 리스크 / 수요 전환 리스크
                        summary = (
                            f"상대적 비용 압박 리스크 또는 수요 전환 리스크: {connection}"
                            if connection
                            else "상대적 비용 압박 리스크 또는 수요 전환 리스크"
                        )
                        edge = self._make_edge(
                            source_symbol=source_symbol,
                            source_name=src_name,
                            source_market=source_market,
                            source_sector=src_sector,
                            source_return_3m_pct=source_return_3m_pct,
                            target_symbol=tsym,
                            target_name=tname,
                            target_market=tmarket,
                            target_sector=sector,
                            relation_type=rel_type,
                            direction=Direction.UP_LEADS_DOWN,
                            expected_lag_hours=lag_hours,
                            rule_match=rule_match_score,
                            web_ev=0.0,
                            lag_corr=0.0,
                            event_study=0.0,
                            sector_chain=80.0,
                            liquidity_qual=50.0,
                            ev_freshness=50.0,
                            evidence_type="rule",
                            evidence_url="",
                            evidence_title=rule.get("chain_name", ""),
                            evidence_summary=summary,
                            rule_id=rule_id,
                        )
                        edge.created_at = created_at
                        edges.append(edge)

        except Exception as exc:
            log.warning("[relation_miner] supply rule mining failed %s: %s", source_symbol, exc)

        # ── Pair watch rules (PEER_MOMENTUM) ──────────────────────────────────
        try:
            pair_rules = self._load_pair_rules()
            for rule in pair_rules:
                if rule.get("source", "") != source_symbol:
                    continue
                rule_id = rule.get("id", "")
                direction_str = rule.get("direction", "UP_LEADS_UP")
                for tsym in rule.get("targets", []):
                    if not tsym or tsym == source_symbol:
                        continue
                    tmarket = "KR" if str(tsym).endswith((".KS", ".KQ")) else "US"
                    try:
                        from tele_quant.relation_feed import _NAME_MAP, _SECTOR_MAP
                        tname = _NAME_MAP.get(tsym, str(tsym))
                        tsector = _SECTOR_MAP.get(tsym, "")
                    except ImportError:
                        tname = str(tsym)
                        tsector = ""

                    lag_hours = 4 if tmarket == source_market else 8
                    edge = self._make_edge(
                        source_symbol=source_symbol,
                        source_name=src_name,
                        source_market=source_market,
                        source_sector=src_sector,
                        source_return_3m_pct=source_return_3m_pct,
                        target_symbol=tsym,
                        target_name=tname,
                        target_market=tmarket,
                        target_sector=tsector,
                        relation_type=RelationType.PEER_MOMENTUM,
                        direction=direction_str,
                        expected_lag_hours=lag_hours,
                        rule_match=90.0,
                        web_ev=0.0,
                        lag_corr=0.0,
                        event_study=0.0,
                        sector_chain=70.0,
                        liquidity_qual=50.0,
                        ev_freshness=50.0,
                        evidence_type="rule",
                        evidence_url="",
                        evidence_title=rule.get("note", "pair watch"),
                        evidence_summary=rule.get("note", ""),
                        rule_id=rule_id,
                    )
                    edge.created_at = created_at
                    edges.append(edge)
        except Exception as exc:
            log.warning("[relation_miner] pair rule mining failed %s: %s", source_symbol, exc)

        return edges

    # ── mine_from_price_correlation ───────────────────────────────────────────

    def mine_from_price_correlation(
        self,
        source_symbol: str,
        target_symbols: list[str],
        source_return_3m_pct: float,
        source_market: str,
        lookback_days: int = 252,
    ) -> list[RelationEdge]:
        """Generate edges using price correlation and conditional probability."""
        edges: list[RelationEdge] = []
        if not target_symbols:
            return edges

        try:
            import numpy as np
            import pandas as pd
            import yfinance as yf

            all_syms = [source_symbol, *list(target_symbols)]
            data = yf.download(
                all_syms,
                period=f"{lookback_days}d",
                interval="1d",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            if data is None or data.empty:
                return edges

            try:
                closes: pd.DataFrame = data["Close"]
            except (KeyError, TypeError):
                closes = data

            closes = closes.dropna(how="all")
            if source_symbol not in closes.columns:
                return edges

            src_series: pd.Series = closes[source_symbol].dropna()
            src_returns = src_series.pct_change().dropna()

            bullish_days = src_returns[src_returns >= 0.03].index
            bearish_days = src_returns[src_returns <= -0.03].index

        except Exception as exc:
            log.warning(
                "[relation_miner] price_corr fetch failed %s: %s", source_symbol, exc
            )
            return edges

        try:
            from tele_quant.relation_feed import _NAME_MAP, _SECTOR_MAP
            src_name = _NAME_MAP.get(source_symbol, source_symbol)
            src_sector = _SECTOR_MAP.get(source_symbol, "")
        except ImportError:
            src_name = source_symbol
            src_sector = ""

        created_at = datetime.now(UTC)

        for tsym in target_symbols:
            if tsym == source_symbol:
                continue
            try:
                if tsym not in closes.columns:
                    continue
                tgt_series: Any = closes[tsym].dropna()
                if len(tgt_series) < 20:
                    continue

                tgt_returns = tgt_series.pct_change().dropna()

                # Align
                common_idx = src_returns.index.intersection(tgt_returns.index)
                if len(common_idx) < 20:
                    continue

                src_aligned = src_returns.loc[common_idx]
                tgt_aligned = tgt_returns.loc[common_idx]

                try:
                    import numpy as np

                    corr = float(np.corrcoef(src_aligned.values, tgt_aligned.values)[0, 1])
                except Exception:
                    corr = 0.0

                # Bullish conditional prob
                bull_idx = bullish_days.intersection(tgt_aligned.index)
                if len(bull_idx) > 0:
                    tgt_after_bull = tgt_aligned.loc[bull_idx]
                    cond_prob_up = float((tgt_after_bull > 0.01).mean())
                    avg_ret_up = float(tgt_after_bull.mean() * 100)
                else:
                    cond_prob_up = 0.0
                    avg_ret_up = 0.0

                # Bearish conditional prob
                bear_idx = bearish_days.intersection(tgt_aligned.index)
                if len(bear_idx) > 0:
                    tgt_after_bear = tgt_aligned.loc[bear_idx]
                    cond_prob_down = float((tgt_after_bear < -0.01).mean())
                    avg_ret_down = float(tgt_after_bear.mean() * 100)
                else:
                    cond_prob_down = 0.0
                    avg_ret_down = 0.0

                tmarket = "KR" if str(tsym).endswith((".KS", ".KQ")) else "US"
                try:
                    from tele_quant.relation_feed import _NAME_MAP, _SECTOR_MAP
                    tname = _NAME_MAP.get(tsym, tsym)
                    tsector = _SECTOR_MAP.get(tsym, "")
                except ImportError:
                    tname = tsym
                    tsector = ""

                # Determine direction and type from conditional probs
                candidates: list[tuple[float, str, str, float, str]] = []

                if cond_prob_up > 0.6:
                    if avg_ret_up > 0:
                        direction = Direction.UP_LEADS_UP
                        rel_type = RelationType.BENEFICIARY
                    else:
                        direction = Direction.UP_LEADS_DOWN
                        rel_type = RelationType.VICTIM
                    score_raw = min(100.0, cond_prob_up * 100 + abs(avg_ret_up) * 5)
                    summary_up = (
                        f"corr={corr:.2f} cond_prob={cond_prob_up:.2f} "
                        f"avg_ret={avg_ret_up:.1f}%"
                    )
                    candidates.append((score_raw, str(direction), str(rel_type), cond_prob_up, summary_up))

                if cond_prob_down > 0.6:
                    if avg_ret_down < 0:
                        direction_d = Direction.DOWN_LEADS_DOWN
                        rel_type_d = RelationType.VICTIM
                    else:
                        direction_d = Direction.DOWN_LEADS_UP
                        rel_type_d = RelationType.BENEFICIARY
                    score_raw_d = min(100.0, cond_prob_down * 100 + abs(avg_ret_down) * 5)
                    summary_down = (
                        f"corr={corr:.2f} cond_prob={cond_prob_down:.2f} "
                        f"avg_ret={avg_ret_down:.1f}%"
                    )
                    candidates.append((score_raw_d, str(direction_d), str(rel_type_d), cond_prob_down, summary_down))

                for lag_corr_score, direction_val, rel_type_val, _cp, summary in candidates:
                    # Victim summary: must use safe language
                    if rel_type_val in (RelationType.VICTIM, "VICTIM"):
                        display_summary = f"상대적 비용 압박 리스크 또는 수요 전환 리스크 — {summary}"
                    else:
                        display_summary = summary

                    edge = self._make_edge(
                        source_symbol=source_symbol,
                        source_name=src_name,
                        source_market=source_market,
                        source_sector=src_sector,
                        source_return_3m_pct=source_return_3m_pct,
                        target_symbol=tsym,
                        target_name=tname,
                        target_market=tmarket,
                        target_sector=tsector,
                        relation_type=rel_type_val,
                        direction=direction_val,
                        expected_lag_hours=4,
                        rule_match=0.0,
                        web_ev=0.0,
                        lag_corr=lag_corr_score,
                        event_study=0.0,
                        sector_chain=0.0,
                        liquidity_qual=50.0,
                        ev_freshness=50.0,
                        evidence_type="price_corr",
                        evidence_url="",
                        evidence_title="",
                        evidence_summary=display_summary,
                        rule_id="",
                    )
                    edge.created_at = created_at
                    edges.append(edge)

            except Exception as exc:
                log.debug("[relation_miner] price_corr target %s: %s", tsym, exc)
                continue

        return edges

    # ── mine_from_sector ──────────────────────────────────────────────────────

    def mine_from_sector(
        self,
        source_symbol: str,
        source_sector: str,
        source_market: str,
        source_return_3m_pct: float,
    ) -> list[RelationEdge]:
        """Generate peer momentum edges from sector cycle rules."""
        edges: list[RelationEdge] = []
        if not source_sector:
            return edges

        created_at = datetime.now(UTC)

        try:
            from tele_quant.relation_feed import _NAME_MAP, _SECTOR_MAP
            src_name = _NAME_MAP.get(source_symbol, source_symbol)
        except ImportError:
            src_name = source_symbol

        try:
            sector_rules = self._load_sector_rules()
            for cycle in sector_rules:
                # Check if source_sector matches any sector in beneficiaries
                all_bens = cycle.get("first_order_beneficiaries", []) + cycle.get(
                    "second_order_beneficiaries", []
                )
                for ben in all_bens:
                    ben_sector = ben.get("sector", "")
                    if not ben_sector:
                        continue
                    if ben_sector not in source_sector and source_sector not in ben_sector:
                        continue
                    for sym_entry in ben.get("symbols", [])[:3]:  # max 3 peers
                        tsym = sym_entry.get("symbol", "")
                        tname = sym_entry.get("name", "")
                        if not tsym or tsym == source_symbol:
                            continue
                        tmarket = "KR" if str(tsym).endswith((".KS", ".KQ")) else "US"
                        try:
                            from tele_quant.relation_feed import _SECTOR_MAP
                            tsector = _SECTOR_MAP.get(tsym, ben_sector)
                        except ImportError:
                            tsector = ben_sector

                        edge = self._make_edge(
                            source_symbol=source_symbol,
                            source_name=src_name,
                            source_market=source_market,
                            source_sector=source_sector,
                            source_return_3m_pct=source_return_3m_pct,
                            target_symbol=tsym,
                            target_name=tname,
                            target_market=tmarket,
                            target_sector=tsector,
                            relation_type=RelationType.PEER_MOMENTUM,
                            direction=Direction.UP_LEADS_UP,
                            expected_lag_hours=24,
                            rule_match=0.0,
                            web_ev=0.0,
                            lag_corr=0.0,
                            event_study=0.0,
                            sector_chain=60.0,
                            liquidity_qual=50.0,
                            ev_freshness=50.0,
                            evidence_type="sector",
                            evidence_url="",
                            evidence_title=cycle.get("name", ""),
                            evidence_summary=ben.get("connection", ""),
                            rule_id=cycle.get("cycle_id", ""),
                        )
                        edge.created_at = created_at
                        edges.append(edge)
        except Exception as exc:
            log.warning(
                "[relation_miner] sector mining failed %s: %s", source_symbol, exc
            )

        return edges

    # ── mine_for_mover ────────────────────────────────────────────────────────

    def mine_for_mover(
        self,
        mover: TopMover,
        target_universe: list[str] | None = None,
    ) -> list[RelationEdge]:
        """Mine all relation edges for a single TopMover."""
        all_edges: list[RelationEdge] = []

        # 1. Rule-based edges
        try:
            rule_edges = self.mine_from_rules(
                source_symbol=mover.symbol,
                source_return_3m_pct=mover.return_pct,
                source_market=mover.market,
            )
            all_edges.extend(rule_edges)
        except Exception as exc:
            log.warning("[relation_miner] mine_from_rules %s: %s", mover.symbol, exc)

        # 2. Price correlation edges
        try:
            universe = target_universe or _get_universe()
            # Limit correlation universe to 50 to avoid excessive yfinance calls
            corr_targets = [s for s in universe if s != mover.symbol][:50]
            if corr_targets:
                corr_edges = self.mine_from_price_correlation(
                    source_symbol=mover.symbol,
                    target_symbols=corr_targets,
                    source_return_3m_pct=mover.return_pct,
                    source_market=mover.market,
                )
                all_edges.extend(corr_edges)
        except Exception as exc:
            log.warning("[relation_miner] mine_from_price_correlation %s: %s", mover.symbol, exc)

        # 3. Sector edges
        try:
            sector_edges = self.mine_from_sector(
                source_symbol=mover.symbol,
                source_sector=mover.sector,
                source_market=mover.market,
                source_return_3m_pct=mover.return_pct,
            )
            all_edges.extend(sector_edges)
        except Exception as exc:
            log.warning("[relation_miner] mine_from_sector %s: %s", mover.symbol, exc)

        # ── Deduplicate by (source, target, relation_type, direction) ─────────
        seen: set[tuple[str, str, str, str]] = set()
        deduped: list[RelationEdge] = []
        for edge in all_edges:
            key = (edge.source_symbol, edge.target_symbol, edge.relation_type, edge.direction)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(edge)

        # ── Score filter (keep >= 50; INACTIVE edges also retained) ──────────
        filtered = [e for e in deduped if e.relation_score >= 50]
        inactive = [e for e in deduped if e.relation_score < 50]

        # ── Ensure at least 2 beneficiaries and 2 victims ─────────────────────
        beneficiary_types = {RelationType.BENEFICIARY, "BENEFICIARY"}
        victim_types = {RelationType.VICTIM, "VICTIM"}

        current_bens = [e for e in filtered if e.relation_type in beneficiary_types]
        current_vics = [e for e in filtered if e.relation_type in victim_types]

        if len(current_bens) < 2:
            extra = [e for e in inactive if e.relation_type in beneficiary_types]
            extra.sort(key=lambda e: -e.relation_score)
            for e in extra[: 2 - len(current_bens)]:
                filtered.append(e)

        if len(current_vics) < 2:
            extra = [e for e in inactive if e.relation_type in victim_types]
            extra.sort(key=lambda e: -e.relation_score)
            for e in extra[: 2 - len(current_vics)]:
                filtered.append(e)

        filtered.sort(key=lambda e: -e.relation_score)
        log.info(
            "[relation_miner] mine_for_mover %s: %d edges (pre-dedup=%d)",
            mover.symbol, len(filtered), len(all_edges),
        )
        return filtered

    # ── mine_all ──────────────────────────────────────────────────────────────

    def mine_all(
        self,
        movers: list[TopMover],
        max_per_mover: int = 10,
    ) -> list[RelationEdge]:
        """Mine relation edges for all movers; deduplicate globally."""
        all_edges: list[RelationEdge] = []
        total = len(movers)

        for idx, mover in enumerate(movers, 1):
            log.info(
                "[relation_miner] mine_all progress: %d/%d symbol=%s",
                idx, total, mover.symbol,
            )
            try:
                edges = self.mine_for_mover(mover)
                all_edges.extend(edges[:max_per_mover])
            except Exception as exc:
                log.warning("[relation_miner] mine_all %s failed: %s", mover.symbol, exc)

        # Global dedup
        seen: set[tuple[str, str, str, str]] = set()
        result: list[RelationEdge] = []
        for edge in all_edges:
            key = (edge.source_symbol, edge.target_symbol, edge.relation_type, edge.direction)
            if key in seen:
                continue
            seen.add(key)
            result.append(edge)

        result.sort(key=lambda e: -e.relation_score)
        log.info("[relation_miner] mine_all done: %d edges from %d movers", len(result), total)
        return result


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "DISCLAIMER",
    "Direction",
    "RelationEdge",
    "RelationMiner",
    "RelationType",
    "classify_confidence",
    "compute_relation_score",
]
