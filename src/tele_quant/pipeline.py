from __future__ import annotations

import asyncio
import logging
import time
from datetime import timedelta
from typing import Any

from tele_quant.db import Store
from tele_quant.dedupe import Deduper
from tele_quant.headline_cleaner import apply_final_report_cleaner
from tele_quant.models import RawItem, RunStats, utc_now
from tele_quant.ollama_client import OllamaClient
from tele_quant.reports.naver import fetch_naver_reports
from tele_quant.reports.yahoo import fetch_market_snapshot
from tele_quant.settings import Settings
from tele_quant.telegram_client import TelegramGateway
from tele_quant.telegram_sender import TelegramSender

log = logging.getLogger(__name__)


def _apply_smart_read_boost(ranked: Any, smart_result: Any) -> Any:
    """smart_read 결과를 반영해 positive/negative 클러스터 순서를 재조정.

    bullish_items에 있는 종목명이 headline/tickers에 포함된 클러스터를 앞으로 이동.
    기존 bucket 크기·구성은 변경하지 않음.
    """
    try:
        bull_names = {
            b.get("name", "").strip().lower()
            for b in (getattr(smart_result, "bullish_items", []) or [])
            if isinstance(b, dict) and b.get("name")
        }
        bear_names = {
            b.get("name", "").strip().lower()
            for b in (getattr(smart_result, "bearish_items", []) or [])
            if isinstance(b, dict) and b.get("name")
        }

        def _reorder(clusters: list, priority_names: set) -> list:
            if not priority_names:
                return clusters
            matched, rest = [], []
            for c in clusters:
                text = (
                    getattr(c, "headline", "") + " " + " ".join(getattr(c, "tickers", []))
                ).lower()
                if any(n in text for n in priority_names if n):
                    matched.append(c)
                else:
                    rest.append(c)
            return matched + rest

        from dataclasses import replace

        return replace(
            ranked,
            positive_stock=_reorder(ranked.positive_stock, bull_names),
            negative_stock=_reorder(ranked.negative_stock, bear_names),
        )
    except Exception:
        return ranked


def _load_watchlist(settings: Settings) -> Any:
    """watchlist.yml을 로드. 실패하면 None 반환."""
    if not settings.watchlist_enabled:
        return None
    try:
        from tele_quant.watchlist import load_watchlist

        cfg = load_watchlist(settings.watchlist_path)
        if cfg:
            total = sum(len(g.symbols) for g in cfg.groups.values())
            log.info("[watchlist] loaded: %d groups, %d symbols", len(cfg.groups), total)
        return cfg
    except Exception as exc:
        log.warning("[watchlist] load failed: %s", exc)
        return None


def _load_providers(settings: Settings) -> dict[str, bool]:
    """provider 목록을 로드."""
    try:
        from tele_quant.provider_config import available_providers

        return available_providers(load_external=True)
    except Exception:
        return {"yfinance": True}


class TeleQuantPipeline:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.settings.ensure_runtime_dirs()
        self.store = Store(settings.sqlite_path)
        self.ollama = OllamaClient(settings)

    async def _collect_and_dedupe(
        self,
        gateway: TelegramGateway,
        lookback: float,
        watchlist_cfg: Any = None,
    ) -> tuple[list[RawItem], RunStats]:
        stats = RunStats()

        telegram_items = await gateway.fetch_recent_messages(hours=lookback)
        stats.telegram_items = len(telegram_items)

        report_items = await fetch_naver_reports(self.settings, hours=max(lookback, 24))
        stats.report_items = len(report_items)

        # RSS 영어 뉴스 수집 (PR Newswire / GlobeNewswire / BusinessWire / Google News)
        rss_items: list[RawItem] = []
        if getattr(self.settings, "rss_enabled", True):
            try:
                from tele_quant.rss_collector import fetch_all_rss

                wl_syms: list[str] = []
                if watchlist_cfg is not None:
                    for _grp in watchlist_cfg.groups.values():
                        wl_syms.extend(
                            s for s in _grp.symbols if "." not in s and s.isalpha()
                        )
                rss_items = await asyncio.to_thread(
                    fetch_all_rss,
                    self.settings,
                    wl_syms[:8],
                    max(lookback, 24.0),
                )
                log.info("[pipeline] rss items: %d", len(rss_items))
            except Exception as _rss_exc:
                log.debug("[pipeline] rss fetch failed: %s", _rss_exc)

        all_items = telegram_items + report_items + rss_items
        inserted = self.store.insert_items(all_items)
        stats.inserted_items = len(inserted)

        since = utc_now() - timedelta(hours=max(lookback, 24))
        candidates = self.store.recent_items(since=since, limit=2000)
        stats.candidate_items = len(candidates)

        deduper = Deduper(self.settings, self.ollama)
        kept = await deduper.dedupe(candidates)
        stats.kept_items = len(kept)
        stats.duplicate_items = max(0, stats.candidate_items - stats.kept_items)

        return kept, stats

    # ------------------------------------------------------------------
    # Deep mode: full LLM synthesis
    # ------------------------------------------------------------------

    async def _summarize_with_evidence(
        self,
        kept: list[RawItem],
        market_snapshot: list[Any],
        stats: RunStats,
        hours: float,
    ) -> str:
        """Build evidence clusters and call Ollama with structured pack. Falls back to raw."""
        try:
            from tele_quant.evidence import build_evidence_clusters

            clusters = await asyncio.to_thread(build_evidence_clusters, kept, self.settings)
            log.info("[pipeline] evidence clusters built: %d", len(clusters))
            if clusters:
                return await self.ollama.summarize_from_clusters(
                    clusters, market_snapshot, stats, hours=hours
                )
        except Exception as exc:
            log.warning("[pipeline] evidence build failed, falling back to raw: %s", exc)

        return await self.ollama.summarize(kept, market_snapshot, stats, hours=hours)

    async def _run_analysis(self, items: list[RawItem], digest: str) -> str | None:
        """LLM-enriched analysis. Never raises."""
        if not self.settings.analysis_enabled:
            return None

        try:
            from tele_quant.analysis.extractor import extract_candidates
            from tele_quant.analysis.fundamental import compute_fundamental
            from tele_quant.analysis.market_data import fetch_ohlcv_batch
            from tele_quant.analysis.report import format_analysis_report
            from tele_quant.analysis.scoring import build_scenario, compute_scorecard
            from tele_quant.analysis.technical import compute_technical

            log.info("[analysis] extracting candidates from %d items", len(items))
            candidates = await extract_candidates(self.ollama, items, digest, self.settings)
            if not candidates:
                log.info("[analysis] no candidates found")
                return None

            watchlist_cfg = _load_watchlist(self.settings)
            providers = _load_providers(self.settings)

            top = candidates[: self.settings.analysis_top_candidates]
            symbols = [c.symbol for c in top]
            log.info("[analysis] %d candidates → fetching market data for %s", len(top), symbols)

            ohlcv = await asyncio.to_thread(fetch_ohlcv_batch, symbols, self.settings)

            scenarios = []
            for candidate in top:
                df = ohlcv.get(candidate.symbol)
                technical = compute_technical(candidate.symbol, df)
                fundamental = await asyncio.to_thread(compute_fundamental, candidate.symbol)
                card = compute_scorecard(candidate, technical, fundamental)
                log.info(
                    "[analysis] %s (mentions=%d) → score=%.0f %s",
                    candidate.symbol,
                    candidate.mentions,
                    card.final_score,
                    card.grade,
                )
                if card.final_score >= self.settings.analysis_min_score_to_send:
                    # watchlist 정보
                    is_wl = False
                    wl_grp = ""
                    is_avoid = False
                    if watchlist_cfg is not None:
                        from tele_quant.watchlist import (
                            group_for_symbol,
                            is_avoid_symbol,
                            is_watchlist_symbol,
                        )

                        is_wl = is_watchlist_symbol(candidate.symbol, watchlist_cfg)
                        is_avoid = is_avoid_symbol(candidate.symbol, watchlist_cfg)
                        wl_grp_key = group_for_symbol(candidate.symbol, watchlist_cfg) or ""
                        if wl_grp_key and wl_grp_key in watchlist_cfg.groups:
                            wl_grp = watchlist_cfg.groups[wl_grp_key].label

                    # VerifySummary
                    verify_line = ""
                    try:
                        from tele_quant.providers.market_verify import build_verify_summary

                        vsummary = build_verify_summary(candidate.symbol, providers)
                        verify_line = vsummary.to_report_line()
                    except Exception:
                        pass

                    scenario = build_scenario(
                        candidate,
                        technical,
                        fundamental,
                        card.final_score,
                        card.grade,
                        scorecard=card,
                        is_watchlist=is_wl,
                        watchlist_group=wl_grp,
                        is_avoid=is_avoid,
                    )
                    scenario.verify_summary = verify_line
                    scenarios.append(scenario)

            if not scenarios:
                log.info(
                    "[analysis] no candidates above min score %.0f",
                    self.settings.analysis_min_score_to_send,
                )
                return None

            return format_analysis_report(
                scenarios,
                compact=self.settings.report_compact_scenarios,
                compact_max_longs=self.settings.report_compact_max_longs,
                compact_max_shorts=self.settings.report_compact_max_shorts,
                compact_max_watch=self.settings.report_compact_max_watch,
                compact_max_reasons=self.settings.report_compact_max_reasons,
            )

        except Exception as exc:
            log.exception("[analysis] pipeline failed: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Fast / no_llm mode: Python-only digest + deterministic analysis
    # ------------------------------------------------------------------

    async def _summarize_fast(
        self,
        kept: list[RawItem],
        market_snapshot: list[Any],
        stats: RunStats,
        hours: float,
        start_time: float,
        watchlist_cfg: Any = None,
        macro_only: bool = False,
        relation_feed: Any = None,
        prev_sector_sentiments: dict[str, dict] | None = None,
        external_data: dict[str, Any] | None = None,
    ) -> tuple[str, Any, Any]:
        """Build deterministic digest, optionally polish with Ollama."""
        from tele_quant.deterministic_report import apply_polish_guard, build_macro_digest
        from tele_quant.evidence import build_evidence_clusters
        from tele_quant.evidence_ranker import rank_evidence_clusters

        clusters = await asyncio.to_thread(build_evidence_clusters, kept, self.settings)
        log.info("[pipeline] evidence clusters built: %d", len(clusters))

        ranked = rank_evidence_clusters(clusters, self.settings)

        # ① 4H LLM 전처리 패스: smart_read — 시간 분산 샘플링 + 구조화 JSON
        smart_result: Any = None
        market_narrative = ""
        if self.settings.digest_mode == "fast" and kept:
            elapsed = time.monotonic() - start_time
            narrative_timeout = getattr(
                self.settings, "ollama_narrative_timeout_seconds", 300.0
            )
            if self.settings.run_max_seconds - elapsed > narrative_timeout + 30:
                try:
                    smart_result = await asyncio.wait_for(
                        self.ollama.smart_read(kept, hours),
                        timeout=narrative_timeout,
                    )
                    if smart_result is not None and not getattr(smart_result, "is_empty", True):
                        market_narrative = smart_result.as_narrative_text()
                        log.info(
                            "[digest] smart_read ok: %d chars, bullish=%d bearish=%d",
                            len(market_narrative),
                            len(getattr(smart_result, "bullish_items", [])),
                            len(getattr(smart_result, "bearish_items", [])),
                        )
                        # ② smart_read 결과로 증거 클러스터 재가중 — AI 독해가 호재로 본
                        #    종목의 클러스터를 positive_stock 버킷 앞으로 이동
                        ranked = _apply_smart_read_boost(ranked, smart_result)
                except TimeoutError:
                    log.warning("[digest] smart_read timeout → skipped")
                except Exception as exc:
                    log.warning("[digest] smart_read failed: %s", type(exc).__name__)

        digest = build_macro_digest(
            ranked,
            market_snapshot,
            stats,
            hours,
            watchlist_cfg=watchlist_cfg,
            macro_only=macro_only,
            relation_feed=relation_feed,
            prev_sector_sentiments=prev_sector_sentiments,
            market_narrative=market_narrative,
            external_data=external_data,
            settings=self.settings,
        )
        log.info("[digest] mode=%s deterministic=ok", self.settings.digest_mode)

        if self.settings.digest_mode == "fast":
            elapsed = time.monotonic() - start_time
            remaining = self.settings.run_max_seconds - elapsed
            call_max = min(self.settings.ollama_call_max_seconds, remaining - 30)
            if call_max > 10:
                try:
                    polished = await asyncio.wait_for(
                        self.ollama.polish_digest(digest),
                        timeout=call_max,
                    )
                    digest = apply_polish_guard(digest, polished)
                    log.info("[digest] polish=ok (guard applied)")
                except TimeoutError:
                    log.warning("[digest] polish timeout → deterministic kept")
                except Exception as exc:
                    log.warning("[digest] polish failed: %s → deterministic kept", exc)

        return digest, ranked, smart_result

    def _load_price_store(self) -> Any:
        """Load PriceHistoryStore from CSV. Returns None if disabled or file missing."""
        if not self.settings.local_data_enabled:
            return None
        try:
            from tele_quant.local_data import load_price_history

            return load_price_history(self.settings)
        except Exception as exc:
            log.warning("[pipeline] price_store load failed: %s", type(exc).__name__)
            return None

    def _load_corr_store(self) -> Any:
        """Load CorrelationStore from CSV. Returns None if disabled or file missing."""
        if not self.settings.local_data_enabled or not self.settings.correlation_expansion_enabled:
            return None
        try:
            from tele_quant.local_data import load_correlation

            return load_correlation(self.settings)
        except Exception as exc:
            log.warning("[pipeline] corr_store load failed: %s", type(exc).__name__)
            return None

    def _run_pair_watch(
        self,
        relation_feed: Any = None,
        macro_only: bool = False,
    ) -> tuple[str, list[Any]]:
        """Run live pair watch engine. Returns (section_text, signals). Never raises."""
        if not getattr(self.settings, "live_pair_watch_enabled", True):
            return "", []
        try:
            from tele_quant.live_pair_watch import build_pair_watch_section, run_pair_watch

            corr_store = self._load_corr_store()
            signals, used_stale, diagnostics = run_pair_watch(
                self.settings,
                relation_feed=relation_feed,
                corr_store=corr_store,
            )
            section = build_pair_watch_section(
                signals,
                settings=self.settings,
                used_stale_cache=used_stale,
                diagnostics=diagnostics or [],
            )
            log.info("[pair_watch] signals=%d section_len=%d", len(signals), len(section))
            return section, signals
        except Exception as exc:
            log.warning("[pair_watch] run failed: %s", exc)
            return "", []

    def _load_research_pairs(self) -> list[Any]:
        """Load GPTPRO research lead-lag pairs. Returns empty list if disabled/missing."""
        if not getattr(self.settings, "research_db_enabled", True):
            return []
        try:
            from tele_quant.research_db import load_research_pairs

            return load_research_pairs(self.settings)
        except Exception as exc:
            log.warning("[pipeline] research_pairs load failed: %s", type(exc).__name__)
            return []

    def _load_relation_feed(self) -> Any:
        """Load relation feed from shared directory. Never raises."""
        try:
            from tele_quant.relation_feed import load_relation_feed

            feed = load_relation_feed(self.settings)
            if (
                feed.available
                and not feed.leadlag
                and feed.movers
                and getattr(self.settings, "relation_fallback_enabled", True)
            ):
                try:
                    from tele_quant.local_data import load_correlation, load_price_history
                    from tele_quant.relation_fallback import compute_fallback_leadlag

                    price_store = load_price_history(self.settings)
                    corr_store = load_correlation(self.settings)
                    feed.fallback_candidates = compute_fallback_leadlag(
                        feed, self.settings, price_store, corr_store
                    )
                    if feed.fallback_candidates:
                        log.info(
                            "[pipeline] fallback leadlag: %d candidates",
                            len(feed.fallback_candidates),
                        )
                except Exception as _fb_exc:
                    log.warning("[pipeline] fallback leadlag failed: %s", type(_fb_exc).__name__)
            return feed
        except Exception as exc:
            log.warning("[pipeline] relation_feed load failed: %s", type(exc).__name__)
            return None

    async def _run_analysis_fast(
        self,
        items: list[RawItem],
        ranked: Any,
        start_time: float,
        watchlist_cfg: Any = None,
        relation_feed: Any = None,
    ) -> tuple[str | None, list[Any], dict[str, float], dict[str, str], list[dict]]:
        """Python-only analysis without LLM extraction. Returns (report, scenarios, close_map, sector_map)."""
        if not self.settings.analysis_enabled:
            return None, [], {}, {}, []

        try:
            from tele_quant.analysis.extractor import fast_extract_candidates
            from tele_quant.analysis.fundamental import compute_fundamental
            from tele_quant.analysis.market_data import fetch_ohlcv_batch
            from tele_quant.analysis.scoring import build_scenario, compute_scorecard
            from tele_quant.analysis.technical import compute_technical
            from tele_quant.candidate_expansion import expand_candidates
            from tele_quant.deterministic_report import apply_polish_guard, build_long_short_report
            from tele_quant.sector_quota import apply_sector_quota

            base_candidates = fast_extract_candidates(items, self.settings)
            if not base_candidates:
                log.info("[analysis-fast] no candidates found")
                return None, [], {}, {}, []

            # Build evidence clusters for expansion context
            clusters: list[Any] = []
            try:
                from tele_quant.evidence import build_evidence_clusters

                clusters = await asyncio.to_thread(build_evidence_clusters, items, self.settings)
            except Exception:
                pass

            corr_store = self._load_corr_store()
            research_pairs = self._load_research_pairs()

            # Expand candidates (correlation peers, watchlist sector matches, research DB)
            try:
                expanded = expand_candidates(
                    base_candidates,
                    clusters,
                    self.settings,
                    watchlist_cfg,
                    corr_store,
                    research_pairs if research_pairs else None,
                )
                log.info(
                    "[analysis-fast] expanded: %d → %d candidates",
                    len(base_candidates),
                    len(expanded),
                )
            except Exception as exc:
                log.warning("[analysis-fast] expand failed: %s", type(exc).__name__)
                expanded = [
                    c.to_stock_candidate() if hasattr(c, "to_stock_candidate") else c
                    for c in base_candidates
                ]  # type: ignore[union-attr]

            _sector_map: dict[str, str] = {
                getattr(c, "symbol", ""): getattr(c, "sector", "") or "" for c in expanded
            }

            # Apply sector quota
            try:
                expanded = apply_sector_quota(expanded, self.settings)
            except Exception as exc:
                log.warning("[analysis-fast] sector_quota failed: %s", type(exc).__name__)

            providers = _load_providers(self.settings)
            max_tickers = self.settings.analysis_max_tickers
            top = expanded[:max_tickers]
            symbols = [c.symbol for c in top]
            log.info("[analysis-fast] %d candidates → %s", len(top), symbols[:10])

            price_store = self._load_price_store()
            ohlcv = await asyncio.to_thread(fetch_ohlcv_batch, symbols, self.settings, price_store)

            # ③ narrative_history 기반 반복 등장 종목 boost map 구성 (최근 24H)
            _narrative_boost_map: dict[str, int] = {}
            try:
                _nar_rows = self.store.recent_narratives(
                    since=utc_now() - timedelta(hours=24), limit=20
                )
                for _nr in _nar_rows:
                    for _b in (_nr.get("bullish_json") or []):
                        _name = (_b.get("name") or "").strip().lower() if isinstance(_b, dict) else ""
                        if _name:
                            _narrative_boost_map[_name] = _narrative_boost_map.get(_name, 0) + 1
            except Exception:
                pass

            scenarios = []
            close_map: dict[str, float] = {}
            _tech_scan_rows: list[dict] = []  # 점수 미달 포함 전체 기술 스캔용
            for candidate in top:
                df = ohlcv.get(candidate.symbol)
                technical = compute_technical(candidate.symbol, df)
                if technical and technical.close is not None:
                    close_map[candidate.symbol] = technical.close
                fundamental = await asyncio.to_thread(compute_fundamental, candidate.symbol)

                # 4H 기술지표 — scoring 전에 fetch해서 new formula에 투입
                snap_4h: Any = None
                if self.settings.intraday_tech_enabled:
                    try:
                        from tele_quant.analysis.intraday import fetch_intraday_4h

                        snap_4h = await asyncio.to_thread(
                            fetch_intraday_4h, candidate.symbol, self.settings
                        )
                    except Exception:
                        pass

                # narrative_boost: 후보 이름/심볼이 최근 AI 독해에서 반복 호재 언급된 횟수
                _cand_name = (getattr(candidate, "name", "") or "").strip().lower()
                _cand_sym = (getattr(candidate, "symbol", "") or "").strip().lower()
                _nb = _narrative_boost_map.get(_cand_name, 0) or _narrative_boost_map.get(_cand_sym, 0)

                card = compute_scorecard(
                    candidate, technical, fundamental, technical_4h=snap_4h, narrative_boost=_nb
                )

                # Relation feed 보조 가점: telegram + feed + technical 모두 있을 때만
                rf_note = ""
                if relation_feed is not None:
                    try:
                        from tele_quant.relation_feed import get_relation_boost

                        has_tg = len(getattr(candidate, "catalysts", [])) > 0
                        tech_ok = card.technical_score > 10
                        boost, rf_note = get_relation_boost(
                            relation_feed, candidate.symbol, has_tg, tech_ok
                        )
                        if boost > 0:
                            card.final_score = min(100.0, card.final_score + boost)
                            log.info(
                                "[analysis-fast] relation_feed boost %s +%.1f → %.0f",
                                candidate.symbol,
                                boost,
                                card.final_score,
                            )
                    except Exception as _rf_exc:
                        log.debug("[analysis-fast] relation boost failed: %s", _rf_exc)

                log.info(
                    "[analysis-fast] %s score=%.0f %s",
                    candidate.symbol,
                    card.final_score,
                    card.grade,
                )

                # 항상 tech scan 데이터 수집 (점수 미달 후보 포함)
                _tech_scan_rows.append({
                    "symbol": candidate.symbol,
                    "name": getattr(candidate, "name", None) or candidate.symbol,
                    "sentiment": getattr(candidate, "sentiment", "neutral"),
                    "score": card.final_score,
                    "catalysts": list(getattr(candidate, "catalysts", []))[:3],
                    "risks": list(getattr(candidate, "risks", []))[:2],
                    "snap_4h": snap_4h,
                    "technical": technical,
                    "sentiment_alpha": getattr(candidate, "sentiment_alpha_score", None),
                    "direct_ev": getattr(candidate, "direct_evidence_count", 0),
                })

                if card.final_score >= self.settings.analysis_min_score_to_send:
                    # watchlist 정보
                    is_wl = False
                    wl_grp = ""
                    is_avoid = False
                    if watchlist_cfg is not None:
                        from tele_quant.watchlist import (
                            group_for_symbol,
                            is_avoid_symbol,
                            is_watchlist_symbol,
                        )

                        is_wl = is_watchlist_symbol(candidate.symbol, watchlist_cfg)
                        is_avoid = is_avoid_symbol(candidate.symbol, watchlist_cfg)
                        wl_grp_key = group_for_symbol(candidate.symbol, watchlist_cfg) or ""
                        if wl_grp_key and wl_grp_key in watchlist_cfg.groups:
                            wl_grp = watchlist_cfg.groups[wl_grp_key].label

                    # VerifySummary
                    verify_line = ""
                    try:
                        from tele_quant.providers.market_verify import build_verify_summary

                        vsummary = build_verify_summary(candidate.symbol, providers)
                        verify_line = vsummary.to_report_line()
                    except Exception:
                        pass

                    scenario = build_scenario(
                        candidate,
                        technical,
                        fundamental,
                        card.final_score,
                        card.grade,
                        scorecard=card,
                        is_watchlist=is_wl,
                        watchlist_group=wl_grp,
                        is_avoid=is_avoid,
                    )
                    scenario.verify_summary = verify_line
                    if rf_note:
                        scenario.relation_feed_note = rf_note

                    # Evidence 품질 정보 저장
                    scenario.direct_evidence_count = getattr(candidate, "direct_evidence_count", 0)
                    catalysts = getattr(candidate, "catalysts", [])
                    if catalysts:
                        scenario.evidence_summary = catalysts[0][:100]

                    # 3D 기술지표 (daily snapshot에서)
                    if technical and technical.close is not None:
                        scenario.rsi_3d = technical.rsi14
                        scenario.obv_3d = technical.obv_trend
                        scenario.bollinger_3d = technical.bb_position

                    # 4H intraday technical (이미 fetch된 snap_4h 재사용)
                    if snap_4h is not None:
                        try:
                            from tele_quant.analysis.intraday import format_4h_section

                            scenario.intraday_4h_summary = format_4h_section(snap_4h)
                            scenario.rsi_4h = snap_4h.rsi14
                            scenario.obv_4h = snap_4h.obv_trend
                            scenario.bollinger_4h = snap_4h.bb_position
                            scenario.bb_upper_4h = snap_4h.bb_upper
                            scenario.bb_middle_4h = snap_4h.bb_middle
                            scenario.bb_lower_4h = snap_4h.bb_lower
                        except Exception:
                            pass

                    scenarios.append(scenario)

            if not scenarios:
                log.info(
                    "[analysis-fast] no candidates above min score %.0f",
                    self.settings.analysis_min_score_to_send,
                )
                # tech scan rows 반환 (점수 미달이어도 기술 스캔 섹션에 활용)
                _tech_scan_rows.sort(key=lambda r: -r["score"])
                return None, [], close_map, _sector_map, _tech_scan_rows

            # Apply report limits
            longs = [s for s in scenarios if s.side == "LONG"][: self.settings.report_max_longs]
            shorts = [s for s in scenarios if s.side == "SHORT"][: self.settings.report_max_shorts]
            watches = [s for s in scenarios if s.side == "WATCH"][: self.settings.report_max_watch]
            limited_scenarios = longs + shorts + watches

            # ③ 종목별 "왜?" 설명 — 롱 상위 3개에 Ollama 서술 생성 (병렬)
            if self.settings.digest_mode == "fast":
                elapsed = time.monotonic() - start_time
                if self.settings.run_max_seconds - elapsed >= 60:
                    _ps_timeout = getattr(
                        self.settings, "ollama_stock_summary_timeout_seconds", 90.0
                    )

                    async def _gen_plain(sc: Any) -> None:
                        try:
                            sc.plain_summary = await asyncio.wait_for(
                                self.ollama.generate_stock_plain_summary(sc),
                                timeout=_ps_timeout,
                            )
                            log.info(
                                "[analysis-fast] plain_summary %s (%d chars)",
                                sc.symbol,
                                len(sc.plain_summary),
                            )
                        except TimeoutError:
                            log.warning(
                                "[analysis-fast] plain_summary timeout: %s", sc.symbol
                            )
                        except Exception as _ps_exc:
                            log.debug(
                                "[analysis-fast] plain_summary failed: %s", _ps_exc
                            )

                    await asyncio.gather(*[_gen_plain(sc) for sc in longs[:3]])

                    # SHORT 상위 2개: 왜 약세인지 설명 (side="short")
                    async def _gen_plain_short(sc: Any) -> None:
                        try:
                            elapsed2 = time.monotonic() - start_time
                            if self.settings.run_max_seconds - elapsed2 < 60:
                                return
                            sc.plain_summary = await asyncio.wait_for(
                                self.ollama.generate_stock_plain_summary(sc, side="short"),
                                timeout=_ps_timeout,
                            )
                            log.info(
                                "[analysis-fast] plain_summary SHORT %s (%d chars)",
                                sc.symbol,
                                len(sc.plain_summary),
                            )
                        except TimeoutError:
                            log.warning("[analysis-fast] plain_summary SHORT timeout: %s", sc.symbol)
                        except Exception as _ps_exc2:
                            log.debug("[analysis-fast] plain_summary SHORT failed: %s", _ps_exc2)

                    await asyncio.gather(*[_gen_plain_short(sc) for sc in shorts[:2]])

            # ④ Google Trends — 롱 상위 종목 검색 관심도 (선택)
            _trends_data: dict[str, float] = {}
            if getattr(self.settings, "google_trends_enabled", True) and longs:
                try:
                    from tele_quant.external_indicators import fetch_google_trends

                    _kw_limit = getattr(self.settings, "google_trends_max_keywords", 5)
                    _gt_timeout = getattr(self.settings, "google_trends_timeout_seconds", 25.0)
                    _trend_kws = [
                        sc.name or sc.symbol for sc in longs[:_kw_limit] if sc.name or sc.symbol
                    ]
                    elapsed = time.monotonic() - start_time
                    if self.settings.run_max_seconds - elapsed > _gt_timeout + 10:
                        _trends_data = await asyncio.wait_for(
                            asyncio.to_thread(
                                fetch_google_trends, _trend_kws, "now 7-d", "", _gt_timeout
                            ),
                            timeout=_gt_timeout + 5,
                        ) or {}
                        if _trends_data:
                            log.info(
                                "[analysis-fast] google_trends: %d symbols",
                                len(_trends_data),
                            )
                            # 관심도 높은 종목의 plain_summary에 추가 context
                            for sc in longs[:_kw_limit]:
                                kw = sc.name or sc.symbol
                                t_val = _trends_data.get(kw)
                                if t_val is not None and t_val >= 50:
                                    sc.plain_summary = (
                                        (sc.plain_summary + "\n" if sc.plain_summary else "")
                                        + f"🔍 Google 검색 관심도 {t_val:.0f}/100 (최근 7일)"
                                    )
                except (TimeoutError, Exception) as _gt_exc:
                    log.debug("[analysis-fast] google_trends failed: %s", _gt_exc)

            report = build_long_short_report(
                limited_scenarios,
                ranked,
                {},
                compact=self.settings.report_compact_scenarios,
                compact_max_longs=self.settings.report_compact_max_longs,
                compact_max_shorts=self.settings.report_compact_max_shorts,
                compact_max_watch=self.settings.report_compact_max_watch,
                compact_max_reasons=self.settings.report_compact_max_reasons,
            )
            log.info(
                "[analysis-fast] long=%d short=%d watch=%d",
                len(longs),
                len(shorts),
                len(watches),
            )

            # Coverage summary append
            if self.settings.report_show_coverage:
                try:
                    from tele_quant.candidate_expansion import build_coverage_summary

                    coverage = build_coverage_summary(
                        expanded, limited_scenarios, relation_feed=relation_feed
                    )
                    if coverage:
                        report = report + "\n\n" + coverage
                except Exception:
                    pass

            # Optional polish with guard
            if self.settings.digest_mode == "fast" and report:
                elapsed = time.monotonic() - start_time
                remaining = self.settings.run_max_seconds - elapsed
                call_max = min(self.settings.ollama_call_max_seconds, remaining - 10)
                if call_max > 10:
                    try:
                        polished = await asyncio.wait_for(
                            self.ollama.polish_analysis(report),
                            timeout=call_max,
                        )
                        report = apply_polish_guard(report, polished)
                        log.info("[analysis-fast] polish=ok (guard applied)")
                    except TimeoutError:
                        log.warning("[analysis-fast] polish timeout → deterministic kept")
                    except Exception as exc:
                        log.warning("[analysis-fast] polish failed: %s → deterministic kept", exc)

            _tech_scan_rows.sort(key=lambda r: -r["score"])
            return report, limited_scenarios, close_map, _sector_map, _tech_scan_rows

        except Exception as exc:
            log.exception("[analysis-fast] pipeline failed: %s", exc)
            return None, [], {}, {}, []

    # ------------------------------------------------------------------
    # Public run methods
    # ------------------------------------------------------------------

    async def run_once(
        self,
        send: bool = True,
        hours: float | None = None,
        macro_only: bool = False,
    ) -> tuple[str, str | None]:
        """Run one full cycle. Returns (digest, analysis_report | None)."""
        start_time = time.monotonic()
        lookback = hours if hours is not None else self.settings.fetch_lookback_hours
        issues = self.settings.validate_minimum()
        if issues:
            raise ValueError("\n".join(issues))

        digest_mode = self.settings.digest_mode
        # Honour explicit macro_only flag or settings-level weekend flag (KST 기준)
        if not macro_only and getattr(self.settings, "weekend_macro_only", False):
            try:
                from zoneinfo import ZoneInfo

                _kst = ZoneInfo("Asia/Seoul")
                _now_kst = time.time()
                import datetime as _dt

                _now_kst_dt = _dt.datetime.now(_kst)
                _wd = _now_kst_dt.weekday()  # 5=Sat, 6=Sun
                _hr = _now_kst_dt.hour
                _mn = _now_kst_dt.minute
                # Sat 07:00 ~ Sun 22:59 → macro_only
                if (_wd == 5 and (_hr > 7 or (_hr == 7 and _mn >= 0))) or (_wd == 6 and _hr < 23):
                    macro_only = True
            except Exception:
                pass
        watchlist_cfg = _load_watchlist(self.settings)
        relation_feed = self._load_relation_feed()
        pair_watch_section = ""
        pair_watch_signals: list[Any] = []

        saved_scenarios: list[Any] = []
        saved_close_map: dict[str, float] = {}
        saved_sector_map: dict[str, str] = {}
        analysis: str | None = None
        ranked: Any = None
        _smart_result: Any = None

        async with TelegramGateway(self.settings) as gateway:
            kept, stats = await self._collect_and_dedupe(
                gateway, lookback, watchlist_cfg=watchlist_cfg
            )

            market_snapshot = await asyncio.to_thread(fetch_market_snapshot, self.settings)

            # SEC EDGAR 8-K 공시: watchlist 미국 주식 최신 공시를 직접증거로 추가
            if not macro_only and getattr(self.settings, "sec_enabled", True):
                try:
                    from tele_quant.sec_client import fetch_sec_8k_for_watchlist

                    _sec_items = await asyncio.to_thread(
                        fetch_sec_8k_for_watchlist, self.settings, watchlist_cfg
                    )
                    if _sec_items:
                        _inserted_sec = self.store.insert_items(_sec_items)
                        kept = kept + _inserted_sec
                        log.info("[pipeline] sec 8-K inserted: %d items", len(_inserted_sec))
                except Exception as _sec_exc:
                    log.debug("[pipeline] sec fetch failed: %s", _sec_exc)

            # OpenDART 한국 공시
            if not macro_only and getattr(self.settings, "opendart_enabled", True):
                try:
                    from tele_quant.opendart_client import fetch_dart_for_watchlist

                    _dart_items = await asyncio.to_thread(
                        fetch_dart_for_watchlist, self.settings, watchlist_cfg
                    )
                    if _dart_items:
                        _inserted_dart = self.store.insert_items(_dart_items)
                        kept = kept + _inserted_dart
                        log.info("[pipeline] opendart inserted: %d items", len(_inserted_dart))
                except Exception as _dart_exc:
                    log.debug("[pipeline] opendart fetch failed: %s", _dart_exc)

            # Finnhub 미국 주식 뉴스
            if not macro_only and getattr(self.settings, "finnhub_enabled", True):
                try:
                    from tele_quant.finnhub_client import fetch_finnhub_for_watchlist

                    _fh_items = await asyncio.to_thread(
                        fetch_finnhub_for_watchlist, self.settings, watchlist_cfg
                    )
                    if _fh_items:
                        _inserted_fh = self.store.insert_items(_fh_items)
                        kept = kept + _inserted_fh
                        log.info("[pipeline] finnhub inserted: %d items", len(_inserted_fh))
                except Exception as _fh_exc:
                    log.debug("[pipeline] finnhub fetch failed: %s", _fh_exc)

            # 외부 지표 병렬 fetch (Fear&Greed + FRED + EIA + ECOS + ECB + Frankfurter)
            _external_data: dict[str, Any] = {}
            try:
                from tele_quant.external_indicators import (
                    fetch_ecb_deposit_rate,
                    fetch_eia_energy,
                    fetch_exchange_rates,
                    fetch_fear_greed,
                    fetch_fred_series,
                )

                _fg_enabled = getattr(self.settings, "fear_greed_enabled", True)
                _fred_enabled = getattr(self.settings, "fred_enabled", True)
                _fred_key = getattr(self.settings, "fred_api_key", "")
                _fred_ids = [
                    s.strip()
                    for s in getattr(self.settings, "fred_series", "").split(",")
                    if s.strip()
                ]
                _fg_timeout = getattr(self.settings, "fear_greed_timeout_seconds", 10.0)
                _fred_timeout = getattr(self.settings, "fred_timeout_seconds", 12.0)
                _eia_key = getattr(self.settings, "eia_api_key", "")
                _eia_timeout = getattr(self.settings, "eia_timeout_seconds", 10.0)
                _ecb_timeout = getattr(self.settings, "ecb_timeout_seconds", 10.0)
                _fr_timeout = getattr(self.settings, "frankfurter_timeout_seconds", 8.0)

                _fetch_coros: list[Any] = []
                _coro_keys: list[str] = []

                if _fg_enabled:
                    _fetch_coros.append(asyncio.to_thread(fetch_fear_greed, _fg_timeout))
                    _coro_keys.append("fear_greed")
                if _fred_enabled and _fred_key and _fred_ids:
                    _fetch_coros.append(
                        asyncio.to_thread(fetch_fred_series, _fred_key, _fred_ids, _fred_timeout)
                    )
                    _coro_keys.append("fred")
                if getattr(self.settings, "eia_enabled", True) and _eia_key:
                    _fetch_coros.append(
                        asyncio.to_thread(fetch_eia_energy, _eia_key, _eia_timeout)
                    )
                    _coro_keys.append("energy")
                if getattr(self.settings, "ecb_enabled", True):
                    _fetch_coros.append(asyncio.to_thread(fetch_ecb_deposit_rate, _ecb_timeout))
                    _coro_keys.append("ecb_rate")
                if getattr(self.settings, "frankfurter_enabled", True):
                    _fetch_coros.append(asyncio.to_thread(fetch_exchange_rates, "USD", "KRW,EUR,JPY,CNY,GBP", _fr_timeout))
                    _coro_keys.append("exchange_rates")

                if _fetch_coros:
                    _ind_results = await asyncio.gather(*_fetch_coros, return_exceptions=True)
                    for _ck, _rv in zip(_coro_keys, _ind_results, strict=False):
                        if isinstance(_rv, Exception):
                            log.debug("[pipeline] %s fetch failed: %s", _ck, _rv)
                        elif _rv is not None and _rv != {}:
                            _external_data[_ck] = _rv

                # ECOS 한국은행 (직렬 — API 키 있을 때만)
                _ecos_key = getattr(self.settings, "ecos_api_key", "")
                if getattr(self.settings, "ecos_enabled", True) and _ecos_key:
                    try:
                        from tele_quant.ecos_client import fetch_ecos_series

                        _ecos_ids = [
                            s.strip()
                            for s in getattr(self.settings, "ecos_series", "").split(",")
                            if s.strip()
                        ]
                        _ecos_series = [
                            (sid, "D" if sid in ("722Y001", "731Y003") else "M", sid, "")
                            for sid in _ecos_ids
                        ]
                        _ecos_result = await asyncio.to_thread(
                            fetch_ecos_series,
                            _ecos_key,
                            _ecos_series,
                            getattr(self.settings, "ecos_timeout_seconds", 12.0),
                        )
                        if _ecos_result:
                            _external_data["ecos"] = _ecos_result
                    except Exception as _ecos_exc:
                        log.debug("[pipeline] ecos fetch failed: %s", _ecos_exc)

                # ① yfinance → FRED 대체: API 키 없어도 ^TNX/DXY/VIX 표시
                try:
                    from tele_quant.external_indicators import (
                        extract_yfinance_macro,
                        merge_macro_data,
                    )

                    yf_macro = extract_yfinance_macro(market_snapshot)
                    existing_fred = _external_data.get("fred") or {}
                    merged = merge_macro_data(existing_fred, yf_macro)
                    if merged:
                        _external_data["fred"] = merged
                except Exception as _yf_macro_exc:
                    log.debug("[pipeline] yfinance macro extract failed: %s", _yf_macro_exc)

                if _external_data:
                    log.info(
                        "[pipeline] external indicators: %s",
                        list(_external_data.keys()),
                    )
            except Exception as _ext_exc:
                log.debug("[pipeline] external indicators failed: %s", _ext_exc)

            if digest_mode == "deep":
                digest = await self._summarize_with_evidence(kept, market_snapshot, stats, lookback)
                if not macro_only:
                    analysis = await self._run_analysis(kept, digest)
            else:
                # fast or no_llm
                _prev_sent: dict[str, dict] | None = None
                try:
                    _prev_rows = self.store.recent_sentiment_history(
                        since=utc_now() - timedelta(hours=8), limit=50
                    )
                    if _prev_rows:
                        _by_sector: dict[str, list[float]] = {}
                        for _row in _prev_rows:
                            _sec = _row.get("sector") or ""
                            _sc_val = float(_row.get("sentiment_score") or 50.0)
                            if _sec:
                                _by_sector.setdefault(_sec, []).append(_sc_val)
                        _prev_sent = {
                            sec: {"score": sum(vals) / len(vals), "bullish": 0, "bearish": 0}
                            for sec, vals in _by_sector.items()
                        }
                except Exception:
                    pass

                digest, ranked, _smart_result = await self._summarize_fast(
                    kept,
                    market_snapshot,
                    stats,
                    lookback,
                    start_time,
                    watchlist_cfg=watchlist_cfg,
                    macro_only=macro_only,
                    relation_feed=relation_feed,
                    prev_sector_sentiments=_prev_sent,
                    external_data=_external_data or None,
                )
                if not macro_only:
                    (
                        analysis,
                        saved_scenarios,
                        saved_close_map,
                        saved_sector_map,
                        _tech_scan_rows,
                    ) = await self._run_analysis_fast(
                        kept,
                        ranked,
                        start_time,
                        watchlist_cfg=watchlist_cfg,
                        relation_feed=relation_feed,
                    )
                    # 중복/잉여 문구 제거
                    if analysis:
                        from tele_quant.trade_phrase_cleaner import clean_report

                        analysis = clean_report(analysis)

                    # 시나리오 없을 때 → 기술 스캔 섹션을 digest에 주입
                    if not analysis and _tech_scan_rows:
                        try:
                            from tele_quant.deterministic_report import build_tech_scan_section

                            _ts_section = build_tech_scan_section(_tech_scan_rows)
                            if _ts_section:
                                digest = digest + "\n\n" + _ts_section
                        except Exception as _ts_exc:
                            log.debug("[pipeline] tech_scan section failed: %s", _ts_exc)

            # Live pair watch — run regardless of macro_only, append to digest
            pair_watch_section, pair_watch_signals = await asyncio.to_thread(
                self._run_pair_watch, relation_feed, macro_only
            )
            if pair_watch_section:
                digest = digest + "\n\n" + pair_watch_section

            elapsed = time.monotonic() - start_time
            log.info(
                "[run] raw=%d quality_dropped=- clusters=- selected=- elapsed=%.0fs mode=%s macro_only=%s",
                stats.telegram_items + stats.report_items,
                elapsed,
                digest_mode,
                macro_only,
            )

            if send:
                sender = TelegramSender(self.settings, gateway=gateway)
                await sender.send(digest)
                if analysis:
                    await sender.send(analysis)
                stats.sent = True

            # Clean digest/analysis before DB storage so lint-report and weekly see clean text
            digest = apply_final_report_cleaner(digest)
            if analysis:
                analysis = apply_final_report_cleaner(analysis)

            self.store.save_digest(digest, period_hours=lookback, stats=stats.as_dict())
            report_id = self.store.save_run_report(
                digest, analysis, lookback, digest_mode, stats.as_dict()
            )
            if saved_scenarios:
                self.store.save_scenarios(
                    report_id,
                    saved_scenarios,
                    mode=digest_mode,
                    close_map=saved_close_map,
                    sector_map=saved_sector_map,
                    sent=send,
                )
            if relation_feed is not None:
                try:
                    saved_chains = self.store.save_mover_chain(relation_feed, report_id=report_id)
                    if saved_chains:
                        log.info("[pipeline] mover_chain saved: %d rows", saved_chains)
                except Exception as _mc_exc:
                    log.debug("[pipeline] mover_chain save failed: %s", _mc_exc)
            if pair_watch_signals:
                try:
                    saved_pw = self.store.save_pair_watch_signals(pair_watch_signals)
                    if saved_pw:
                        log.info("[pipeline] pair_watch saved: %d signals", saved_pw)
                except Exception as _pw_exc:
                    log.debug("[pipeline] pair_watch save failed: %s", _pw_exc)
            # Save Fear & Greed to history DB
            _fg_data = (_external_data or {}).get("fear_greed")
            if _fg_data:
                try:
                    self.store.save_fear_greed(_fg_data, report_id=report_id)
                    log.info("[pipeline] fear_greed_history saved: score=%.0f", _fg_data.get("score", 0))
                except Exception as _fg_exc:
                    log.debug("[pipeline] fear_greed save failed: %s", _fg_exc)
            # Save 4H AI narrative to DB for weekly reuse
            if _smart_result is not None and not getattr(_smart_result, "is_empty", True):
                try:
                    self.store.save_narrative(_smart_result, report_id=report_id, hours=lookback)
                    log.info("[pipeline] narrative_history saved")
                except Exception as _nr_exc:
                    log.debug("[pipeline] narrative_history save failed: %s", _nr_exc)
            # Save per-sector sentiment history for trend tracking
            if ranked is not None:
                try:
                    import json as _json

                    from tele_quant.deterministic_report import _compute_sector_sentiments

                    sector_sents = _compute_sector_sentiments(ranked)
                    for sector, data in sector_sents.items():
                        self.store.save_sentiment_history(
                            report_id=report_id,
                            sector=sector,
                            sentiment_score=data["score"],
                            bullish_count=data["bullish"],
                            bearish_count=data["bearish"],
                            novelty_count=data.get("novelty", 0),
                            top_events_json=_json.dumps(data.get("events", [])[:3], ensure_ascii=False),
                            source_count=data.get("sources", 0),
                            confidence=data.get("confidence", "medium"),
                        )
                    if sector_sents:
                        log.info("[pipeline] sentiment_history saved: %d sectors", len(sector_sents))
                except Exception as _sh_exc:
                    log.debug("[pipeline] sentiment_history save failed: %s", _sh_exc)
            return digest, analysis

    async def run_candidates(
        self,
        hours: float | None = None,
        use_llm: bool = False,
        expanded: bool = False,
    ) -> list[Any]:
        """Collect + dedupe + extract candidates only (no summarize, no send)."""
        lookback = hours if hours is not None else self.settings.fetch_lookback_hours
        issues = self.settings.validate_minimum()
        if issues:
            raise ValueError("\n".join(issues))

        async with TelegramGateway(self.settings) as gateway:
            kept, _ = await self._collect_and_dedupe(gateway, lookback)

        if use_llm:
            from tele_quant.analysis.extractor import extract_candidates

            base = await extract_candidates(self.ollama, kept, "", self.settings)
        else:
            from tele_quant.analysis.extractor import fast_extract_candidates

            base = fast_extract_candidates(kept, self.settings)

        if not expanded:
            return base

        # Expand with correlation + sector
        try:
            from tele_quant.candidate_expansion import expand_candidates
            from tele_quant.evidence import build_evidence_clusters
            from tele_quant.sector_quota import apply_sector_quota

            clusters = await asyncio.to_thread(build_evidence_clusters, kept, self.settings)
            watchlist_cfg = _load_watchlist(self.settings)
            corr_store = self._load_corr_store()
            research_pairs = self._load_research_pairs()
            expanded_list = expand_candidates(
                base,
                clusters,
                self.settings,
                watchlist_cfg,
                corr_store,
                research_pairs if research_pairs else None,
            )
            return apply_sector_quota(expanded_list, self.settings)
        except Exception as exc:
            log.warning("[run_candidates] expand failed: %s", type(exc).__name__)
            return base

    async def run_loop(self) -> None:
        interval = max(60, int(self.settings.digest_interval_hours * 3600))
        while True:
            try:
                log.info("[loop] starting run (lookback=%sh)", self.settings.fetch_lookback_hours)
                _digest, analysis = await self.run_once(send=True)
                log.info(
                    "[loop] done — digest sent, analysis: %s",
                    "sent" if analysis else "none",
                )
            except Exception as exc:
                log.exception("[loop] run failed: %s", exc)
            log.info("[loop] sleeping %ds until next run", interval)
            await asyncio.sleep(interval)
