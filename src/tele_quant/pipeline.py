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
    ) -> tuple[list[RawItem], RunStats]:
        stats = RunStats()

        telegram_items = await gateway.fetch_recent_messages(hours=lookback)
        stats.telegram_items = len(telegram_items)

        report_items = await fetch_naver_reports(self.settings, hours=max(lookback, 24))
        stats.report_items = len(report_items)

        all_items = telegram_items + report_items
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
    ) -> tuple[str, Any]:
        """Build deterministic digest, optionally polish with Ollama."""
        from tele_quant.deterministic_report import apply_polish_guard, build_macro_digest
        from tele_quant.evidence import build_evidence_clusters
        from tele_quant.evidence_ranker import rank_evidence_clusters

        clusters = await asyncio.to_thread(build_evidence_clusters, kept, self.settings)
        log.info("[pipeline] evidence clusters built: %d", len(clusters))

        ranked = rank_evidence_clusters(clusters, self.settings)
        digest = build_macro_digest(
            ranked,
            market_snapshot,
            stats,
            hours,
            watchlist_cfg=watchlist_cfg,
            macro_only=macro_only,
            relation_feed=relation_feed,
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

        return digest, ranked

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
    ) -> tuple[str | None, list[Any], dict[str, float], dict[str, str]]:
        """Python-only analysis without LLM extraction. Returns (report, scenarios, close_map, sector_map)."""
        if not self.settings.analysis_enabled:
            return None, [], {}, {}

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
                return None, [], {}, {}

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

            scenarios = []
            close_map: dict[str, float] = {}
            for candidate in top:
                df = ohlcv.get(candidate.symbol)
                technical = compute_technical(candidate.symbol, df)
                if technical and technical.close is not None:
                    close_map[candidate.symbol] = technical.close
                fundamental = await asyncio.to_thread(compute_fundamental, candidate.symbol)
                card = compute_scorecard(candidate, technical, fundamental)

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

                    # 4H intraday technical
                    if self.settings.intraday_tech_enabled:
                        try:
                            from tele_quant.analysis.intraday import (
                                fetch_intraday_4h,
                                format_4h_section,
                            )

                            snap = await asyncio.to_thread(
                                fetch_intraday_4h, candidate.symbol, self.settings
                            )
                            if snap is not None:
                                scenario.intraday_4h_summary = format_4h_section(snap)
                                scenario.rsi_4h = snap.rsi14
                                scenario.obv_4h = snap.obv_trend
                                scenario.bollinger_4h = snap.bb_position
                        except Exception:
                            pass

                    scenarios.append(scenario)

            if not scenarios:
                log.info(
                    "[analysis-fast] no candidates above min score %.0f",
                    self.settings.analysis_min_score_to_send,
                )
                return None, [], close_map, _sector_map

            # Apply report limits
            longs = [s for s in scenarios if s.side == "LONG"][: self.settings.report_max_longs]
            shorts = [s for s in scenarios if s.side == "SHORT"][: self.settings.report_max_shorts]
            watches = [s for s in scenarios if s.side == "WATCH"][: self.settings.report_max_watch]
            limited_scenarios = longs + shorts + watches

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

            return report, limited_scenarios, close_map, _sector_map

        except Exception as exc:
            log.exception("[analysis-fast] pipeline failed: %s", exc)
            return None, [], {}, {}

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

        async with TelegramGateway(self.settings) as gateway:
            kept, stats = await self._collect_and_dedupe(gateway, lookback)

            market_snapshot = await asyncio.to_thread(fetch_market_snapshot, self.settings)

            if digest_mode == "deep":
                digest = await self._summarize_with_evidence(kept, market_snapshot, stats, lookback)
                if not macro_only:
                    analysis = await self._run_analysis(kept, digest)
            else:
                # fast or no_llm
                digest, ranked = await self._summarize_fast(
                    kept,
                    market_snapshot,
                    stats,
                    lookback,
                    start_time,
                    watchlist_cfg=watchlist_cfg,
                    macro_only=macro_only,
                    relation_feed=relation_feed,
                )
                if not macro_only:
                    (
                        analysis,
                        saved_scenarios,
                        saved_close_map,
                        saved_sector_map,
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
