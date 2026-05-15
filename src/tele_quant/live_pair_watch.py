"""
live_pair_watch.py — 선행·후행 페어 실시간 관찰 엔진.

4시간마다 yfinance 최신 시세를 가져와 source 종목이 크게 움직였는데
target 종목이 아직 덜 움직인 상태를 찾아 관찰 후보로 표시한다.

⚠️  이 모듈의 출력은 매수·매도 추천이 아닙니다.
    통계적 관찰 후보이며 현재 가격 확인 전까지는 관찰만 합니다.
"""

from __future__ import annotations

import contextlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_DISCLAIMER = (
    "이 섹션은 매수·매도 지시가 아닙니다. "
    "과거 선행·후행 패턴과 현재 시세 차이를 바탕으로 한 통계적 관찰 후보이며, "
    "현재 가격·거래량·4H 기술 지표를 직접 확인하기 전까지는 관찰만 합니다."
)

_CONF_ORDER = {"high": 2, "medium": 1, "low": 0}
_KST = timezone(timedelta(hours=9))

# Module-level price CSV cache — keyed by file path to avoid inter-test contamination
_csv_price_cache: dict[str, Any] = {}


def _fmt_kst(dt_str: str) -> str:
    if not dt_str:
        return "?"
    try:
        dt = datetime.fromisoformat(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(_KST).strftime("%Y-%m-%d %H:%M KST")
    except Exception:
        return str(dt_str)[:16]


def _fmt_price_simple(price: float | None, market: str) -> str:
    if price is None:
        return "확인 불가"
    if (market or "").upper() == "KR":
        return f"{price:,.0f}원"
    return f"${price:,.2f}"


def _symbol_to_market_ticker(symbol: str) -> tuple[str, str]:
    """Convert yfinance ticker to (market, CSV-ticker) pair for local DB lookup."""
    if symbol.endswith(".KS"):
        return "KR", symbol[:-3]
    if symbol.endswith(".KQ"):
        return "KR", symbol[:-3]
    return "US", symbol


def _load_price_csv(settings: Any) -> Any:
    """Load event_price CSV once and cache by path. Returns pandas DataFrame or None."""
    csv_path = str(getattr(settings, "event_price_csv_path", "data/external/event_price_1000d.csv"))
    if csv_path in _csv_price_cache:
        return _csv_price_cache[csv_path]
    try:
        import pandas as pd

        df = pd.read_csv(csv_path, parse_dates=["date"])
        df = df.sort_values(["market", "ticker", "date"]).reset_index(drop=True)
        _csv_price_cache[csv_path] = df
        return df
    except Exception as exc:
        log.debug("[pair_watch] price CSV load failed: %s", exc)
        return None


_MIN_EVENT_COUNT = 5
_LAG_DAYS = 3


def _compute_local_pair_stats(
    source_symbol: str,
    target_symbol: str,
    expected_direction: str,
    settings: Any,
) -> tuple[float | None, float | None, int]:
    """
    Compute (conditional_prob, lift, event_count) from local event_price CSV.

    Returns (None, None, 0) on any error or insufficient data.
    Stale relation feed와 무관하게 항상 계산 시도.
    """
    try:
        df = _load_price_csv(settings)
        if df is None or getattr(df, "empty", True):
            return None, None, 0

        src_market, src_ticker = _symbol_to_market_ticker(source_symbol)
        tgt_market, tgt_ticker = _symbol_to_market_ticker(target_symbol)

        src_df = df[(df["market"] == src_market) & (df["ticker"] == src_ticker)].copy()
        tgt_df = df[(df["market"] == tgt_market) & (df["ticker"] == tgt_ticker)].copy()

        if len(src_df) < 10 or len(tgt_df) < 10:
            return None, None, 0

        src_df = src_df.sort_values("date").reset_index(drop=True)
        tgt_df = tgt_df.sort_values("date").reset_index(drop=True)

        # Daily returns for source
        src_threshold = 7.0 if src_market == "KR" else 5.0
        src_df["ret"] = src_df["close"].pct_change() * 100

        # Identify event days
        if expected_direction == "UP":
            event_rows = src_df[src_df["ret"] >= src_threshold]
        else:
            event_rows = src_df[src_df["ret"] <= -src_threshold]

        event_count_raw = len(event_rows)
        if event_count_raw < _MIN_EVENT_COUNT:
            return None, None, event_count_raw

        # For each event, check target's cumulative return over lag 1-3 trading days
        hit_count = 0
        valid_events = 0

        for _, evt in event_rows.iterrows():
            event_date = evt["date"]
            # Target close on or before event date
            mask_before = tgt_df["date"] <= event_date
            mask_after = tgt_df["date"] > event_date
            tgt_before = tgt_df[mask_before]
            tgt_after = tgt_df[mask_after].head(_LAG_DAYS)

            if tgt_before.empty or tgt_after.empty:
                continue

            tgt_base = float(tgt_before["close"].iloc[-1])
            if tgt_base <= 0:
                continue

            tgt_end = float(tgt_after["close"].iloc[-1])
            tgt_return = (tgt_end - tgt_base) / tgt_base * 100

            valid_events += 1
            if (expected_direction == "UP" and tgt_return > 0) or (
                expected_direction == "DOWN" and tgt_return < 0
            ):
                hit_count += 1

        if valid_events < _MIN_EVENT_COUNT:
            return None, None, valid_events

        cond_prob = hit_count / valid_events

        # Base probability: fraction of all LAG_DAYS-day target windows in expected direction
        base_hits = 0
        base_total = 0
        for i in range(len(tgt_df) - _LAG_DAYS):
            base_close = float(tgt_df["close"].iloc[i])
            end_close = float(tgt_df["close"].iloc[i + _LAG_DAYS])
            if base_close <= 0:
                continue
            ret = (end_close - base_close) / base_close * 100
            if (expected_direction == "UP" and ret > 0) or (
                expected_direction == "DOWN" and ret < 0
            ):
                base_hits += 1
            base_total += 1

        if base_total == 0:
            return cond_prob, None, valid_events

        base_prob = base_hits / base_total
        lift = cond_prob / base_prob if base_prob > 0 else None

        return cond_prob, lift, valid_events

    except Exception as exc:
        log.debug(
            "[pair_watch] local_pair_stats %s→%s failed: %s", source_symbol, target_symbol, exc
        )
        return None, None, 0


# ── Data Models ─────────────────────────────────────────────────────────────


@dataclass
class UniverseStock:
    ticker: str
    name: str
    market: str
    sector: str
    theme: str
    role: str


@dataclass
class PairRule:
    id: str
    sector: str
    theme: str
    source: str
    targets: list[str]
    direction: str
    min_source_move_pct: float
    note: str


@dataclass
class TickerPrice:
    symbol: str
    return_4h: float | None = None
    return_1d: float | None = None
    volume_ratio: float | None = None
    close: float | None = None
    fetched_at: str = ""
    from_cache: bool = False


@dataclass
class LivePairSignal:
    created_at: str
    source_symbol: str
    source_name: str
    source_market: str
    source_sector: str
    source_theme: str
    source_return_4h: float | None
    source_return_1d: float | None
    source_volume_ratio: float | None
    target_symbol: str
    target_name: str
    target_market: str
    target_sector: str
    target_theme: str
    target_return_4h: float | None
    target_return_1d: float | None
    target_volume_ratio: float | None
    relation_type: str
    expected_direction: str
    gap_type: str
    lag_status: str
    correlation: float | None
    conditional_prob: float | None
    lift: float | None
    confidence: str
    pair_score: float
    explanation: str
    watch_action: str
    rule_note: str = ""
    used_cache: bool = False
    event_count: int = 0
    target_price_at_signal: float | None = None


# ── Config Loaders ───────────────────────────────────────────────────────────


def _universe_path(settings: Any) -> Path:
    return Path(getattr(settings, "pair_watch_universe_path", "config/pair_watch_universe.yml"))


def _rules_path(settings: Any) -> Path:
    return Path(getattr(settings, "pair_watch_rules_path", "config/pair_watch_rules.yml"))


def load_universe(settings: Any) -> list[UniverseStock]:
    """Load pair watch universe from YAML. Returns empty list on error."""
    path = _universe_path(settings)
    try:
        import yaml

        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        result: list[UniverseStock] = []
        for item in data.get("stocks", []):
            result.append(
                UniverseStock(
                    ticker=str(item.get("ticker", "")),
                    name=str(item.get("name", "")),
                    market=str(item.get("market", "US")).upper(),
                    sector=str(item.get("sector", "")),
                    theme=str(item.get("theme", "")),
                    role=str(item.get("role", "both")),
                )
            )
        log.info("[pair_watch] universe loaded: %d stocks from %s", len(result), path)
        return result
    except FileNotFoundError:
        log.warning("[pair_watch] universe file not found: %s", path)
        return []
    except Exception as exc:
        log.warning("[pair_watch] universe load error: %s", exc)
        return []


def load_rules(settings: Any) -> list[PairRule]:
    """Load pair watch rules from YAML. Returns empty list on error."""
    path = _rules_path(settings)
    try:
        import yaml

        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        result: list[PairRule] = []
        for item in data.get("rules", []):
            result.append(
                PairRule(
                    id=str(item.get("id", "")),
                    sector=str(item.get("sector", "")),
                    theme=str(item.get("theme", "")),
                    source=str(item.get("source", "")),
                    targets=[str(t) for t in item.get("targets", [])],
                    direction=str(item.get("direction", "UP_LEADS_UP")),
                    min_source_move_pct=float(item.get("min_source_move_pct", 2.5)),
                    note=str(item.get("note", "")),
                )
            )
        log.info("[pair_watch] rules loaded: %d rules from %s", len(result), path)
        return result
    except FileNotFoundError:
        log.warning("[pair_watch] rules file not found: %s", path)
        return []
    except Exception as exc:
        log.warning("[pair_watch] rules load error: %s", exc)
        return []


# ── Price Cache ──────────────────────────────────────────────────────────────


def _cache_path(settings: Any) -> Path:
    return Path("data/cache/live_pair_watch_prices.json")


def _load_cache(settings: Any) -> dict[str, Any]:
    path = _cache_path(settings)
    try:
        if not path.exists():
            return {}
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_cache(settings: Any, prices: dict[str, TickerPrice]) -> None:
    path = _cache_path(settings)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any] = {
            "fetched_at": datetime.now(UTC).isoformat(),
            "prices": {
                sym: {
                    "return_4h": p.return_4h,
                    "return_1d": p.return_1d,
                    "volume_ratio": p.volume_ratio,
                    "close": p.close,
                }
                for sym, p in prices.items()
                if not p.from_cache
            },
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception as exc:
        log.debug("[pair_watch] cache save failed: %s", exc)


def _cache_age_hours(cache: dict[str, Any]) -> float | None:
    fetched_at = cache.get("fetched_at", "")
    if not fetched_at:
        return None
    try:
        dt = datetime.fromisoformat(fetched_at)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return (datetime.now(UTC) - dt).total_seconds() / 3600.0
    except Exception:
        return None


def _prices_from_cache(cache: dict[str, Any], symbols: list[str]) -> dict[str, TickerPrice]:
    cached_prices = cache.get("prices", {})
    result: dict[str, TickerPrice] = {}
    for sym in symbols:
        if sym in cached_prices:
            p = cached_prices[sym]
            result[sym] = TickerPrice(
                symbol=sym,
                return_4h=p.get("return_4h"),
                return_1d=p.get("return_1d"),
                volume_ratio=p.get("volume_ratio"),
                close=p.get("close"),
                fetched_at=cache.get("fetched_at", ""),
                from_cache=True,
            )
    return result


# ── Price Fetching ───────────────────────────────────────────────────────────


def _calc_returns_from_df(sym: str, df: Any) -> TickerPrice:
    """Calculate 4h/1d return and volume ratio from OHLCV DataFrame.

    yfinance returns daily bars for many KR tickers even when interval='1h'.
    Detect this by checking the median bar gap; use proper daily-bar formulas
    (last vs prev-close for 1D, last vs 5-bar-ago for ~5D/4H label) in that case.
    """
    result = TickerPrice(symbol=sym)
    try:
        if df is None or getattr(df, "empty", True):
            return result
        closes = df["Close"].dropna()
        n = len(closes)
        if n < 2:
            return result
        last_close = float(closes.iloc[-1])
        result.close = last_close

        # Detect daily bars: median gap between bars >= 20 hours
        is_daily = False
        if hasattr(closes.index, "to_series") and n >= 2:
            gaps = closes.index.to_series().diff().dropna()
            if len(gaps) > 0:
                median_gap_h = gaps.median().total_seconds() / 3600
                is_daily = median_gap_h >= 20.0

        if is_daily:
            # Daily bars: 1D = last close vs previous close; 4H = None (no intraday data)
            prev_close = float(closes.iloc[-2])
            if prev_close > 0:
                result.return_1d = (last_close - prev_close) / prev_close * 100
            # 4H is meaningless with daily bars — leave as None
        else:
            # Hourly bars: 4H = last 4 hours (4 bars back), 1D = last ~24h (24 bars back)
            if n >= 5:
                prev = float(closes.iloc[-5])
                if prev > 0:
                    result.return_4h = (last_close - prev) / prev * 100

            idx_1d = max(0, n - 25)
            prev_1d = float(closes.iloc[idx_1d])
            if prev_1d > 0 and idx_1d < n - 1:
                result.return_1d = (last_close - prev_1d) / prev_1d * 100

        # Volume ratio
        if "Volume" in df.columns:
            vols = df["Volume"].dropna()
            if len(vols) >= 5:
                last_vol = float(vols.iloc[-1])
                avg_vol = float(vols.iloc[:-1].mean())
                if avg_vol > 0:
                    result.volume_ratio = last_vol / avg_vol
    except Exception as exc:
        log.debug("[pair_watch] calc_returns %s failed: %s", sym, exc)
    return result


def _fetch_yfinance_batch(
    symbols: list[str],
    period: str,
    interval: str,
    batch_size: int = 20,
) -> tuple[dict[str, TickerPrice], list[str]]:
    """Batch-fetch prices via yfinance. Returns (prices_dict, failed_symbols)."""
    try:
        import yfinance as yf
    except ImportError:
        log.warning("[pair_watch] yfinance not installed")
        return {}, list(symbols)

    prices: dict[str, TickerPrice] = {}
    failed: list[str] = []

    # Process in batches to stay within yfinance limits
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        try:
            if len(batch) == 1:
                df = yf.Ticker(batch[0]).history(period=period, interval=interval, auto_adjust=True)
                if df is not None and not df.empty:
                    prices[batch[0]] = _calc_returns_from_df(batch[0], df)
                else:
                    failed.append(batch[0])
            else:
                raw = yf.download(
                    tickers=batch,
                    period=period,
                    interval=interval,
                    group_by="ticker",
                    auto_adjust=True,
                    progress=False,
                    threads=True,
                )
                if raw is None or getattr(raw, "empty", True):
                    failed.extend(batch)
                    continue
                import pandas as pd

                if isinstance(raw.columns, pd.MultiIndex):
                    for sym in batch:
                        try:
                            sym_df = raw[sym]
                            if sym_df is not None and not sym_df.empty:
                                prices[sym] = _calc_returns_from_df(sym, sym_df)
                            else:
                                failed.append(sym)
                        except (KeyError, TypeError):
                            failed.append(sym)
                else:
                    # Single ticker returned without MultiIndex
                    prices[batch[0]] = _calc_returns_from_df(batch[0], raw)
        except Exception as exc:
            log.warning("[pair_watch] batch fetch failed (%s): %s", batch[:3], exc)
            failed.extend(batch)

    return prices, failed


def _fetch_local_csv_prices(
    symbols: list[str],
    settings: Any,
) -> dict[str, TickerPrice]:
    """Fallback: read daily close from local event_price CSV."""
    result: dict[str, TickerPrice] = {}
    try:
        import pandas as pd

        csv_path = Path(
            getattr(settings, "event_price_csv_path", "data/external/event_price_1000d.csv")
        )
        if not csv_path.exists():
            return result
        df = pd.read_csv(csv_path)
        if "symbol" not in df.columns or "close" not in df.columns:
            return result
        for sym in symbols:
            sub = df[df["symbol"] == sym].sort_values("date", ascending=False).head(3)
            if len(sub) < 2:
                continue
            last_close = float(sub["close"].iloc[0])
            prev_close = float(sub["close"].iloc[1])
            ret_1d = (last_close - prev_close) / prev_close * 100 if prev_close > 0 else None
            result[sym] = TickerPrice(
                symbol=sym,
                return_4h=None,
                return_1d=ret_1d,
                volume_ratio=None,
                close=last_close,
                fetched_at=datetime.now(UTC).isoformat(),
                from_cache=False,
            )
    except Exception as exc:
        log.debug("[pair_watch] local_csv fallback failed: %s", exc)
    return result


def _fetch_relation_feed_prices(
    symbols: list[str],
    settings: Any,
) -> dict[str, TickerPrice]:
    """Fallback: read movers CSV from shared relation feed directory."""
    result: dict[str, TickerPrice] = {}
    try:
        import csv

        feed_dir = Path(
            getattr(
                settings,
                "relation_feed_dir",
                "/home/kwanni/projects/quant_spillover/shared_relation_feed",
            )
        )
        movers_path = feed_dir / "latest_movers.csv"
        if not movers_path.exists():
            return result
        sym_set = set(symbols)
        with open(movers_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sym = row.get("symbol", "")
                if sym not in sym_set:
                    continue
                try:
                    ret = float(row.get("return_pct", 0) or 0)
                    close = float(row.get("close", 0) or 0) or None
                    result[sym] = TickerPrice(
                        symbol=sym,
                        return_4h=None,
                        return_1d=ret if ret != 0 else None,
                        volume_ratio=float(row.get("volume_ratio_20d", 0) or 0) or None,
                        close=close,
                        fetched_at=datetime.now(UTC).isoformat(),
                        from_cache=False,
                    )
                except (ValueError, TypeError):
                    pass
    except Exception as exc:
        log.debug("[pair_watch] relation_feed fallback failed: %s", exc)
    return result


def fetch_prices(
    symbols: list[str],
    settings: Any,
) -> tuple[dict[str, TickerPrice], bool]:
    """
    Fetch live prices for all symbols.

    Priority: cache (if fresh) → yfinance → local CSV → relation feed.
    Returns (prices_dict, used_stale_cache).
    """
    ttl_hours = float(getattr(settings, "live_pair_watch_refresh_hours", 4.0))
    period = str(getattr(settings, "live_pair_watch_period", "60d"))
    interval = str(getattr(settings, "live_pair_watch_interval", "1h"))

    cache = _load_cache(settings)
    age = _cache_age_hours(cache)
    used_stale = False

    if age is not None and age <= ttl_hours:
        cached = _prices_from_cache(cache, symbols)
        if len(cached) == len(symbols):
            log.info("[pair_watch] all %d prices from cache (age=%.1fh)", len(symbols), age)
            return cached, False

    # Live fetch
    log.info("[pair_watch] fetching live prices for %d symbols (yfinance)", len(symbols))
    live_prices, failed = _fetch_yfinance_batch(symbols, period=period, interval=interval)
    log.info("[pair_watch] yfinance: got=%d failed=%d", len(live_prices), len(failed))

    # Fallback 1: local CSV for failed symbols
    if failed:
        csv_prices = _fetch_local_csv_prices(failed, settings)
        live_prices.update(csv_prices)
        still_failed = [s for s in failed if s not in live_prices]
        log.info(
            "[pair_watch] local_csv fallback: got=%d still_failed=%d",
            len(csv_prices),
            len(still_failed),
        )
        failed = still_failed

    # Fallback 2: relation feed for still-failed symbols
    if failed:
        rf_prices = _fetch_relation_feed_prices(failed, settings)
        live_prices.update(rf_prices)
        log.info("[pair_watch] relation_feed fallback: got=%d", len(rf_prices))

    # Fill in stale cache for anything still missing
    if age is not None and cache:
        stale_cached = _prices_from_cache(cache, [s for s in symbols if s not in live_prices])
        if stale_cached:
            live_prices.update(stale_cached)
            used_stale = True
            log.info("[pair_watch] stale cache used for %d symbols", len(stale_cached))

    # Persist fresh prices to cache (non-cache entries only)
    fresh = {k: v for k, v in live_prices.items() if not v.from_cache}
    if fresh:
        _save_cache(settings, fresh)

    return live_prices, used_stale


# ── Signal Calculation ───────────────────────────────────────────────────────


def _primary_return(p: TickerPrice) -> float:
    """Return the best available return metric: prefer 4h, fallback to 1d."""
    if p.return_4h is not None:
        return p.return_4h
    if p.return_1d is not None:
        return p.return_1d
    return 0.0


def _classify_gap(
    rule: PairRule,
    src: TickerPrice,
    tgt: TickerPrice,
) -> str:
    """Classify how target reacted relative to source movement."""
    src_ret = _primary_return(src)
    tgt_4h = tgt.return_4h
    tgt_1d = tgt.return_1d
    tgt_ret = _primary_return(tgt)

    expects_up = src_ret > 0  # UP_LEADS_UP or DOWN_LEADS_DOWN in reverse
    if ("DOWN_LEADS_DOWN" in rule.direction and src_ret < 0) or (
        "UP_LEADS_DOWN" in rule.direction and src_ret > 0
    ):
        expects_up = False

    if expects_up:
        if tgt_ret >= 3.0:
            return "이미반응"
        # Both 4H and 1D explicitly negative → current dissonance despite mild individual values
        t4h_neg = tgt_4h is not None and tgt_4h < 0
        t1d_neg = tgt_1d is not None and tgt_1d < 0
        if t4h_neg and t1d_neg:
            return "현재불일치"
        if tgt_ret < -1.0:
            return "불일치"
        elif tgt_ret <= 1.0:
            return "미반응"
        else:
            return "부분반응"
    else:
        # Expects target to fall
        if tgt_ret > 1.0:
            return "불일치"
        elif tgt_ret >= -1.0:
            return "약세전이미확인"
        elif tgt_ret <= -3.0:
            return "이미반응"
        else:
            return "부분반응"


def _get_relation_stats(
    relation_feed: Any,
    source_symbol: str,
    target_symbol: str,
) -> tuple[float | None, float | None, int, str]:
    """Extract (conditional_prob, lift, event_count, relation_type) from feed if available."""
    if relation_feed is None or not getattr(relation_feed, "available", False):
        return None, None, 0, "UP_LEADS_UP"

    for row in getattr(relation_feed, "leadlag", []):
        if row.source_symbol == source_symbol and row.target_symbol == target_symbol:
            return row.conditional_prob, row.lift, row.event_count, row.relation_type

    for c in getattr(relation_feed, "fallback_candidates", []):
        if c.source_symbol == source_symbol and c.target_symbol == target_symbol:
            return (
                c.conditional_prob,
                c.lift,
                getattr(c, "event_count", 0),
                getattr(c, "relation_type", "UP_LEADS_UP"),
            )

    return None, None, 0, "UP_LEADS_UP"


def _get_correlation(corr_store: Any, sym_a: str, sym_b: str) -> float | None:
    if corr_store is None:
        return None
    try:
        val = corr_store.get_correlation(sym_a, sym_b)
        return float(val) if val is not None else None
    except Exception:
        return None


def _compute_pair_score(
    src: TickerPrice,
    tgt: TickerPrice,
    gap_type: str,
    conditional_prob: float | None,
    lift: float | None,
    correlation: float | None,
    src_stock: UniverseStock | None,
    tgt_stock: UniverseStock | None,
) -> float:
    score = 0.0
    src_ret = abs(_primary_return(src))

    # Source move strength (0-30)
    score += min(30.0, src_ret * 4.0)
    if src.volume_ratio is not None and src.volume_ratio >= 1.2:
        score += 5.0

    # Target underreaction (0-30)
    if gap_type == "미반응":
        score += 28.0
    elif gap_type == "약세전이미확인":
        score += 22.0
    elif gap_type == "부분반응":
        score += 12.0
    elif gap_type == "이미반응":
        score -= 20.0
    elif gap_type in ("현재불일치", "불일치"):
        score -= 15.0

    # Historical statistics (0-15)
    if conditional_prob is not None:
        score += min(8.0, conditional_prob * 12.0)
    if lift is not None and lift > 1.0:
        score += min(7.0, (lift - 1.0) * 6.0)

    # Correlation (0-10)
    if correlation is not None and correlation > 0:
        score += min(10.0, correlation * 12.0)

    # Sector/theme match (0-5)
    if src_stock and tgt_stock:
        if src_stock.sector == tgt_stock.sector:
            score += 3.0
        if (
            src_stock.theme == tgt_stock.theme
            or src_stock.theme.split("→")[0].strip() == tgt_stock.theme
        ):
            score += 2.0

    # Target volume (0-5)
    if tgt.volume_ratio is not None and tgt.volume_ratio >= 1.0:
        score += 3.0

    return max(0.0, min(100.0, score))


def _compute_confidence(
    event_count: int,
    conditional_prob: float | None,
    lift: float | None,
    src_volume_ratio: float | None,
) -> str:
    prob = conditional_prob or 0.0
    lft = lift or 0.0
    vol = src_volume_ratio or 0.0

    if event_count >= 20 and prob >= 0.60 and lft >= 1.5 and vol >= 1.2:
        return "high"
    if event_count >= 10 and prob >= 0.55 and lft >= 1.2:
        return "medium"
    if event_count > 0 or prob > 0 or lft > 1.0:
        return "medium"
    # No statistical basis → always "low" regardless of volume
    # (volume alone doesn't establish historical lead-lag probability)
    return "low"


def _watch_action(gap_type: str, confidence: str) -> str:
    if gap_type == "미반응":
        if confidence in ("high", "medium"):
            return "장중 확인 후보 — 거래량 증가 + 4H RSI 우상향 확인"
        return "가격 확인 후 관찰"
    if gap_type == "약세전이미확인":
        if confidence in ("high", "medium"):
            return "약세 확산 관찰 — 반등 실패 + 거래량 동반 하락 여부 확인"
        return "가격 확인 후 관찰"
    if gap_type == "이미반응":
        return "추격주의 — target이 이미 많이 움직임"
    if gap_type == "부분반응":
        return "부분 확인 — target이 일부 반응, 추가 확인 필요"
    if gap_type == "현재불일치":
        return "관찰만 — target 4H·1D 모두 음수, 추가 약세 주의"
    if gap_type == "불일치":
        return "관찰만 — 통계와 현재가 불일치"
    return "현재가 확인 필요"


def _explain_signal(
    rule: PairRule,
    src_stock: UniverseStock | None,
    tgt_stock: UniverseStock | None,
    src: TickerPrice,
    tgt: TickerPrice,
    gap_type: str,
    conditional_prob: float | None,
    lift: float | None,
) -> str:
    src_name = (src_stock.name if src_stock else rule.source) or rule.source
    tgt_name = (tgt_stock.name if tgt_stock else "") or tgt.symbol
    src_ret = _primary_return(src)
    sign = "+" if src_ret > 0 else ""
    parts = [f"{src_name} {sign}{src_ret:.1f}% 후 {tgt_name} 후행 반응 관찰"]
    if rule.note:
        parts.append(rule.note)
    if conditional_prob is not None:
        parts.append(f"조건부확률 {conditional_prob:.1%}")
    if lift is not None:
        parts.append(f"lift {lift:.1f}x")
    return " / ".join(parts)


# ── Main Signal Computation ──────────────────────────────────────────────────


def compute_signals(
    settings: Any,
    universe: list[UniverseStock],
    rules: list[PairRule],
    prices: dict[str, TickerPrice],
    relation_feed: Any = None,
    corr_store: Any = None,
    used_cache: bool = False,
) -> list[LivePairSignal]:
    """Compute live pair signals. Never raises."""
    if not rules or not prices:
        return []

    global_min_move = float(getattr(settings, "live_pair_watch_min_source_move_pct", 2.5))
    min_conf_str = str(getattr(settings, "live_pair_watch_min_confidence", "medium")).lower()
    max_targets = int(getattr(settings, "live_pair_watch_max_targets", 40))

    stock_map: dict[str, UniverseStock] = {s.ticker: s for s in universe}
    now_str = datetime.now(UTC).isoformat()

    signals: list[LivePairSignal] = []
    seen_targets: set[str] = set()

    for rule in rules:
        src_sym = rule.source
        src_price = prices.get(src_sym)
        if src_price is None:
            continue

        src_ret = _primary_return(src_price)
        effective_min = max(global_min_move, rule.min_source_move_pct)
        if abs(src_ret) < effective_min:
            continue

        src_stock = stock_map.get(src_sym)
        expected_dir = "UP" if src_ret > 0 else "DOWN"

        for tgt_sym in rule.targets:
            if tgt_sym == src_sym:
                continue
            tgt_price = prices.get(tgt_sym)
            if tgt_price is None:
                continue

            gap_type = _classify_gap(rule, src_price, tgt_price)

            # Skip "이미반응" — target already moved, nothing to observe
            if gap_type == "이미반응":
                continue

            cond_prob, lift, event_count, rel_type = _get_relation_stats(
                relation_feed, src_sym, tgt_sym
            )

            # When no relation-feed stats, try local price CSV
            if cond_prob is None and lift is None and event_count == 0:
                local_prob, local_lift, local_count = _compute_local_pair_stats(
                    src_sym, tgt_sym, expected_dir, settings
                )
                if local_prob is not None or local_count >= _MIN_EVENT_COUNT:
                    cond_prob, lift, event_count = local_prob, local_lift, local_count

            correlation = _get_correlation(corr_store, src_sym, tgt_sym)

            tgt_stock = stock_map.get(tgt_sym)
            pair_score = _compute_pair_score(
                src_price,
                tgt_price,
                gap_type,
                cond_prob,
                lift,
                correlation,
                src_stock,
                tgt_stock,
            )
            confidence = _compute_confidence(event_count, cond_prob, lift, src_price.volume_ratio)

            # Filter by min confidence — bypass for rule-based signals (no stats)
            # so they appear with "규칙기반" label rather than being silently dropped
            has_stats = event_count > 0 or cond_prob is not None or lift is not None
            if has_stats:
                if min_conf_str == "high" and confidence != "high":
                    continue
                if min_conf_str == "medium" and _CONF_ORDER.get(confidence, 0) < 1:
                    continue

            explanation = _explain_signal(
                rule,
                src_stock,
                tgt_stock,
                src_price,
                tgt_price,
                gap_type,
                cond_prob,
                lift,
            )
            watch_action = _watch_action(gap_type, confidence)

            tgt_market = (
                tgt_stock.market
                if tgt_stock
                else "KR"
                if tgt_sym.endswith((".KS", ".KQ"))
                else "US"
            )

            signals.append(
                LivePairSignal(
                    created_at=now_str,
                    source_symbol=src_sym,
                    source_name=(src_stock.name if src_stock else "") or src_sym,
                    source_market=(src_stock.market if src_stock else "US"),
                    source_sector=(src_stock.sector if src_stock else rule.sector),
                    source_theme=(src_stock.theme if src_stock else rule.theme),
                    source_return_4h=src_price.return_4h,
                    source_return_1d=src_price.return_1d,
                    source_volume_ratio=src_price.volume_ratio,
                    target_symbol=tgt_sym,
                    target_name=(tgt_stock.name if tgt_stock else "") or tgt_sym,
                    target_market=tgt_market,
                    target_sector=(tgt_stock.sector if tgt_stock else rule.sector),
                    target_theme=(tgt_stock.theme if tgt_stock else rule.theme),
                    target_return_4h=tgt_price.return_4h,
                    target_return_1d=tgt_price.return_1d,
                    target_volume_ratio=tgt_price.volume_ratio,
                    relation_type=rel_type,
                    expected_direction=expected_dir,
                    gap_type=gap_type,
                    lag_status="미확인",
                    correlation=correlation,
                    conditional_prob=cond_prob,
                    lift=lift,
                    confidence=confidence,
                    pair_score=pair_score,
                    explanation=explanation,
                    watch_action=watch_action,
                    rule_note=rule.note,
                    used_cache=used_cache,
                    event_count=event_count,
                    target_price_at_signal=tgt_price.close,
                )
            )

    # Sort by pair_score desc, then deduplicate targets (keep highest score per target)
    signals.sort(key=lambda s: s.pair_score, reverse=True)
    deduped: list[LivePairSignal] = []
    for sig in signals:
        if sig.target_symbol not in seen_targets:
            seen_targets.add(sig.target_symbol)
            deduped.append(sig)
        if len(deduped) >= max_targets:
            break

    return deduped


# ── Report Section Builder ───────────────────────────────────────────────────

_SECTOR_LABELS: dict[str, str] = {
    "semiconductor": "반도체 / AI 칩",
    "power_infra": "AI 전력·ESS·원전",
    "cosmetics": "화장품 / K-뷰티",
    "defense": "조선 / 방산",
}

_GAP_LABELS: dict[str, str] = {
    "미반응": "미반응 관찰",
    "약세전이미확인": "약세 확산 관찰",
    "부분반응": "부분 확인 관찰",
    "현재불일치": "현재 불일치 (관찰만)",
    "불일치": "통계·현재가 불일치 관찰",
    "이미반응": "이미 반응 (추격주의)",
}


def _fmt_return(val: float | None) -> str:
    if val is None:
        return "N/A"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.1f}%"


def _fmt_prob_lift(cond_prob: float | None, lift: float | None, event_count: int = 0) -> str:
    if cond_prob is None and lift is None:
        return "규칙 기반 (통계 N/A, 표본 부족)"
    parts: list[str] = []
    if event_count >= _MIN_EVENT_COUNT and cond_prob is not None:
        hit_count = round(event_count * cond_prob)
        parts.append(f"과거 유사 이벤트 {event_count}회 중 {hit_count}회 반응")
    if cond_prob is not None:
        parts.append(f"조건부확률 {cond_prob:.1%}")
    if lift is not None:
        parts.append(f"lift {lift:.1f}x")
    return " / ".join(parts) or "규칙 기반"


def build_pair_watch_section(
    signals: list[LivePairSignal],
    settings: Any = None,
    used_stale_cache: bool = False,
    diagnostics: list[str] | None = None,
) -> str:
    """Build the 🔗 선행·후행 페어 관찰 section for inclusion in 4H reports."""
    max_items = min(6, int(getattr(settings, "live_pair_watch_max_report_items", 6)))
    MAX_PER_SECTOR = 2
    MAX_PER_SOURCE = 2
    MAX_DISSONANCE = 2

    sector_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    dissonance_count = 0

    now_kst = datetime.now(_KST).strftime("%Y-%m-%d %H:%M KST")
    lines: list[str] = ["🔗 선행·후행 페어 관찰"]
    lines.append(
        "- 데이터: Yahoo/yfinance 1h" + (" + 일부 가격 캐시 사용" if used_stale_cache else "")
    )
    lines.append(f"- 가격 갱신: {now_kst}")
    lines.append("- 저장: 실제 전송 리포트만 성과 DB 저장")
    lines.append("- 중복: 같은 source-target은 하루 1회 대표 신호로 관리")
    lines.append("- 기준: 최근 4시간/1일 source 급등락 대비 target 반응 차이")
    lines.append("- 주의: 매수·매도 지시 아님, 통계적 관찰 후보")

    if diagnostics:
        for d in diagnostics:
            lines.append(f"- ⚠️ {d}")

    # Filter: show stat-based medium/high + all rule-based (no prob/lift) signals
    displayable = [
        s
        for s in signals
        if s.gap_type not in ("이미반응",)
        and (s.confidence in ("high", "medium") or (s.conditional_prob is None and s.lift is None))
    ]

    if not displayable:
        lines.append("- 현재 기준 충족 페어 없음 (source 움직임 부족 또는 모든 target이 이미 반응)")
        lines.append("")
        lines.append(f"※ {_DISCLAIMER}")
        return "\n".join(lines)

    lines.append("")
    item_count = 0  # counts source groups shown

    # Group displayable signals by source_symbol (preserving first-appearance order)
    source_group_order: list[str] = []
    by_source: dict[str, list[LivePairSignal]] = {}
    for sig in displayable:
        if sig.source_symbol not in by_source:
            by_source[sig.source_symbol] = []
            source_group_order.append(sig.source_symbol)
        by_source[sig.source_symbol].append(sig)

    idx = 0
    total_hidden = 0

    for source_sym in source_group_order:
        sigs = by_source[source_sym]
        sector = sigs[0].source_sector

        if item_count >= max_items or sector_counts.get(sector, 0) >= MAX_PER_SECTOR:
            total_hidden += len(sigs)
            continue

        # Select up to MAX_PER_SOURCE targets respecting dissonance cap
        shown: list[LivePairSignal] = []
        for sig in sigs:
            if len(shown) >= MAX_PER_SOURCE:
                total_hidden += 1
                continue
            if sig.gap_type == "현재불일치" and dissonance_count >= MAX_DISSONANCE:
                total_hidden += 1
                continue
            shown.append(sig)
            if sig.gap_type == "현재불일치":
                dissonance_count += 1

        if not shown:
            total_hidden += len(sigs)
            continue

        src = shown[0]
        src_ret_4h = _fmt_return(src.source_return_4h)
        src_ret_1d = _fmt_return(src.source_return_1d)
        sector_label = _SECTOR_LABELS.get(sector, sector)

        idx += 1
        lines.append(f"{idx}. {sector_label}")
        lines.append(
            f"   source: {src.source_name} / {source_sym}"
            f"  4H {src_ret_4h} / 1D {src_ret_1d}"
        )

        for sig in shown:
            tgt_ret_4h = _fmt_return(sig.target_return_4h)
            tgt_ret_1d = _fmt_return(sig.target_return_1d)
            gap_label = _GAP_LABELS.get(sig.gap_type, sig.gap_type)
            is_rule_based = sig.conditional_prob is None and sig.lift is None
            prob_str = _fmt_prob_lift(sig.conditional_prob, sig.lift, sig.event_count)
            conf_kr = (
                "규칙기반"
                if is_rule_based
                else {"high": "높음", "medium": "중간", "low": "낮음"}.get(
                    sig.confidence, sig.confidence
                )
            )
            lines.append(
                f"   → {gap_label}: {sig.target_name} / {sig.target_symbol}"
                f"  4H {tgt_ret_4h} / 1D {tgt_ret_1d}"
            )
            lines.append(f"     - 왜: {sig.rule_note or sig.explanation}")
            lines.append(f"     - 통계: {prob_str} / 신뢰도 {conf_kr}")
            lines.append(f"     - 오늘 볼 것: {sig.watch_action}")

        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        source_counts[source_sym] = len(shown)
        item_count += 1

    if total_hidden > 0:
        lines.append(f"  (그 외 {total_hidden}개 후보 숨김 — 소스·섹터·최대 {max_items}개 한도)")

    lines.append("")
    lines.append(f"※ {_DISCLAIMER}")
    return "\n".join(lines)


# ── CLI Summary Line (single-line format) ────────────────────────────────────


def format_signal_oneline(sig: LivePairSignal) -> str:
    """Format a single signal as one-liner for CLI table output."""
    src_ret = _fmt_return(sig.source_return_4h or sig.source_return_1d)
    tgt_ret = _fmt_return(sig.target_return_4h or sig.target_return_1d)
    prob_str = f"{sig.conditional_prob:.1%}" if sig.conditional_prob is not None else "N/A"
    lift_str = f"{sig.lift:.1f}x" if sig.lift is not None else "N/A"
    gap_kr = {
        "미반응": "미반응",
        "약세전이미확인": "약세미확인",
        "부분반응": "부분반응",
        "현재불일치": "현재불일치",
        "불일치": "불일치",
        "이미반응": "이미반응",
    }.get(sig.gap_type, sig.gap_type)
    action_short = (
        sig.watch_action.split(" — ")[0] if " — " in sig.watch_action else sig.watch_action
    )
    is_rule_based = sig.conditional_prob is None and sig.lift is None
    conf_label = "규칙기반" if is_rule_based else sig.confidence
    return (
        f"source {sig.source_symbol} {src_ret} → target {sig.target_name} {tgt_ret}, "
        f"gap={gap_kr}, prob={prob_str}, lift={lift_str}, "
        f"confidence={conf_label}, action={action_short}"
    )


# ── Main Entry Point ─────────────────────────────────────────────────────────


def run_pair_watch(
    settings: Any,
    sector_filter: str | None = None,
    relation_feed: Any = None,
    corr_store: Any = None,
) -> tuple[list[LivePairSignal], bool, list[str]]:
    """
    Full pair watch run: load universe+rules → fetch prices → compute signals.

    Returns (signals, used_stale_cache, diagnostics_list).
    Never raises.
    """
    diagnostics: list[str] = []

    if not getattr(settings, "live_pair_watch_enabled", True):
        return [], False, ["pair watch disabled"]

    universe = load_universe(settings)
    rules = load_rules(settings)

    if not universe:
        diagnostics.append("universe 파일을 읽을 수 없음")
    if not rules:
        diagnostics.append("rules 파일을 읽을 수 없음")
    if not universe or not rules:
        return [], False, diagnostics

    # relation_feed is now always self-computed (no stale concept)

    # Filter by sector if requested
    if sector_filter:
        sector_filter_lower = sector_filter.lower()
        # Map CLI short names to sector keys
        _SECTOR_ALIAS = {
            "semiconductor": "semiconductor",
            "semi": "semiconductor",
            "power": "power_infra",
            "ess": "power_infra",
            "energy": "power_infra",
            "cosmetics": "cosmetics",
            "beauty": "cosmetics",
            "defense": "defense",
            "shipbuilding": "defense",
        }
        sector_key = _SECTOR_ALIAS.get(sector_filter_lower, sector_filter_lower)
        rules = [r for r in rules if r.sector == sector_key or sector_filter_lower in r.sector]
        universe = [
            u for u in universe if u.sector == sector_key or sector_filter_lower in u.sector
        ]
        if not rules:
            diagnostics.append(f"sector '{sector_filter}' 에 해당하는 규칙 없음")
            return [], False, diagnostics

    # Collect all symbols needed
    all_symbols: set[str] = set()
    for rule in rules:
        all_symbols.add(rule.source)
        all_symbols.update(rule.targets)

    max_sources = int(getattr(settings, "live_pair_watch_max_sources", 30))
    max_total = max_sources + int(getattr(settings, "live_pair_watch_max_targets", 40))
    sym_list = list(all_symbols)[:max_total]

    prices, used_stale = fetch_prices(sym_list, settings)

    failed_syms = [s for s in sym_list if s not in prices]
    if failed_syms:
        diagnostics.append(
            f"가격 조회 실패: {', '.join(failed_syms[:5])}"
            + (f" 외 {len(failed_syms) - 5}개" if len(failed_syms) > 5 else "")
        )

    signals = compute_signals(
        settings,
        universe,
        rules,
        prices,
        relation_feed=relation_feed,
        corr_store=corr_store,
        used_cache=used_stale,
    )

    log.info("[pair_watch] signals computed: %d", len(signals))
    return signals, used_stale, diagnostics


# ── Weekly Performance Review ────────────────────────────────────────────────


def _fetch_review_price_yf(sym: str, mkt: str) -> float | None:
    """Fetch latest close price from yfinance (force fresh, no cache)."""
    if not sym:
        return None
    try:
        import yfinance as yf

        yf_sym = f"{sym}.KS" if (mkt or "").upper() == "KR" and not sym.endswith((".KS", ".KQ")) else sym
        df = yf.Ticker(yf_sym).history(period="2d", interval="1d", auto_adjust=True)
        if df is not None and not df.empty:
            return float(df["Close"].iloc[-1])
    except Exception:
        pass
    return None


def _group_pair_rows(rows: list[dict]) -> dict[tuple, list[dict]]:
    """Group rows by (source_symbol, target_symbol, expected_direction, relation_type)."""
    groups: dict[tuple, list[dict]] = {}
    for row in rows:
        key = (
            row.get("source_symbol", ""),
            row.get("target_symbol", ""),
            row.get("expected_direction", "UP"),
            row.get("relation_type", "") or "",
        )
        groups.setdefault(key, []).append(row)
    return groups


def _best_rep_row(group_rows: list[dict]) -> dict:
    """Pick the representative row with highest pair_score (or first)."""
    return max(group_rows, key=lambda r: r.get("pair_score") or 0.0)


def build_pair_watch_weekly_review(
    store: Any,
    since: Any,
    settings: Any = None,
) -> str:
    """Build the weekly pair-watch performance review section.

    Deduplicates by (source, target, direction, relation_type) to show each pair once.
    Legacy rows with missing target_price_at_signal are shown as aggregate count only.
    """
    lines: list[str] = []
    lines.append("📈 선행·후행 페어 관찰 성과")
    lines.append("- 평가 기준: pair-watch 신호 시점 target 가격 vs 주간 리포트 시점 가격")
    lines.append("- 신호가: 최초 신호 시점 기준가 (고정) / 평가가: 주간 리포트 생성 시점 최신 가격")
    lines.append("- 주의: 실제 매매 수익이 아니라 통계 후보 사후 검증이며 매수·매도 권장 아님")

    try:
        rows = store.recent_pair_watch_signals(since=since, exclude_archived=True)
    except Exception as exc:
        lines.append(f"- DB 조회 실패: {exc}")
        lines.append("")
        lines.append("※ 이 섹션은 통계적 관찰 후보의 사후 검증입니다. 실제 수익 보장 아님.")
        return "\n".join(lines)

    if not rows:
        lines.append("- 이번 주 pair-watch 신호 없음")
        lines.append("")
        lines.append("※ 이 섹션은 통계적 관찰 후보의 사후 검증입니다. 실제 수익 보장 아님.")
        return "\n".join(lines)

    # Split into rows with/without signal price
    has_price = [r for r in rows if r.get("target_price_at_signal") is not None]
    legacy_count = len(rows) - len(has_price)

    # Deduplicate: one entry per (source, target, direction, relation_type)
    groups = _group_pair_rows(has_price)
    rep_rows: list[dict] = [_best_rep_row(g) for g in groups.values()]

    # For each representative row, get aggregated seen stats
    group_meta: dict[tuple, dict] = {}
    for key, group_rows in groups.items():
        first_seen = min((r.get("first_seen_at") or r.get("created_at") or "") for r in group_rows)
        last_seen = max((r.get("last_seen_at") or r.get("created_at") or "") for r in group_rows)
        seen_total = sum(r.get("seen_count") or 1 for r in group_rows)
        group_meta[key] = {
            "first_seen": first_seen,
            "last_seen": last_seen,
            "seen_total": seen_total,
        }

    now_kst_str = datetime.now(_KST).strftime("%Y-%m-%d %H:%M KST")

    # Fetch review prices (force refresh from yfinance)
    evaluable: list[dict] = []
    no_price_count = 0

    for rep in rep_rows:
        signal_price = rep.get("target_price_at_signal")
        if signal_price is None:
            continue  # already counted in legacy_count

        sym = rep.get("target_symbol", "")
        mkt = rep.get("target_market", "US")

        review_price = rep.get("target_price_at_review")
        review_updated = rep.get("review_price_updated_at") or ""

        # Always try to refresh review price
        fresh_price = _fetch_review_price_yf(sym, mkt)
        if fresh_price is not None:
            review_price = fresh_price
            review_updated = now_kst_str
            with contextlib.suppress(Exception):
                store.update_pair_watch_review(
                    rep["id"],
                    review_price,
                    _calc_outcome(signal_price, review_price, (rep.get("expected_direction") or "UP").upper()),
                    _calc_hit(signal_price, review_price, (rep.get("expected_direction") or "UP").upper()),
                )

        if review_price is None:
            no_price_count += 1
            continue

        expected = (rep.get("expected_direction") or "UP").upper()
        outcome = _calc_outcome(signal_price, review_price, expected)
        hit = _calc_hit(signal_price, review_price, expected)

        key = (
            rep.get("source_symbol", ""),
            rep.get("target_symbol", ""),
            rep.get("expected_direction", "UP"),
            rep.get("relation_type", "") or "",
        )
        evaluable.append({
            **rep,
            "target_price_at_review": review_price,
            "review_price_updated_at": review_updated,
            "outcome_return_pct": outcome,
            "hit": hit,
            "_meta": group_meta.get(key, {}),
        })

    total_groups = len(rep_rows)
    eval_count = len(evaluable)
    lines.append(f"- 이번 주 pair-watch 신호 (dedupe 기준): {total_groups}개 페어")
    lines.append(f"- 평가 가능: {eval_count}개")

    if evaluable:
        hits = sum(1 for r in evaluable if r.get("hit") == 1)
        rets = [r.get("outcome_return_pct", 0.0) or 0.0 for r in evaluable]
        avg_ret = sum(rets) / len(rets) if rets else 0.0
        hit_rate = hits / eval_count * 100 if eval_count > 0 else 0.0
        lines.append(f"- 평균 성과: {'+' if avg_ret >= 0 else ''}{avg_ret:.1f}%")
        lines.append(f"- 적중률: {hit_rate:.0f}% ({hits}/{eval_count})")

        # Gap-type breakdown
        for gap_type, label in [
            ("미반응", "미반응 관찰 후보"),
            ("부분반응", "부분반응 후보"),
            ("현재불일치", "현재불일치 후보"),
        ]:
            gap_rows = [r for r in evaluable if r.get("gap_type") == gap_type]
            if gap_rows:
                g_hits = sum(1 for r in gap_rows if r.get("hit") == 1)
                g_rets = [r.get("outcome_return_pct", 0.0) or 0.0 for r in gap_rows]
                g_avg = sum(g_rets) / len(g_rets)
                g_rate = g_hits / len(gap_rows) * 100
                lines.append(
                    f"- {label} 성과: 적중 {g_hits}/{len(gap_rows)} ({g_rate:.0f}%)"
                    f" / 평균 {'+' if g_avg >= 0 else ''}{g_avg:.1f}%"
                )

        winners = sorted(evaluable, key=lambda r: r.get("outcome_return_pct", 0) or 0, reverse=True)
        losers = sorted(evaluable, key=lambda r: r.get("outcome_return_pct", 0) or 0)
        if winners:
            w = winners[0]
            w_ret = w.get("outcome_return_pct", 0) or 0
            lines.append(
                f"- 가장 잘 맞은 페어: {w.get('source_symbol')} → {w.get('target_symbol')}"
                f" {'+' if w_ret >= 0 else ''}{w_ret:.1f}%"
            )
        if losers and losers[0].get("outcome_return_pct", 0) < 0:
            lo = losers[0]
            lo_ret = lo.get("outcome_return_pct", 0) or 0
            lines.append(
                f"- 빗나간 페어: {lo.get('source_symbol')} → {lo.get('target_symbol')}"
                f" {lo_ret:.1f}%"
            )

        # Best sector
        sector_hits: dict[str, list[float]] = {}
        for r in evaluable:
            sec = r.get("source_sector") or "기타"
            sector_hits.setdefault(sec, []).append(r.get("outcome_return_pct", 0) or 0)
        best_sectors = sorted(
            [(sec, sum(v) / len(v)) for sec, v in sector_hits.items()],
            key=lambda x: x[1],
            reverse=True,
        )
        if best_sectors:
            top_sector = _SECTOR_LABELS.get(best_sectors[0][0], best_sectors[0][0])
            lines.append(f"- 다음 주 반복 관찰 테마: {top_sector} (이번 주 성과 기준)")

        # Per-pair detail block — with display limits
        lines.append("")
        lines.append("[종목별 페어 성과]")

        # Limits: max 8 pairs, max 2 per source_symbol, max 2 per target_symbol
        shown_pairs: list[dict] = []
        source_counts: dict[str, int] = {}
        target_counts: dict[str, int] = {}
        hidden_count = 0
        for row in winners:
            src_sym = row.get("source_symbol", "?")
            tgt_sym = row.get("target_symbol", "?")
            if (
                len(shown_pairs) >= 8
                or source_counts.get(src_sym, 0) >= 2
                or target_counts.get(tgt_sym, 0) >= 2
            ):
                hidden_count += 1
                continue
            shown_pairs.append(row)
            source_counts[src_sym] = source_counts.get(src_sym, 0) + 1
            target_counts[tgt_sym] = target_counts.get(tgt_sym, 0) + 1

        for i, row in enumerate(shown_pairs, 1):
            src_sym = row.get("source_symbol", "?")
            tgt_sym = row.get("target_symbol", "?")
            src_name = row.get("source_name") or src_sym
            tgt_name = row.get("target_name") or tgt_sym
            mkt = row.get("target_market", "US")
            gap_type = row.get("gap_type", "?")
            outcome = row.get("outcome_return_pct")
            hit = row.get("hit")
            sp = row.get("target_price_at_signal")
            rp = row.get("target_price_at_review")
            rv_upd = row.get("review_price_updated_at") or "?"
            meta = row.get("_meta") or {}

            first_seen_str = _fmt_kst(meta.get("first_seen") or row.get("created_at", ""))
            last_seen_str = _fmt_kst(meta.get("last_seen") or row.get("last_seen_at") or row.get("created_at", ""))
            seen_total = meta.get("seen_total") or row.get("seen_count") or 1

            sp_str = _fmt_price_simple(sp, mkt)
            rp_str = _fmt_price_simple(rp, mkt)
            out_str = (
                f"{'+' if (outcome or 0) >= 0 else ''}{outcome:.1f}%"
                if outcome is not None
                else "?"
            )
            hit_str = "✅ 후행 반응 적중" if hit == 1 else "❌ 미확인"

            lines.append(f"{i}. {src_name} → {tgt_name} ({tgt_sym})")
            lines.append(f"   - 최초 신호 시점: {first_seen_str}")
            if seen_total > 1:
                lines.append(f"   - 마지막 재등장: {last_seen_str}")
                lines.append(f"   - 반복 감지: {seen_total}회")
            lines.append(f"   - 상태: {gap_type}")
            lines.append(f"   - 당시 target 기준가: {sp_str}")
            lines.append(f"   - 평가 기준가: {rp_str}")
            lines.append(f"   - 평가가 갱신: {rv_upd}")
            lines.append(f"   - 가상 성과: {out_str}")
            lines.append(f"   - 결과: {hit_str}")

        if hidden_count > 0:
            lines.append(f"  (그 외 {hidden_count}개 dedupe 후 숨김 — source·target 최대 2개 한도)")

    # Legacy / no-price summary (single line each, no details)
    if legacy_count > 0:
        lines.append(
            f"- 평가 대기 / 과거 가격 미기록: legacy row {legacy_count}개는 평가에서 제외했습니다."
        )
    if no_price_count > 0:
        lines.append(f"- 평가가 확인 불가: {no_price_count}개 제외")

    lines.append("")
    lines.append("※ 이 섹션은 통계적 관찰 후보의 사후 검증입니다. 실제 수익 보장 아님.")
    return "\n".join(lines)


def _calc_outcome(signal_price: float, review_price: float, expected: str) -> float:
    if expected == "UP":
        return (review_price - signal_price) / signal_price * 100
    return (signal_price - review_price) / signal_price * 100


def _calc_hit(signal_price: float, review_price: float, expected: str) -> int:
    if expected == "UP":
        return 1 if review_price > signal_price else 0
    return 1 if review_price < signal_price else 0
