from __future__ import annotations

import contextlib
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

from tele_quant.models import RunReport

_KST_OFFSET = timezone(timedelta(hours=9))


def _fmt_kst_datetime(dt: datetime | str | None) -> str:
    """Return 'YYYY-MM-DD HH:MM KST' from a datetime or ISO string."""
    if dt is None:
        return "?"
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except Exception:
            return str(dt)[:16]
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(_KST_OFFSET).strftime("%Y-%m-%d %H:%M KST")


def _fmt_price(price: float | None, market: str) -> str:
    if price is None:
        return "확인 불가"
    if (market or "").upper() == "KR":
        return f"{price:,.0f}원"
    return f"${price:,.2f}"


def _fmt_hold_period(from_dt: datetime | str | None, to_dt: datetime | None = None) -> str:
    if from_dt is None:
        return "?"
    if to_dt is None:
        to_dt = datetime.now(UTC)
    if isinstance(from_dt, str):
        try:
            from_dt = datetime.fromisoformat(from_dt)
        except Exception:
            return "?"
    if from_dt.tzinfo is None:
        from_dt = from_dt.replace(tzinfo=UTC)
    if to_dt.tzinfo is None:
        to_dt = to_dt.replace(tzinfo=UTC)
    delta = to_dt - from_dt
    if delta.total_seconds() <= 0:
        return "0시간"
    total_hours = int(delta.total_seconds() / 3600)
    days, hours = divmod(total_hours, 24)
    if days > 0 and hours > 0:
        return f"{days}일 {hours}시간"
    if days > 0:
        return f"{days}일"
    return f"{hours}시간"


def _fetch_review_price(symbol: str, market: str) -> float | None:
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


def build_relation_signal_review_section(
    store: Any,
    since: datetime,
    until: datetime | None = None,
) -> str:
    """주간 relation 신호 성과 리뷰 섹션. 가격 조회 및 DB 업데이트 포함."""
    lines: list[str] = []
    lines.append("📈 급등·급락 후행 후보 성과 리뷰")
    lines.append(
        "- 평가 기준: 4시간 리포트에 표시된 통계적 후행 후보를 신호 시점 가격 기준으로 추적"
    )
    lines.append("- 신호가 기준: 후보가 처음 리포트에 표시된 시점의 target 기준가")
    lines.append("- 평가가 기준: 주간 리포트 생성 시점의 target 최신 가격")
    lines.append("- 주의: 실제 매매 수익이 아니라 통계 후보 사후 검증")

    try:
        rows = store.recent_mover_chain_signals(since=since, until=until)
    except Exception as exc:
        lines.append(f"- DB 조회 실패: {exc}")
        lines.append("")
        lines.append(
            "※ 이 평가는 실제 매매 수익이 아니라, 통계적 후행 후보가 사후에 얼마나 맞았는지 점검하는 리서치 성과표입니다."
        )
        return "\n".join(lines)

    if not rows:
        lines.append("- 이번 주 관찰 후보 없음")
        lines.append("")
        lines.append(
            "※ 이 평가는 실제 매매 수익이 아니라, 통계적 후행 후보가 사후에 얼마나 맞았는지 점검하는 리서치 성과표입니다."
        )
        return "\n".join(lines)

    now = datetime.now(UTC)
    evaluable: list[dict] = []
    pending: list[dict] = []
    no_price: list[dict] = []
    review_price_cache: dict[str, float | None] = {}

    for row in rows:
        try:
            created_at = datetime.fromisoformat(row.get("created_at", ""))
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=UTC)
        except Exception:
            no_price.append(row)
            continue

        lag_days = int(row.get("lag_days") or 0)
        elapsed_days = (now - created_at).days

        if elapsed_days < lag_days:
            pending.append(row)
            continue

        signal_price = row.get("target_price_at_signal")
        if signal_price is None:
            no_price.append(row)
            continue

        # Already reviewed in a previous weekly run
        if row.get("target_price_at_review") is not None:
            evaluable.append(dict(row))
            continue

        tgt_sym = row.get("target_symbol") or ""
        tgt_market = row.get("target_market") or ""
        if tgt_sym not in review_price_cache:
            review_price_cache[tgt_sym] = _fetch_review_price(tgt_sym, tgt_market)
        review_price = review_price_cache[tgt_sym]

        if review_price is None:
            no_price.append(row)
            continue

        direction = (row.get("direction") or "").lower()
        relation_type = (row.get("relation_type") or "").upper()

        if direction == "beneficiary" or relation_type in ("UP_LEADS_UP", "DOWN_LEADS_UP"):
            outcome_ret = (review_price - signal_price) / signal_price * 100
            hit_val = 1 if review_price > signal_price else 0
        else:
            outcome_ret = (signal_price - review_price) / signal_price * 100
            hit_val = 1 if review_price < signal_price else 0

        with contextlib.suppress(Exception):
            store.update_mover_chain_review(row["id"], review_price, outcome_ret, hit_val)

        evaluable.append(
            {
                **row,
                "target_price_at_review": review_price,
                "outcome_return_pct": outcome_ret,
                "hit": hit_val,
            }
        )

    total = len(rows)
    eval_count = len(evaluable)
    lines.append(f"- 이번 주 후보: {total}개")
    lines.append(f"- 평가 가능: {eval_count}개")

    if evaluable:
        win_list = [e for e in evaluable if e.get("hit")]
        avg_ret = sum((e.get("outcome_return_pct") or 0.0) for e in evaluable) / eval_count
        lines.append(f"- 평균 성과: {avg_ret:+.1f}%")
        lines.append(f"- 적중률: {len(win_list)}/{eval_count}")

        beneficiary_list = [
            e for e in evaluable if (e.get("direction") or "").lower() == "beneficiary"
        ]
        risk_list = [e for e in evaluable if (e.get("direction") or "").lower() == "risk"]
        if beneficiary_list:
            b_hits = sum(1 for e in beneficiary_list if e.get("hit"))
            lines.append(f"- 동행 후보 적중률: {b_hits}/{len(beneficiary_list)}")
        if risk_list:
            r_hits = sum(1 for e in risk_list if e.get("hit"))
            lines.append(f"- 약세 후보 적중률: {r_hits}/{len(risk_list)}")

        hits_sorted = sorted(
            [e for e in evaluable if e.get("hit")],
            key=lambda x: -(x.get("outcome_return_pct") or 0.0),
        )
        if hits_sorted:
            best = hits_sorted[0]
            src = best.get("source_name") or best.get("source_symbol") or "?"
            tgt = best.get("target_name") or best.get("target_symbol") or "?"
            ret = best.get("outcome_return_pct") or 0.0
            lines.append(f"- 가장 잘 맞은 후보: {src} → {tgt} ({ret:+.1f}%)")

        misses = [e for e in evaluable if not e.get("hit")]
        if misses:
            worst = sorted(misses, key=lambda x: x.get("outcome_return_pct") or 0.0)[0]
            src = worst.get("source_name") or worst.get("source_symbol") or "?"
            tgt = worst.get("target_name") or worst.get("target_symbol") or "?"
            ret = worst.get("outcome_return_pct") or 0.0
            lines.append(f"- 빗나간 후보: {src} → {tgt} ({ret:+.1f}%)")

    if no_price:
        lines.append(f"- 가격 확인 불가: {len(no_price)}개")
    if pending:
        lines.append(f"- 평가 대기 (lag_days 미경과): {len(pending)}개")

    if evaluable:
        lines.append("")
        for idx, e in enumerate(evaluable[:5], 1):
            src = e.get("source_name") or e.get("source_symbol") or "?"
            tgt = e.get("target_name") or e.get("target_symbol") or "?"
            direction = (e.get("direction") or "").lower()
            dir_label = (
                "동행 후보"
                if direction == "beneficiary"
                else "약세 전이 후보"
                if direction == "risk"
                else "후보"
            )
            s_price = e.get("target_price_at_signal")
            r_price = e.get("target_price_at_review")
            outcome = e.get("outcome_return_pct") or 0.0
            tgt_market = e.get("target_market") or "KR"
            cond_prob = e.get("conditional_prob")
            lift_val = e.get("lift")
            created_at_str = e.get("created_at")
            if direction == "beneficiary":
                hit_label = "✅ 후행 반응 적중" if e.get("hit") else "❌ 부진"
            elif direction == "risk":
                hit_label = "✅ 약세 전이 적중" if e.get("hit") else "❌ 부진"
            else:
                hit_label = "✅ 적중" if e.get("hit") else "❌ 부진"
            lines.append(f"{idx}. {src} → {tgt}")
            lines.append(f"   - 신호 시점: {_fmt_kst_datetime(created_at_str)}")
            lines.append(f"   - 방향: {dir_label}")
            if cond_prob is not None and lift_val is not None:
                lines.append(f"   - 조건부확률/lift: {cond_prob * 100:.1f}% / {lift_val:.1f}x")
            if s_price is not None:
                lines.append(f"   - 당시 target 기준가: {_fmt_price(s_price, tgt_market)}")
            if r_price is not None:
                lines.append(f"   - 평가 기준가: {_fmt_price(r_price, tgt_market)}")
            if s_price is not None and r_price is not None:
                lines.append(f"   - 보유 가정 기간: {_fmt_hold_period(created_at_str, now)}")
                lines.append(f"   - 가상 성과: {outcome:+.1f}%")
            lines.append(f"   - 결과: {hit_label}")

    lines.append("")
    lines.append(
        "※ 이 평가는 실제 매매 수익이 아니라, 통계적 후행 후보가 사후에 얼마나 맞았는지 점검하는 리서치 성과표입니다."
    )
    return "\n".join(lines)


# Regex to extract "N. Name / SYMBOL" from analysis sections
_SCENARIO_LINE_RE = re.compile(r"^\d+\.\s+\S*\s+(.+?)\s*/\s*(\S+)")
# Regex to extract score from "점수: 82/100" lines
_SCORE_LINE_RE = re.compile(r"점수:\s*(\d+(?:\.\d+)?)\s*/\s*100")

_MACRO_KEYWORDS: list[str] = [
    "FOMC",
    "CPI",
    "PCE",
    "금리",
    "고용",
    "실업수당",
    "환율",
    "유가",
    "관세",
    "지정학",
    "연준",
    "국채",
    "달러",
]

_SECTOR_KEYWORDS: list[str] = [
    "AI",
    "반도체",
    "HBM",
    "바이오",
    "조선",
    "방산",
    "2차전지",
    "금융",
    "자동차",
    "전력",
    "원전",
    "화장품",
    "엔터",
]

_MACRO_GOOD_RE = re.compile(
    r"금리인하|고용호조|경기회복|무역타결|유가하락|정책호재|낙관|완화|피봇|고용증가|환율안정"
)
_MACRO_BAD_RE = re.compile(
    r"금리인상|금리상승|CPI|PCE|관세|지정학|유가급등|실업증가|인플레이션|긴축|경기침체|무역전쟁"
)

_FORBIDDEN_RE = re.compile(r"무조건\s*매수|반드시\s*상승|확정\s*수익|Buy\s*Now", re.IGNORECASE)

# Section headers from build_macro_digest and format_analysis_report
_GOOD_MACRO_HEADERS = {"🌍 좋은 매크로", "🔥 핵심 호재", "🔥 좋은 주식 이슈"}
_BAD_MACRO_HEADERS = {"⚠️ 나쁜 매크로", "⚠️ 주요 리스크", "📉 나쁜 주식 이슈"}
_STRONG_SECTOR_HEADERS = {"📌 강한 섹터", "강한 섹터", "강했던 섹터"}
_WEAK_SECTOR_HEADERS = {"📉 약한 섹터", "약한 섹터", "약했던 섹터"}
_LONG_HEADERS = {"🟢 롱 관심 후보", "롱 관심 후보"}
_SHORT_HEADERS = {"🔴 숏/매도 경계 후보", "숏/매도 경계 후보"}

# Pattern to extract symbol from "N. Name / SYMBOL" in analysis reports
_SCENARIO_RE = re.compile(r"^\d+\.\s+.+?\s*/\s*(\S+)")


@dataclass
class WeeklyInput:
    start_at: datetime
    end_at: datetime
    report_count: int
    digests: list[str] = field(default_factory=list)
    analyses: list[str] = field(default_factory=list)
    top_tickers: dict[str, int] = field(default_factory=dict)
    long_mentions: dict[str, int] = field(default_factory=dict)
    short_mentions: dict[str, int] = field(default_factory=dict)
    macro_keywords: dict[str, int] = field(default_factory=dict)
    sector_keywords: dict[str, int] = field(default_factory=dict)
    ticker_names: dict[str, str] = field(default_factory=dict)
    good_macro_lines: list[str] = field(default_factory=list)
    bad_macro_lines: list[str] = field(default_factory=list)
    strong_sector_lines: list[str] = field(default_factory=list)
    weak_sector_lines: list[str] = field(default_factory=list)
    # Performance review: list of dicts with symbol, name, score, entry_price, current_price, return_pct, win
    performance_entries: list[dict] = field(default_factory=list)


@dataclass
class WeeklySummary:
    title: str
    week_range: str
    report_count: int
    market_summary: str
    good_macro: list[str]
    bad_macro: list[str]
    strong_sectors: list[str]
    weak_sectors: list[str]
    top_long_candidates: list[str]
    top_short_candidates: list[str]
    watchlist_next_week: list[str]
    next_week_scenarios: dict[str, str]
    risk_checkpoints: list[str]


def _header_matches(line: str, headers: set[str]) -> bool:
    return any(h in line for h in headers)


def _parse_sections(
    text: str,
) -> tuple[list[str], list[str], list[str], list[str], dict[str, int], dict[str, int]]:
    """Return (good_macro, bad_macro, strong_sectors, weak_sectors, long_tickers, short_tickers)."""
    good_macro: list[str] = []
    bad_macro: list[str] = []
    strong_sectors: list[str] = []
    weak_sectors: list[str] = []
    long_tickers: dict[str, int] = {}
    short_tickers: dict[str, int] = {}

    SECTION_NONE = 0
    SECTION_GOOD = 1
    SECTION_BAD = 2
    SECTION_STRONG = 3
    SECTION_WEAK = 4
    SECTION_LONG = 5
    SECTION_SHORT = 6

    current = SECTION_NONE
    end_markers = ("출처", "주의", "─", "공개 정보")

    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue

        if any(s.startswith(m) or m in s for m in end_markers):
            current = SECTION_NONE
            continue

        if _header_matches(s, _GOOD_MACRO_HEADERS):
            current = SECTION_GOOD
            continue
        if _header_matches(s, _BAD_MACRO_HEADERS):
            current = SECTION_BAD
            continue
        if _header_matches(s, _STRONG_SECTOR_HEADERS):
            current = SECTION_STRONG
            continue
        if _header_matches(s, _WEAK_SECTOR_HEADERS):
            current = SECTION_WEAK
            continue
        if _header_matches(s, _LONG_HEADERS):
            current = SECTION_LONG
            continue
        if _header_matches(s, _SHORT_HEADERS):
            current = SECTION_SHORT
            continue

        # New section with emoji or number-dot pattern that doesn't match above
        if s and s[0] in "🧠📅📊👀📌" and current not in (SECTION_NONE,):
            current = SECTION_NONE
            continue

        if current == SECTION_GOOD and s.startswith("-"):
            content = s.lstrip("-• ").split("  ")[0][:100]
            if content and content not in good_macro:
                good_macro.append(content)

        elif current == SECTION_BAD and s.startswith("-"):
            content = s.lstrip("-• ").split("  ")[0][:100]
            if content and content not in bad_macro:
                bad_macro.append(content)

        elif current == SECTION_STRONG and s.startswith("-"):
            content = s.lstrip("-• ").strip()[:80]
            if content and content not in strong_sectors:
                strong_sectors.append(content)

        elif current == SECTION_WEAK and s.startswith("-"):
            content = s.lstrip("-• ").strip()[:80]
            if content and content not in weak_sectors:
                weak_sectors.append(content)

        elif current == SECTION_LONG:
            m = _SCENARIO_RE.match(s)
            if m:
                sym = m.group(1).strip()
                long_tickers[sym] = long_tickers.get(sym, 0) + 1

        elif current == SECTION_SHORT:
            m = _SCENARIO_RE.match(s)
            if m:
                sym = m.group(1).strip()
                short_tickers[sym] = short_tickers.get(sym, 0) + 1

    return good_macro, bad_macro, strong_sectors, weak_sectors, long_tickers, short_tickers


def parse_long_candidates_from_analysis(analysis_text: str, min_score: float = 80.0) -> list[dict]:
    """analysis_text에서 LONG ≥80 후보를 파싱해 반환한다.

    Returns list of dicts: {symbol, name, score, created_at=None}
    """
    results: list[dict] = []
    in_long_section = False

    for line in analysis_text.splitlines():
        s = line.strip()
        if not s:
            continue

        # 롱 섹션 진입/이탈 감지
        if _header_matches(s, _LONG_HEADERS):
            in_long_section = True
            continue
        if _header_matches(s, _SHORT_HEADERS) or _header_matches(s, {"🟡 관망/추적", "관망/추적"}):
            in_long_section = False
            continue
        # 다른 섹션 헤더 → 롱 섹션 종료
        if s and s[0] in "📊🔴🟡🚫─" and in_long_section:
            in_long_section = False
            continue

        if not in_long_section:
            continue

        # "N. Name / SYMBOL" 라인 추출
        m = _SCENARIO_LINE_RE.match(s)
        if not m:
            m2 = _SCENARIO_RE.match(s)
            if m2:
                sym = m2.group(1).strip()
                # 이름을 얻기 위해 name 파싱 시도
                parts = s.split("/")
                name = parts[0].lstrip("0123456789. ⭐").strip() if len(parts) >= 2 else sym
                results.append({"symbol": sym, "name": name, "score": 0.0, "created_at": None})
            continue

        name_part = m.group(1).strip().lstrip("⭐ ")
        sym = m.group(2).strip()
        results.append({"symbol": sym, "name": name_part, "score": 0.0, "created_at": None})

    # 점수 후처리: "점수: 82/100" 라인에서 추출 (analysis_text 전체 scan)
    score_map: dict[str, float] = {}
    current_sym: str | None = None
    for line in analysis_text.splitlines():
        s = line.strip()
        m = _SCENARIO_RE.match(s)
        if m:
            current_sym = m.group(1).strip()
        elif current_sym:
            sm = _SCORE_LINE_RE.search(s)
            if sm:
                score_map[current_sym] = float(sm.group(1))
                current_sym = None

    for entry in results:
        if entry["symbol"] in score_map:
            entry["score"] = score_map[entry["symbol"]]

    return [e for e in results if e["score"] >= min_score or e["score"] == 0.0]


def build_weekly_input(
    reports: list[RunReport], performance_entries: list[dict] | None = None
) -> WeeklyInput:
    if not reports:
        now = datetime.now(UTC)
        return WeeklyInput(
            start_at=now, end_at=now, report_count=0, performance_entries=performance_entries or []
        )

    start_at = min(r.created_at for r in reports)
    end_at = max(r.created_at for r in reports)

    try:
        from tele_quant.analysis.aliases import load_alias_config

        book = load_alias_config()
    except Exception:
        book = None

    top_tickers: dict[str, int] = {}
    long_mentions: dict[str, int] = {}
    short_mentions: dict[str, int] = {}
    macro_keywords: dict[str, int] = {}
    sector_keywords: dict[str, int] = {}
    ticker_names: dict[str, str] = {}
    all_good_macro: list[str] = []
    all_bad_macro: list[str] = []
    all_strong_sectors: list[str] = []
    all_weak_sectors: list[str] = []
    digests: list[str] = []
    analyses: list[str] = []

    for report in reports:
        combined = report.digest
        if report.analysis:
            combined += "\n" + report.analysis
            analyses.append(report.analysis)
        digests.append(report.digest)

        # Count macro keywords
        for kw in _MACRO_KEYWORDS:
            cnt = combined.count(kw)
            if cnt:
                macro_keywords[kw] = macro_keywords.get(kw, 0) + cnt

        # Count sector keywords
        for kw in _SECTOR_KEYWORDS:
            cnt = combined.count(kw)
            if cnt:
                sector_keywords[kw] = sector_keywords.get(kw, 0) + cnt

        # Extract tickers via AliasBook
        if book:
            for m in book.match_symbols(combined):
                top_tickers[m.symbol] = top_tickers.get(m.symbol, 0) + m.mentions
                if m.symbol not in ticker_names:
                    ticker_names[m.symbol] = m.name

        # Parse sections for good/bad macro and long/short candidates
        good, bad, strong, weak, longs, shorts = _parse_sections(combined)

        for item in good:
            if item not in all_good_macro:
                all_good_macro.append(item)
        for item in bad:
            if item not in all_bad_macro:
                all_bad_macro.append(item)
        for item in strong:
            if item not in all_strong_sectors:
                all_strong_sectors.append(item)
        for item in weak:
            if item not in all_weak_sectors:
                all_weak_sectors.append(item)

        for sym, cnt in longs.items():
            long_mentions[sym] = long_mentions.get(sym, 0) + cnt
        for sym, cnt in shorts.items():
            short_mentions[sym] = short_mentions.get(sym, 0) + cnt

    return WeeklyInput(
        start_at=start_at,
        end_at=end_at,
        report_count=len(reports),
        digests=digests,
        analyses=analyses,
        top_tickers=top_tickers,
        long_mentions=long_mentions,
        short_mentions=short_mentions,
        macro_keywords=macro_keywords,
        sector_keywords=sector_keywords,
        ticker_names=ticker_names,
        good_macro_lines=all_good_macro[:15],
        bad_macro_lines=all_bad_macro[:15],
        strong_sector_lines=all_strong_sectors[:10],
        weak_sector_lines=all_weak_sectors[:10],
        performance_entries=performance_entries or [],
    )


def build_weekly_deterministic_summary(
    weekly_input: WeeklyInput,
    relation_feed_data: Any = None,
    relation_signal_review: str | None = None,
    pair_watch_review: str | None = None,
) -> str:
    wi = weekly_input

    if wi.report_count == 0:
        return "📅 Tele Quant 주간 총정리\n\n최근 리포트가 없어 주간 요약을 생성할 수 없습니다."

    start_str = wi.start_at.strftime("%Y-%m-%d")
    end_str = wi.end_at.strftime("%Y-%m-%d")

    lines: list[str] = [
        "📅 Tele Quant 주간 총정리",
        "",
        "기간:",
        f"- {start_str} ~ {end_str}",
        f"- 누적 리포트: {wi.report_count}개",
        "",
    ]

    # 1. 이번 주 시장 한 줄
    good_cnt = len(wi.good_macro_lines)
    bad_cnt = len(wi.bad_macro_lines)
    long_cnt = len(wi.long_mentions)
    short_cnt = len(wi.short_mentions)

    if good_cnt > bad_cnt * 2 and long_cnt >= short_cnt:
        conclusion = "호재 우세 흐름. 롱 관심 후보 중심 선별 접근 권장."
    elif bad_cnt > good_cnt * 2 or short_cnt > long_cnt * 2:
        conclusion = "악재·불확실성 우세. 비중 축소·관망 시나리오 우선 검토."
    elif wi.macro_keywords:
        top_kw = max(wi.macro_keywords, key=lambda k: wi.macro_keywords[k])
        conclusion = f"매크로 변수({top_kw}) 집중. 방향성 확인 후 선별 접근."
    else:
        conclusion = "호재·악재 혼재. 선별 접근 권장, 무리한 방향 베팅 자제."

    lines += ["1. 이번 주 시장 한 줄", f"- {conclusion}", ""]

    # 2. 매크로 요약 (금리/환율/유가/고용/정책)
    lines.append("2. 매크로 요약")
    if wi.good_macro_lines:
        lines.append("▸ 호재")
        for item in wi.good_macro_lines[:4]:
            lines.append(f"  - {item}")
    if wi.bad_macro_lines:
        lines.append("▸ 악재·리스크")
        for item in wi.bad_macro_lines[:4]:
            lines.append(f"  - {item}")
    if not wi.good_macro_lines and not wi.bad_macro_lines:
        lines.append("- 이번 주 주요 매크로 신호 미확인")
    lines.append("")

    # 3. 강한 섹터
    lines.append("3. 강한 섹터")
    if wi.strong_sector_lines:
        for item in wi.strong_sector_lines[:5]:
            lines.append(f"- {item}")
    elif wi.sector_keywords:
        top_sectors = sorted(wi.sector_keywords.items(), key=lambda x: -x[1])[:4]
        for kw, cnt in top_sectors:
            lines.append(f"- {kw}: 주간 {cnt}회 언급")
    else:
        lines.append("- 집중 강세 섹터 미확인")
    lines.append("")

    # 4. 약한 섹터
    lines.append("4. 약한 섹터")
    if wi.weak_sector_lines:
        for item in wi.weak_sector_lines[:5]:
            lines.append(f"- {item}")
    else:
        lines.append("- 집중 약세 섹터 미확인")
    lines.append("")

    # 5. 반복 종목 (롱 + 숏 통합)
    lines.append("5. 반복 언급 종목")
    has_repeats = False
    if wi.long_mentions:
        has_repeats = True
        lines.append("▸ 롱 관심")
        top_longs = sorted(wi.long_mentions.items(), key=lambda x: -x[1])[:5]
        for sym, cnt in top_longs:
            name = wi.ticker_names.get(sym, sym)
            display = f"{name} / {sym}" if name != sym else sym
            lines.append(f"  - {display}: {cnt}회")
    if wi.short_mentions:
        has_repeats = True
        lines.append("▸ 숏/매도 경계")
        top_shorts = sorted(wi.short_mentions.items(), key=lambda x: -x[1])[:5]
        for sym, cnt in top_shorts:
            name = wi.ticker_names.get(sym, sym)
            display = f"{name} / {sym}" if name != sym else sym
            lines.append(f"  - {display}: {cnt}회")
    if not has_repeats and wi.top_tickers:
        top = sorted(wi.top_tickers.items(), key=lambda x: -x[1])[:5]
        for sym, cnt in top:
            name = wi.ticker_names.get(sym, sym)
            display = f"{name} / {sym}" if name != sym else sym
            lines.append(f"- {display}: 주간 {cnt}회 언급")
    elif not has_repeats:
        lines.append("- 반복 언급 종목 없음")
    lines.append("")

    # 6. 📈 성과 리뷰: 80점 이상 첫 추천 기준 가상 수익률
    lines.append("6. 📈 성과 리뷰: 80점 이상 첫 추천 기준 가상 수익률")
    lines.append(
        '- 평가 기준: 이번 주 4시간 리포트에서 처음으로 LONG 점수 80점 이상이 된 시점을 "첫 추천 시점"으로 기록'
    )
    lines.append("- 진입가 기준: 첫 추천 리포트 생성 시점의 종가/확인 가능한 최신 가격")
    lines.append("- 평가가 기준: 주간 리포트 생성 시점에 확인 가능한 최신 가격")
    lines.append("- 주의: 실제 매매 체결가가 아니라 리서치 시스템의 사후 검증용 가상 수익률")
    perf = wi.performance_entries
    now_utc = datetime.now(UTC)
    if perf:
        wins = [e for e in perf if e.get("win")]
        eval_count = len([e for e in perf if e.get("entry_price") and e.get("current_price")])
        avg_ret = sum(e.get("return_pct", 0) for e in perf) / len(perf)
        best_entry = max(perf, key=lambda x: x.get("return_pct", 0))
        worst_entry = min(perf, key=lambda x: x.get("return_pct", 0))
        best_ret = best_entry.get("return_pct", 0)
        worst_ret = worst_entry.get("return_pct", 0)

        hold_days_list: list[float] = []
        for _e in perf:
            _cat = _e.get("created_at")
            if _cat:
                try:
                    _fdt = datetime.fromisoformat(str(_cat)) if isinstance(_cat, str) else _cat
                    if _fdt.tzinfo is None:
                        _fdt = _fdt.replace(tzinfo=UTC)
                    _delta_h = (now_utc - _fdt).total_seconds() / 3600
                    if _delta_h > 0:
                        hold_days_list.append(_delta_h / 24)
                except Exception:
                    pass

        all_fallback = all(e.get("_source") == "fallback" for e in perf)
        any_fallback = any(e.get("_source") == "fallback" for e in perf)
        if all_fallback:
            source_label = "analysis_text fallback"
        elif any_fallback:
            source_label = "scenario_history (일부 analysis_text fallback)"
        else:
            source_label = "scenario_history"

        no_price_list = [e for e in perf if not e.get("entry_price")]
        lines.append("")
        lines.append("📊 80점 이상 첫 추천 성과 요약")
        lines.append(f"- 첫 추천 후보 수: {len(perf)}개")
        lines.append(f"- 평가 가능: {eval_count}개")
        if hold_days_list:
            avg_hold = sum(hold_days_list) / len(hold_days_list)
            lines.append(f"- 평균 보유 가정 기간: {avg_hold:.1f}일")
        lines.append(f"- 평균 가상 수익률: {avg_ret:+.1f}%")
        lines.append(f"- 승률: {len(wins)}/{len(perf)} ({len(wins) / len(perf) * 100:.0f}%)")
        best_name = best_entry.get("name") or best_entry.get("symbol", "?")
        worst_name = worst_entry.get("name") or worst_entry.get("symbol", "?")
        lines.append(f"- 최고: {best_name} {best_ret:+.1f}%")
        lines.append(f"- 최악: {worst_name} {worst_ret:+.1f}%")
        lines.append(f"- 저장/파싱 방식: {source_label}")
        if no_price_list:
            lines.append(f"- 가격 확인 불가: {len(no_price_list)}개 제외")

        lines.append("")
        lines.append("종목별:")
        for idx, e in enumerate(perf[:8], 1):
            sym = e.get("symbol", "?")
            name = e.get("name") or sym
            score = e.get("score", 0)
            max_score = e.get("max_score")
            max_score_at = e.get("max_score_at")
            entry = e.get("entry_price")
            current = e.get("current_price")
            ret_pct = e.get("return_pct", 0)
            first_rec = e.get("created_at")
            market = e.get("market") or ("KR" if (sym or "").endswith((".KS", ".KQ")) else "US")
            source_tag = (
                "analysis_text fallback" if e.get("_source") == "fallback" else "scenario_history"
            )
            icon = "✅ 상승 적중" if e.get("win") else "❌ 부진"
            hold_str = _fmt_hold_period(first_rec, now_utc)
            lines.append(f"{idx}. {name} / {sym}")
            if first_rec:
                lines.append(f"   - 첫 80점 이상 추천: {_fmt_kst_datetime(first_rec)}")
            lines.append(f"   - 당시 리포트 점수: {score:.0f}점")
            if max_score is not None and float(max_score) > float(score):
                lines.append(
                    f"   - 최고점 참고: {_fmt_kst_datetime(max_score_at)} / {float(max_score):.0f}점"
                )
            if entry:
                lines.append(f"   - 당시 기준가: {_fmt_price(entry, market)}")
            else:
                lines.append("   - 당시 기준가: 가격 기준 확인 필요")
            if current:
                lines.append(f"   - 평가 기준가: {_fmt_price(current, market)}")
            if entry and current:
                lines.append(f"   - 보유 가정 기간: {hold_str}")
                lines.append(f"   - 가상 수익률: {ret_pct:+.1f}%")
            lines.append(f"   - 결과: {icon}")
            lines.append(f"   - 저장/파싱 방식: {source_tag}")
    else:
        lines.append("- 이번 주 LONG ≥80 점 성과 데이터 없음")
        if not wi.performance_entries:
            diag = getattr(wi, "_perf_diag", None)
            if diag:
                for d in diag:
                    lines.append(f"  진단: {d}")
            else:
                lines.append(
                    "  진단: scenario_history 미저장, 가격 확인 실패, 또는 80점 이상 후보 없음"
                )
    lines.append("")

    # 7. 숏 사후 점검
    lines.append("7. 숏 사후 점검")
    if wi.short_mentions:
        top_shorts = sorted(wi.short_mentions.items(), key=lambda x: -x[1])[:5]
        lines.append(f"- 주간 숏/매도 경계 후보: {len(top_shorts)}종목 반복 언급")
        for sym, cnt in top_shorts:
            name = wi.ticker_names.get(sym, sym)
            display = f"{name}/{sym}" if name != sym else sym
            lines.append(f"  - {display} ({cnt}회 언급) — 현재 가격 확인 필요")
    else:
        lines.append("- 이번 주 숏 후보 반복 언급 없음")
    lines.append("")

    # 8. 다음 주 시나리오
    lines.append("8. 다음 주 시나리오")

    top_macro_kws = sorted(wi.macro_keywords.items(), key=lambda x: -x[1])[:3]
    macro_str = ", ".join(k for k, _ in top_macro_kws) if top_macro_kws else "매크로 변수"
    top_sector_kws = sorted(wi.sector_keywords.items(), key=lambda x: -x[1])[:2]
    sector_str = "/".join(k for k, _ in top_sector_kws) if top_sector_kws else "주요 섹터"

    lines.append(
        f"- 강세 시나리오: {macro_str} 호전 + {sector_str} 수요 지속 시 위험자산 선호 회복 가능"
    )
    lines.append(f"- 약세 시나리오: {macro_str} 재악화 + 달러 강세 시 성장주·신흥국 부담 확대")
    lines.append("- 중립/관망 시나리오: 주요 지표 발표 전 관망 우세, 방향성 확인 후 진입 검토")
    lines.append("")

    # 9. 체크포인트
    lines.append("9. 다음 주 체크포인트")
    checkpoints: list[str] = []
    for kw, label_kr in [
        ("FOMC", "FOMC 일정 및 의사록"),
        ("CPI", "CPI 발표"),
        ("PCE", "PCE 발표"),
        ("고용", "고용 지표 (실업률/비농업)"),
        ("환율", "환율 동향 (원/달러)"),
        ("유가", "유가 흐름 (WTI/브렌트)"),
        ("관세", "관세·무역 이슈"),
        ("지정학", "지정학적 리스크"),
        ("실업수당", "주간 실업수당 청구건수"),
        ("국채", "국채 금리 동향"),
    ]:
        if kw in wi.macro_keywords:
            checkpoints.append(label_kr)

    if not checkpoints:
        checkpoints = ["FOMC 일정", "CPI 발표", "고용 지표", "환율 동향", "유가 흐름"]

    for cp in checkpoints[:6]:
        lines.append(f"- {cp}")
    lines.append("")

    # 10. 급등·급락 후행 후보 리뷰
    lines.append("10. 📈 급등·급락 후행 후보 리뷰")
    if relation_feed_data is not None:
        try:
            from tele_quant.relation_feed import RelationFeedData

            if isinstance(relation_feed_data, RelationFeedData) and relation_feed_data.available:
                feed = relation_feed_data
                summary = feed.summary
                assert summary is not None
                lines.append(f"- 이번 주 relation feed 기준일: {summary.asof_date}")
                lines.append(
                    f"- 신호: mover {len(feed.movers)}개 / lead-lag 후보 {len(feed.leadlag)}개"
                )
                if feed.leadlag:
                    conf_counts: dict[str, int] = {}
                    for r in feed.leadlag:
                        conf_counts[r.confidence] = conf_counts.get(r.confidence, 0) + 1
                    conf_str = ", ".join(f"{k}={v}" for k, v in sorted(conf_counts.items()))
                    lines.append(f"- confidence 분포: {conf_str}")
                    src_syms = list(dict.fromkeys(r.source_symbol for r in feed.leadlag))[:4]
                    lines.append(f"- 주요 source: {', '.join(src_syms)}")
                    tgt_syms = list(dict.fromkeys(r.target_symbol for r in feed.leadlag))[:4]
                    lines.append(f"- 주요 target: {', '.join(tgt_syms)}")
                    # 다음 주 반복 관찰 후보: target symbols unique list
                    repeat_tgts = list(dict.fromkeys(r.target_symbol for r in feed.leadlag))[:5]
                    lines.append(f"- 다음 주 반복 관찰 후보: {', '.join(repeat_tgts)}")
                if summary.warnings:
                    lines.append(f"- 데이터 주의: {', '.join(summary.warnings)}")
                lines.append("- 통계적 후행 관찰 후보이며 실제 수익 보장 아님")
            else:
                lines.append("- relation feed 없음 또는 로드 실패")
        except Exception as _wrf_exc:
            lines.append(f"- relation feed 로드 오류: {_wrf_exc}")
    else:
        lines.append("- relation feed 미제공 (--no-send 모드 또는 비활성화)")

    if relation_signal_review:
        lines.append("")
        lines.append(relation_signal_review)
    lines.append("")

    # 11. 선행·후행 페어 관찰 성과 (pair watch weekly review)
    if pair_watch_review:
        lines.append("11. " + pair_watch_review)
        lines.append("")

    lines += [
        "─" * 30,
        "공개 정보 기반 개인 리서치 보조용이며 투자 판단 책임은 사용자에게 있음.",
    ]

    result = "\n".join(lines).strip()
    # Safety: strip any forbidden expressions
    result = _FORBIDDEN_RE.sub("", result)
    return result
