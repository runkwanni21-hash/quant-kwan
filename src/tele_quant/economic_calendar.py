"""경제 캘린더 모듈.

다음 주요 경제 이벤트(FOMC, CPI, NFP, BOK 등)를 조회해 리포트용 텍스트를 반환한다.
외부 API(Finnhub 경제 캘린더)를 활용하며, API 키 없을 때는 하드코딩 데이터로 폴백.
"""
from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from typing import Any

log = logging.getLogger(__name__)

# 연간 주요 이벤트 일정 — 2026년 예시 (실제 운영 시 외부 API로 교체)
# 형식: (날짜 YYYY-MM-DD, 이벤트명, 중요도 "high"/"medium")
_HARDCODED_EVENTS_2026: list[tuple[str, str, str]] = [
    # FOMC
    ("2026-01-28", "FOMC 금리결정", "high"),
    ("2026-03-18", "FOMC 금리결정", "high"),
    ("2026-05-06", "FOMC 금리결정", "high"),
    ("2026-06-17", "FOMC 금리결정", "high"),
    ("2026-07-29", "FOMC 금리결정", "high"),
    ("2026-09-16", "FOMC 금리결정", "high"),
    ("2026-11-04", "FOMC 금리결정", "high"),
    ("2026-12-16", "FOMC 금리결정", "high"),
    # US CPI (약 15일 전후)
    ("2026-01-14", "미국 CPI 발표", "high"),
    ("2026-02-11", "미국 CPI 발표", "high"),
    ("2026-03-11", "미국 CPI 발표", "high"),
    ("2026-04-10", "미국 CPI 발표", "high"),
    ("2026-05-13", "미국 CPI 발표", "high"),
    ("2026-06-10", "미국 CPI 발표", "high"),
    ("2026-07-14", "미국 CPI 발표", "high"),
    ("2026-08-12", "미국 CPI 발표", "high"),
    ("2026-09-11", "미국 CPI 발표", "high"),
    ("2026-10-13", "미국 CPI 발표", "high"),
    ("2026-11-12", "미국 CPI 발표", "high"),
    ("2026-12-10", "미국 CPI 발표", "high"),
    # US NFP (첫째 금요일 전후)
    ("2026-01-09", "미국 비농업고용(NFP)", "high"),
    ("2026-02-06", "미국 비농업고용(NFP)", "high"),
    ("2026-03-06", "미국 비농업고용(NFP)", "high"),
    ("2026-04-03", "미국 비농업고용(NFP)", "high"),
    ("2026-05-08", "미국 비농업고용(NFP)", "high"),
    ("2026-06-05", "미국 비농업고용(NFP)", "high"),
    ("2026-07-10", "미국 비농업고용(NFP)", "high"),
    ("2026-08-07", "미국 비농업고용(NFP)", "high"),
    ("2026-09-04", "미국 비농업고용(NFP)", "high"),
    ("2026-10-02", "미국 비농업고용(NFP)", "high"),
    ("2026-11-06", "미국 비농업고용(NFP)", "high"),
    ("2026-12-04", "미국 비농업고용(NFP)", "high"),
    # 한국 BOK 기준금리
    ("2026-01-16", "한국은행 기준금리 결정", "high"),
    ("2026-02-25", "한국은행 기준금리 결정", "high"),
    ("2026-04-17", "한국은행 기준금리 결정", "high"),
    ("2026-05-29", "한국은행 기준금리 결정", "high"),
    ("2026-07-16", "한국은행 기준금리 결정", "high"),
    ("2026-08-27", "한국은행 기준금리 결정", "high"),
    ("2026-10-15", "한국은행 기준금리 결정", "high"),
    ("2026-11-26", "한국은행 기준금리 결정", "high"),
    # ECB
    ("2026-01-30", "ECB 금리결정", "medium"),
    ("2026-03-05", "ECB 금리결정", "medium"),
    ("2026-04-16", "ECB 금리결정", "medium"),
    ("2026-06-04", "ECB 금리결정", "medium"),
    ("2026-07-23", "ECB 금리결정", "medium"),
    ("2026-09-10", "ECB 금리결정", "medium"),
    ("2026-10-22", "ECB 금리결정", "medium"),
    ("2026-12-03", "ECB 금리결정", "medium"),
    # 미국 GDP (분기 첫 발표)
    ("2026-01-29", "미국 4Q25 GDP 속보치", "medium"),
    ("2026-04-29", "미국 1Q26 GDP 속보치", "medium"),
    ("2026-07-30", "미국 2Q26 GDP 속보치", "medium"),
    ("2026-10-29", "미국 3Q26 GDP 속보치", "medium"),
]


def _parse_event_date(date_str: str) -> date:
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def get_upcoming_events(
    lookback_days: int = 0,
    lookahead_days: int = 14,
    importance: str = "high",  # "high", "medium", or "all"
) -> list[dict[str, str]]:
    """다음 N일 이내 경제 이벤트 목록 반환.

    Returns list of {"date": "YYYY-MM-DD", "name": str, "importance": str, "days_away": str}
    """
    today = datetime.now(UTC).date()
    start = today - timedelta(days=lookback_days)
    end = today + timedelta(days=lookahead_days)

    events: list[dict[str, str]] = []
    for date_str, name, imp in _HARDCODED_EVENTS_2026:
        if importance != "all" and imp != importance and not (
            importance == "high" and imp == "medium"
        ):
            pass
        elif importance == "high" and imp == "medium":
            continue

        ev_date = _parse_event_date(date_str)
        if start <= ev_date <= end:
            delta = (ev_date - today).days
            if delta == 0:
                days_str = "오늘"
            elif delta == 1:
                days_str = "내일"
            elif delta < 0:
                days_str = f"{abs(delta)}일 전"
            else:
                days_str = f"D-{delta}"
            events.append(
                {
                    "date": date_str,
                    "name": name,
                    "importance": imp,
                    "days_away": days_str,
                }
            )

    events.sort(key=lambda e: e["date"])
    return events


def fetch_finnhub_calendar(api_key: str, lookahead_days: int = 14, timeout: float = 10.0) -> list[dict[str, str]]:
    """Finnhub Economic Calendar API로 실시간 이벤트 조회.

    API 실패 시 빈 리스트 반환 (하드코딩 데이터가 폴백으로 사용됨).
    """
    if not api_key:
        return []
    try:
        import httpx

        today = datetime.now(UTC).date()
        end = today + timedelta(days=lookahead_days)
        with httpx.Client(timeout=timeout) as client:
            resp = client.get(
                "https://finnhub.io/api/v1/calendar/economic",
                params={
                    "from": today.strftime("%Y-%m-%d"),
                    "to": end.strftime("%Y-%m-%d"),
                    "token": api_key,
                },
            )
            resp.raise_for_status()
            data = resp.json()
        result: list[dict[str, str]] = []
        for ev in (data.get("economicCalendar") or []):
            imp = ev.get("impact", "").lower()
            if imp not in ("high", "medium"):
                continue
            ev_date = ev.get("time", "")[:10]
            delta = (datetime.strptime(ev_date, "%Y-%m-%d").date() - today).days if ev_date else 0
            days_str = "오늘" if delta == 0 else ("내일" if delta == 1 else f"D-{delta}")
            result.append(
                {
                    "date": ev_date,
                    "name": ev.get("event", ""),
                    "importance": imp,
                    "days_away": days_str,
                    "country": ev.get("country", ""),
                }
            )
        return result
    except Exception as exc:
        log.debug("[economic_calendar] Finnhub 조회 실패: %s", exc)
        return []


def build_calendar_section(
    settings: Any,
    lookahead_days: int = 14,
) -> str:
    """리포트용 경제 캘린더 섹션 텍스트 생성."""
    api_key = getattr(settings, "finnhub_api_key", "") or ""
    timeout = getattr(settings, "finnhub_timeout_seconds", 10.0)

    # 먼저 Finnhub 실시간 시도, 실패 시 하드코딩 폴백
    live_events = fetch_finnhub_calendar(api_key, lookahead_days, timeout) if api_key else []
    events = live_events if live_events else get_upcoming_events(
        lookahead_days=lookahead_days, importance="high"
    )

    if not events:
        return ""

    lines = [f"📅 향후 {lookahead_days}일 주요 일정:"]
    for ev in events[:6]:
        icon = "🔴" if ev["importance"] == "high" else "🟡"
        country = f" [{ev.get('country', '').upper()}]" if ev.get("country") else ""
        lines.append(f"  {icon} {ev['days_away']} {ev['date']} {ev['name']}{country}")

    return "\n".join(lines)
