from __future__ import annotations

import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from tele_quant.db import Store


def _make_store() -> tuple[Store, str]:
    tmpdir = tempfile.mkdtemp()
    return Store(Path(tmpdir) / "test.sqlite"), tmpdir


def test_save_and_retrieve_run_report() -> None:
    store, _ = _make_store()
    store.save_run_report(
        digest="테스트 요약",
        analysis="테스트 분석",
        period_hours=4.0,
        mode="fast",
        stats={"kept_items": 42},
    )
    since = datetime.now(UTC) - timedelta(days=1)
    reports = store.recent_run_reports(since=since)
    assert len(reports) == 1
    assert reports[0].digest == "테스트 요약"
    assert reports[0].analysis == "테스트 분석"
    assert reports[0].mode == "fast"
    assert reports[0].period_hours == 4.0
    assert reports[0].stats["kept_items"] == 42


def test_recent_run_reports_7day_filter() -> None:
    store, _ = _make_store()
    store.save_run_report("오래된 요약", None, 4.0, "fast", None)
    # Query from 1 hour in the future → should return nothing
    since = datetime.now(UTC) + timedelta(hours=1)
    reports = store.recent_run_reports(since=since)
    assert len(reports) == 0


def test_analysis_none_stored() -> None:
    store, _ = _make_store()
    store.save_run_report("요약", None, 4.0, "no_llm", None)
    since = datetime.now(UTC) - timedelta(days=1)
    reports = store.recent_run_reports(since=since)
    assert len(reports) == 1
    assert reports[0].analysis is None


def test_multiple_reports_ordered() -> None:
    store, _ = _make_store()
    store.save_run_report("요약A", None, 4.0, "fast", {"n": 1})
    store.save_run_report("요약B", "분석B", 4.0, "no_llm", {"n": 2})
    since = datetime.now(UTC) - timedelta(days=1)
    reports = store.recent_run_reports(since=since)
    assert len(reports) == 2
    # DESC order by created_at
    assert reports[0].digest == "요약B"
    assert reports[1].digest == "요약A"


def test_until_filter() -> None:
    store, _ = _make_store()
    store.save_run_report("요약", None, 4.0, "fast", None)
    since = datetime.now(UTC) - timedelta(days=1)
    until = datetime.now(UTC) - timedelta(hours=1)
    # until is 1 hour ago, record was just inserted → should not appear
    reports = store.recent_run_reports(since=since, until=until)
    assert len(reports) == 0


def test_limit_respected() -> None:
    store, _ = _make_store()
    for i in range(5):
        store.save_run_report(f"요약{i}", None, 4.0, "fast", None)
    since = datetime.now(UTC) - timedelta(days=1)
    reports = store.recent_run_reports(since=since, limit=3)
    assert len(reports) == 3


def test_stats_none_stored() -> None:
    store, _ = _make_store()
    store.save_run_report("요약", None, 4.0, "fast", None)
    since = datetime.now(UTC) - timedelta(days=1)
    reports = store.recent_run_reports(since=since)
    assert reports[0].stats == {}


# --- narrative_history tests ---

def _make_smart_result(macro: str = "테스트 매크로") -> object:
    from tele_quant.analysis.models import SmartReaderResult

    return SmartReaderResult(
        macro_summary=macro,
        key_events=["이벤트1", "이벤트2"],
        bullish_items=[{"name": "삼성전자", "reason": "반도체 수요 증가", "importance": 3}],
        bearish_items=[{"name": "TSLA", "reason": "가격 인하", "importance": 2}],
        risks=["금리 인상 리스크"],
        raw_item_count=100,
        filtered_noise=20,
    )


def test_save_narrative_roundtrip() -> None:
    store, _ = _make_store()
    sr = _make_smart_result()
    store.save_narrative(sr, report_id=None, hours=4.0)
    since = datetime.now(UTC) - timedelta(days=1)
    rows = store.recent_narratives(since=since)
    assert len(rows) == 1
    row = rows[0]
    assert row["macro_summary"] == "테스트 매크로"
    assert row["key_events_json"] == ["이벤트1", "이벤트2"]
    assert len(row["bullish_json"]) == 1
    assert row["bullish_json"][0]["name"] == "삼성전자"
    assert row["raw_item_count"] == 100
    assert row["filtered_noise"] == 20


def test_save_narrative_with_report_id() -> None:
    store, _ = _make_store()
    report_id = store.save_run_report("요약", None, 4.0, "fast", None)
    sr = _make_smart_result("매크로 요약")
    store.save_narrative(sr, report_id=report_id, hours=4.0)
    since = datetime.now(UTC) - timedelta(days=1)
    rows = store.recent_narratives(since=since)
    assert rows[0]["report_id"] == report_id


def test_recent_narratives_time_filter() -> None:
    store, _ = _make_store()
    sr = _make_smart_result()
    store.save_narrative(sr, report_id=None, hours=4.0)
    # Query from future → no results
    since = datetime.now(UTC) + timedelta(hours=1)
    rows = store.recent_narratives(since=since)
    assert rows == []


def test_recent_narratives_limit() -> None:
    store, _ = _make_store()
    for i in range(5):
        sr = _make_smart_result(f"매크로{i}")
        store.save_narrative(sr, report_id=None, hours=4.0)
    since = datetime.now(UTC) - timedelta(days=1)
    rows = store.recent_narratives(since=since, limit=3)
    assert len(rows) == 3


def test_recent_narratives_desc_order() -> None:
    store, _ = _make_store()
    store.save_narrative(_make_smart_result("첫번째"), report_id=None, hours=4.0)
    store.save_narrative(_make_smart_result("두번째"), report_id=None, hours=4.0)
    since = datetime.now(UTC) - timedelta(days=1)
    rows = store.recent_narratives(since=since)
    assert rows[0]["macro_summary"] == "두번째"
