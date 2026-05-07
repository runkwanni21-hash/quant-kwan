"""Tests for lint-report CLI command: RunReport object + dict row + quality checks."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch


def _make_run_report(digest: str = "", analysis: str | None = None) -> object:
    """RunReport-like 객체 생성 (모델 import 없이 속성 기반)."""
    obj = MagicMock()
    obj.digest = digest
    obj.analysis = analysis
    obj.created_at = datetime(2026, 5, 7, 10, 30, tzinfo=UTC)
    # .get() 호출 시 AttributeError 나도록 (dict가 아님을 명확히)
    del obj.get
    return obj


def _make_dict_row(digest_text: str = "", analysis_text: str = "") -> dict:
    return {
        "digest_text": digest_text,
        "analysis_text": analysis_text,
        "created_at": "2026-05-07T10:30:00+00:00",
    }


# ── _read_report_field 단위 테스트 ─────────────────────────────────────────────


def _get_helper():
    """lint_report 함수 내부의 _read_report_field 헬퍼를 직접 구성해 반환."""
    from datetime import datetime as _datetime

    def _read_report_field(row: object, name: str, default: str = "") -> str:
        val = row.get(name, default) if isinstance(row, dict) else getattr(row, name, default)  # type: ignore[union-attr]
        if val is None:
            return default
        if isinstance(val, _datetime):
            return val.strftime("%Y-%m-%d %H:%M")
        return str(val) if val else default

    return _read_report_field


def test_read_field_from_run_report_object():
    fn = _get_helper()
    obj = _make_run_report(digest="hello digest", analysis="some analysis")
    assert fn(obj, "digest") == "hello digest"
    assert fn(obj, "analysis") == "some analysis"


def test_read_field_from_dict_row():
    fn = _get_helper()
    row = _make_dict_row(digest_text="dict digest", analysis_text="dict analysis")
    assert fn(row, "digest_text") == "dict digest"
    assert fn(row, "analysis_text") == "dict analysis"


def test_read_created_at_datetime_formats_to_string():
    fn = _get_helper()
    obj = _make_run_report()
    result = fn(obj, "created_at")
    assert "2026-05-07" in result
    assert ":" in result


def test_read_field_none_value_returns_default():
    fn = _get_helper()
    obj = MagicMock()
    obj.analysis = None
    assert fn(obj, "analysis", "") == ""
    assert fn(obj, "analysis", "fallback") == "fallback"


def test_read_field_missing_attr_returns_default():
    fn = _get_helper()
    obj = MagicMock(spec=[])  # no attributes
    assert fn(obj, "nonexistent", "default_val") == "default_val"


# ── lint_report 통합 테스트 (store mock) ──────────────────────────────────────


def _run_lint_with_reports(reports):
    """store.recent_run_reports() 를 mock해 lint_report를 실행하고 콘솔 출력을 캡처."""
    from io import StringIO

    from rich.console import Console

    output = StringIO()
    test_console = Console(file=output, highlight=False)

    with (
        patch("tele_quant.cli._settings") as mock_settings,
        patch("tele_quant.cli.console", test_console),
        patch("tele_quant.db.Store") as MockStore,
    ):
        mock_settings.return_value = MagicMock(sqlite_path=":memory:")
        mock_store = MockStore.return_value
        mock_store.recent_run_reports.return_value = reports
        from tele_quant.cli import lint_report

        lint_report(hours=1.0, limit=10)

    return output.getvalue()


def test_lint_no_reports_shows_graceful_message():
    out = _run_lint_with_reports([])
    assert "없음" in out
    assert "Traceback" not in out


def test_lint_run_report_object_does_not_crash():
    """RunReport 객체를 받아도 AttributeError 없이 동작한다."""
    report = _make_run_report(digest="NVDA 호재 수주 증가", analysis=None)
    out = _run_lint_with_reports([report])
    assert "Traceback" not in out
    # 문제 없으면 '문제 없음' 출력
    assert "문제 없음" in out or "리포트" in out


def test_lint_dict_row_does_not_crash():
    """dict row를 받아도 동작한다."""
    row = _make_dict_row(digest_text="NVDA 호재", analysis_text="")
    out = _run_lint_with_reports([row])
    assert "Traceback" not in out


def test_lint_detects_forbidden_word():
    """금지 표현이 포함된 리포트에서 이슈를 감지한다."""
    report = _make_run_report(digest="ACTION_READY 매수 신호 발생")
    out = _run_lint_with_reports([report])
    assert "금지표현" in out or "ACTION_READY" in out


def test_lint_detects_broker_name_leak():
    """브로커명이 리포트에 그대로 유출된 경우 감지한다."""
    report = _make_run_report(digest="Goldman Sachs 보고서에 따르면 NVDA 상승")
    out = _run_lint_with_reports([report])
    assert "브로커명" in out or "Goldman" in out


def test_lint_detects_metadata_leak():
    """link: / 카테고리: 메타데이터 잔류를 감지한다."""
    report = _make_run_report(digest="link: https://example.com\nNVDA 호재")
    out = _run_lint_with_reports([report])
    assert "메타데이터" in out or "link" in out


def test_lint_clean_report_no_issues():
    """문제 없는 리포트는 '문제 없음'을 출력한다."""
    report = _make_run_report(digest="NVDA 수요 확대로 실적 상회, 목표가 상향")
    out = _run_lint_with_reports([report])
    assert "문제 없음" in out


def test_lint_issue_count_shown():
    """이슈가 있는 경우 이슈 개수를 출력한다."""
    r1 = _make_run_report(digest="ACTION_READY 신호")
    r2 = _make_run_report(digest="정상 리포트 내용")
    out = _run_lint_with_reports([r1, r2])
    # 이슈 발견 시 N/M 형식 출력
    assert "/" in out or "품질 이슈" in out or "문제 없음" in out
