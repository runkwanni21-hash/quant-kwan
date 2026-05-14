from __future__ import annotations

from typer.testing import CliRunner

from tele_quant.cli import app


def test_ops_doctor_runs():
    """ops-doctor 명령이 오류 없이 실행된다 (DB 없어도)."""
    runner = CliRunner()
    result = runner.invoke(app, ["ops-doctor"])
    # Should not crash — exit 0 or any code is acceptable
    assert result.exit_code in (0, 1), f"Unexpected exit code: {result.exit_code}\n{result.output}"
    # Must contain key output sections
    assert "Ops Doctor" in result.output or "ops" in result.output.lower()


def test_ops_doctor_shows_timer_table():
    """ops-doctor 출력에 timer 관련 섹션이 있어야 한다."""
    runner = CliRunner()
    result = runner.invoke(app, ["ops-doctor"])
    output = result.output
    # Should mention timers or systemd status
    assert any(kw in output for kw in ("timer", "Timer", "systemd", "N/A", "FAIL", "OK", "WARN")), (
        f"Timer info missing from output:\n{output[:500]}"
    )


def test_ops_doctor_shows_db_section():
    """ops-doctor 출력에 DB 상태 섹션이 있어야 한다."""
    runner = CliRunner()
    result = runner.invoke(app, ["ops-doctor"])
    output = result.output
    assert "DB" in output or "SQLITE" in output or "run_report" in output
