"""output-lint CLI regression tests."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

FIXTURE = Path(__file__).parent / "fixtures" / "bad_outputs" / "output_quality_bad_examples.txt"
PKG = [sys.executable, "-m", "tele_quant.cli"]  # fallback if uv not in PATH


def _run_lint(extra: list[str] | None = None) -> subprocess.CompletedProcess[str]:
    cmd = ["uv", "run", "tele-quant", "output-lint", "--file", str(FIXTURE)]
    if extra:
        cmd += extra
    return subprocess.run(cmd, capture_output=True, text=True, cwd=Path(__file__).parent.parent)


def test_bad_fixture_has_high_issues():
    """bad fixture는 HIGH 이슈를 1개 이상 포함해야 한다."""
    result = _run_lint()
    assert result.returncode == 0
    combined = result.stdout + result.stderr
    assert "HIGH" in combined, f"HIGH 이슈 미감지:\n{combined}"


def test_bad_fixture_catches_web_balsin():
    result = _run_lint()
    combined = result.stdout + result.stderr
    assert "Web발신" in combined


def test_bad_fixture_catches_report_link():
    result = _run_lint()
    combined = result.stdout + result.stderr
    assert "보고서링크" in combined


def test_bad_fixture_catches_fragment_start():
    """치 후 / 드 플 로 시작하는 조각 문장을 HIGH로 잡아야 함."""
    result = _run_lint()
    combined = result.stdout + result.stderr
    assert "조각 문장" in combined


def test_bad_fixture_catches_bb_price_scale():
    """BB 비정상 가격(삼성전자 311,051 등)을 HIGH로 잡아야 함."""
    result = _run_lint()
    combined = result.stdout + result.stderr
    assert "가격 스케일 이상" in combined


def test_bad_fixture_catches_pair_watch_direction():
    """급등 후(음수) / 급락 후(양수) 방향 오류를 HIGH로 잡아야 함."""
    result = _run_lint()
    combined = result.stdout + result.stderr
    assert "급등 후" in combined or "급락 후" in combined


def test_bad_fixture_catches_short_mae_do():
    """숏/매도 경계 후보 잘못된 표기를 HIGH로 잡아야 함."""
    result = _run_lint()
    combined = result.stdout + result.stderr
    assert "숏/매도 경계 후보" in combined


def test_bad_fixture_catches_price_unavail():
    """현재가 확인 불가 직접 노출을 HIGH로 잡아야 함."""
    result = _run_lint()
    combined = result.stdout + result.stderr
    assert "현재가 확인 불가" in combined


def test_fail_on_high_exits_nonzero():
    """--fail-on-high는 HIGH 발견 시 exit code 1이어야 한다."""
    result = _run_lint(["--fail-on-high"])
    assert result.returncode == 1, f"exit 0 반환됨 — HIGH 미감지:\n{result.stdout}"


def test_clean_output_passes_lint(tmp_path: Path):
    """깨끗한 출력은 output-lint에서 HIGH 0이고 exit 0이어야 한다."""
    clean = tmp_path / "clean.txt"
    clean.write_text(
        "한미약품 / 128940.KS\n"
        "   최종점수: 72.0  (감성 70 / 가치 75 / 4H기술 70 / 3D기술 72) (근거: 직접)\n"
        "   왜 지금: RSI 60 반등 구간, 직접 근거 확인\n"
        "   기준가: 310,000원\n",
        encoding="utf-8",
    )
    cmd = ["uv", "run", "tele-quant", "output-lint", "--file", str(clean), "--fail-on-high"]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path(__file__).parent.parent)
    assert result.returncode == 0, f"깨끗한 출력에서 HIGH 감지됨:\n{result.stdout}"
