from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from typer.testing import CliRunner

from tele_quant.cli import app

runner = CliRunner()


def _fake_candidate(sym: str, name: str = "", sector: str = "", origin: str = "직접 언급"):
    c = MagicMock()
    c.symbol = sym
    c.name = name
    c.market = "US"
    c.mentions = 1
    c.sentiment = "neutral"
    c.catalysts = []
    c.risks = []
    c.source_titles = []
    c.sector = sector
    c.origin = origin
    c.correlation_parent = ""
    return c


def _mock_pipeline(cands):
    mock_pipeline = MagicMock()
    mock_pipeline.run_candidates = AsyncMock(return_value=cands)
    return mock_pipeline


def test_candidates_no_expanded_flag():
    """Default --no-expanded shows standard table without origin/sector columns."""
    cand = _fake_candidate("NVDA", "NVIDIA")
    with (
        patch("tele_quant.cli.TeleQuantPipeline", return_value=_mock_pipeline([cand])),
        patch("tele_quant.cli._settings", return_value=MagicMock()),
    ):
        result = runner.invoke(app, ["candidates", "--hours", "1"])

    assert result.exit_code == 0
    assert "NVDA" in result.output


def test_candidates_expanded_flag():
    """--expanded shows extended table with origin/sector/peer columns."""
    cand = _fake_candidate("NVDA", "NVIDIA", sector="빅테크", origin="직접 언급")
    with (
        patch("tele_quant.cli.TeleQuantPipeline", return_value=_mock_pipeline([cand])),
        patch("tele_quant.cli._settings", return_value=MagicMock()),
    ):
        result = runner.invoke(app, ["candidates", "--hours", "1", "--expanded"])

    assert result.exit_code == 0
    assert "NVDA" in result.output
    assert "확장" in result.output or "섹터" in result.output or "출처" in result.output


def test_candidates_empty():
    with (
        patch("tele_quant.cli.TeleQuantPipeline", return_value=_mock_pipeline([])),
        patch("tele_quant.cli._settings", return_value=MagicMock()),
    ):
        result = runner.invoke(app, ["candidates", "--hours", "1"])

    assert result.exit_code == 0
    assert "없습니다" in result.output
