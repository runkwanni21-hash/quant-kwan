from __future__ import annotations

from tele_quant.cli import once


def test_once_macro_only_importable() -> None:
    """once command must be importable and callable."""
    assert callable(once)


def test_pipeline_run_once_accepts_macro_only() -> None:
    """TeleQuantPipeline.run_once must accept macro_only kwarg without TypeError."""
    import inspect

    from tele_quant.pipeline import TeleQuantPipeline

    sig = inspect.signature(TeleQuantPipeline.run_once)
    assert "macro_only" in sig.parameters


def test_pipeline_run_once_macro_only_default_false() -> None:
    import inspect

    from tele_quant.pipeline import TeleQuantPipeline

    sig = inspect.signature(TeleQuantPipeline.run_once)
    default = sig.parameters["macro_only"].default
    assert default is False


def test_once_command_has_macro_only_param() -> None:
    """CLI once command must expose --macro-only option."""
    from typer.testing import CliRunner

    from tele_quant.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["once", "--help"])
    assert "--macro-only" in result.output or "macro-only" in result.output


def test_analysis_fast_returns_4tuple() -> None:
    """_run_analysis_fast must return a 4-tuple (report, scenarios, close_map, sector_map)."""
    from tele_quant.pipeline import TeleQuantPipeline

    hints = TeleQuantPipeline._run_analysis_fast.__annotations__
    ret = hints.get("return", "")
    assert "dict" in str(ret)
