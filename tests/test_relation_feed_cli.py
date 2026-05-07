from __future__ import annotations

from typer.testing import CliRunner

from tele_quant.cli import app


def test_relation_feed_command_importable():
    """relation-feed CLI 명령이 등록되어 있음."""
    # Check the command exists in the app
    cmd_names = [c.name for c in app.registered_commands]
    assert "relation-feed" in cmd_names


def test_relation_feed_no_data_does_not_crash(tmp_path):
    """피드 파일이 없어도 relation-feed 명령이 죽지 않음."""
    import os

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["relation-feed"],
        env={
            **os.environ,
            "RELATION_FEED_DIR": str(tmp_path / "nonexistent"),
            "TELEGRAM_API_ID": "12345",
            "TELEGRAM_API_HASH": "abc",
            "TELEGRAM_PHONE": "+821012345678",
            "TELEGRAM_SOURCE_CHATS": "test",
            "TELEGRAM_INCLUDE_ALL_CHANNELS": "false",
        },
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "없음" in result.output or "relation feed" in result.output.lower()


def test_relation_feed_with_real_feed(tmp_path):
    """실제 피드 파일이 있으면 요약 테이블이 출력됨."""
    import os
    from pathlib import Path

    feed_dir = Path("/home/kwanni/projects/quant_spillover/shared_relation_feed")
    if not (feed_dir / "latest_relation_summary.json").exists():
        import pytest

        pytest.skip("shared_relation_feed 없음")

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["relation-feed"],
        env={
            **os.environ,
            "RELATION_FEED_DIR": str(feed_dir),
            "TELEGRAM_API_ID": "12345",
            "TELEGRAM_API_HASH": "abc",
            "TELEGRAM_PHONE": "+821012345678",
            "TELEGRAM_SOURCE_CHATS": "test",
            "TELEGRAM_INCLUDE_ALL_CHANNELS": "false",
        },
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "Relation Feed" in result.output or "relation feed" in result.output.lower()
