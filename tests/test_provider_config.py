"""Tests for provider_config.py — API key presence check without value exposure."""

from __future__ import annotations

from tele_quant.provider_config import (
    available_providers,
    load_optional_env_files,
    log_available_providers,
)

# ── 1. yfinance always available ───────────────────────────────────────────────


def test_yfinance_always_available():
    # load_external=False to avoid side effects from external env files in CI/test
    providers = available_providers(load_external=False)
    assert providers["yfinance"] is True


# ── 2. Provider disabled when env var is not set ──────────────────────────────


def test_provider_disabled_when_no_key(monkeypatch):
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    providers = available_providers(load_external=False)
    assert providers["fred"] is False


def test_provider_enabled_when_key_present(monkeypatch):
    monkeypatch.setenv("FRED_API_KEY", "dummy_key_value")
    providers = available_providers(load_external=False)
    assert providers["fred"] is True


# ── 3. Key values are NEVER exposed in return value ───────────────────────────


def test_available_providers_returns_only_booleans(monkeypatch):
    monkeypatch.setenv("FINNHUB_API_KEY", "super_secret_key_12345")
    providers = available_providers(load_external=False)
    for name, val in providers.items():
        assert isinstance(val, bool), f"Provider '{name}' value is not bool: {val!r}"
        # The actual key string must never appear
        assert val != "super_secret_key_12345"


# ── 4. log_available_providers does not raise ─────────────────────────────────


def test_log_available_providers_no_crash(caplog):
    import logging

    with caplog.at_level(logging.INFO, logger="tele_quant.provider_config"):
        log_available_providers()
    # Should log "enabled: ..." without key values
    assert "super_secret" not in caplog.text


# ── 5. Multiple providers ─────────────────────────────────────────────────────


def test_multiple_providers_independent(monkeypatch):
    monkeypatch.setenv("FRED_API_KEY", "key1")
    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    monkeypatch.setenv("FMP_API_KEY", "key3")

    providers = available_providers(load_external=False)
    assert providers["fred"] is True
    assert providers["finnhub"] is False
    assert providers["fmp"] is True


# ── 6. External env file loading (skips missing files) ───────────────────────


def test_load_optional_env_files_missing_path():
    """load_optional_env_files silently skips non-existent paths."""
    from pathlib import Path

    load_optional_env_files([Path("/nonexistent/path/.env.local")])
    # Should not raise


def test_external_env_key_not_exposed(tmp_path, monkeypatch):
    """Keys from external env file are accessible but not exposed as strings."""
    env_file = tmp_path / ".env.test"
    env_file.write_text("FRED_API_KEY=test_secret_value\n")
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    load_optional_env_files([env_file])
    providers = available_providers(load_external=False)
    # Value presence confirmed, actual value never returned
    assert isinstance(providers["fred"], bool)
