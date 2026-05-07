from __future__ import annotations

import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)

# Map of human-readable name → environment variable name
# Values are NEVER read here; only presence is checked.
_PROVIDER_ENV_VARS: dict[str, str] = {
    "fred": "FRED_API_KEY",
    "finnhub": "FINNHUB_API_KEY",
    "fmp": "FMP_API_KEY",
    "alpha_vantage": "ALPHA_VANTAGE_API_KEY",
    "polygon": "POLYGON_API_KEY",
    "newsapi": "NEWSAPI_KEY",
    "tavily": "TAVILY_API_KEY",
    "naver": "NAVER_CLIENT_ID",
}

_DEFAULT_EXTERNAL_PATHS: list[Path] = [
    Path(".env.local"),
    Path("/mnt/c/Users/runkw/Downloads/.env.local"),
]


def load_optional_env_files(paths: list[Path] | None = None) -> None:
    """Load extra .env files without exposing key values.

    Uses python-dotenv if available; skips silently if not.
    Only loads files that actually exist.
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    targets = paths if paths is not None else _DEFAULT_EXTERNAL_PATHS
    for p in targets:
        if p.exists():
            load_dotenv(p, override=False)
            log.debug("[providers] loaded env file: %s", p)


def available_providers(load_external: bool = True) -> dict[str, bool]:
    """Return {provider_name: is_configured}. Never exposes key values."""
    if load_external:
        load_optional_env_files()

    result: dict[str, bool] = {}
    for name, env_var in _PROVIDER_ENV_VARS.items():
        val = os.environ.get(env_var, "").strip()
        result[name] = bool(val)
    # yfinance is always available (no key required)
    result["yfinance"] = True
    return result


def log_available_providers() -> None:
    providers = available_providers()
    enabled = [k for k, v in providers.items() if v]
    disabled = [k for k, v in providers.items() if not v]
    log.info("[providers] enabled: %s", ", ".join(enabled) if enabled else "none")
    if disabled:
        log.debug("[providers] disabled (no key): %s", ", ".join(disabled))
