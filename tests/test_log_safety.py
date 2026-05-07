from __future__ import annotations

import logging

from tele_quant.textutil import mask_url_secrets


def _collect_logs(func, *args, **kwargs) -> list[str]:
    """Run func and collect all log messages."""
    records: list[str] = []

    class CaptureHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(self.format(record))

    handler = CaptureHandler()
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        func(*args, **kwargs)
    except Exception:
        pass
    finally:
        root.removeHandler(handler)
    return records


def test_mask_url_secrets_no_token_in_output():
    url = "https://finnhub.io/api/v1/company-news?symbol=AAPL&token=SECRETTOKEN123"
    masked = mask_url_secrets(url)
    assert "token=" in masked
    assert "SECRETTOKEN123" not in masked


def test_mask_url_secrets_no_api_key_in_output():
    url = "https://api.stlouisfed.org/fred?api_key=SECRETKEY456&series_id=FEDFUNDS"
    masked = mask_url_secrets(url)
    assert "SECRETKEY456" not in masked


def test_httpx_logger_level():
    """httpx logger must be WARNING or higher after configure_logging is called."""
    from tele_quant.logging import configure_logging

    configure_logging("INFO")
    httpx_logger = logging.getLogger("httpx")
    assert httpx_logger.level >= logging.WARNING


def test_httpcore_logger_level():
    from tele_quant.logging import configure_logging

    configure_logging("INFO")
    httpcore_logger = logging.getLogger("httpcore")
    assert httpcore_logger.level >= logging.WARNING


def test_no_dummy_secret_in_masked_url():
    dummy_secret = "MY_SUPER_SECRET_KEY_XYZ"
    url = f"https://api.example.com/data?api_key={dummy_secret}&q=hello"
    masked = mask_url_secrets(url)
    assert dummy_secret not in masked
    assert "q=hello" in masked
