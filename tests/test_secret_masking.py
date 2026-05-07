from __future__ import annotations

from tele_quant.textutil import mask_url_secrets


def test_fred_api_key_masked():
    url = "https://api.stlouisfed.org/fred/series/observations?series_id=FEDFUNDS&api_key=abc123secret&file_type=json"
    masked = mask_url_secrets(url)
    assert "abc123secret" not in masked
    assert "***REDACTED***" in masked
    assert "series_id=FEDFUNDS" in masked


def test_finnhub_token_masked():
    url = "https://finnhub.io/api/v1/company-news?symbol=AAPL&from=2024-01-01&to=2024-01-08&token=mytoken999"
    masked = mask_url_secrets(url)
    assert "mytoken999" not in masked
    assert "***REDACTED***" in masked
    assert "symbol=AAPL" in masked


def test_plain_url_unchanged():
    url = "https://example.com/api/data?page=1&limit=10"
    assert mask_url_secrets(url) == url


def test_no_query_string_unchanged():
    url = "https://example.com/api/data"
    assert mask_url_secrets(url) == url


def test_multiple_secret_params():
    url = "https://api.example.com/v1/foo?api_key=KEY1&token=TOK2&safe_param=hello"
    masked = mask_url_secrets(url)
    assert "KEY1" not in masked
    assert "TOK2" not in masked
    assert "safe_param=hello" in masked


def test_access_token_masked():
    url = "https://api.example.com/data?access_token=supersecret&q=test"
    masked = mask_url_secrets(url)
    assert "supersecret" not in masked
    assert "q=test" in masked


def test_log_string_no_real_key():
    """Simulate what a log message might contain after masking."""
    log_record = f"GET {mask_url_secrets('https://api.stlouisfed.org/fred?api_key=REALKEY123')}"
    assert "REALKEY123" not in log_record
