from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from tele_quant.watchlist import (
    WatchlistConfig,
    group_for_symbol,
    is_avoid_symbol,
    is_watchlist_symbol,
    load_watchlist,
    preferred_sector_bonus,
    report_focus_for_hour,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def watchlist_yml(tmp_path: Path) -> Path:
    content = textwrap.dedent(
        """\
        version: 1
        groups:
          core_kr:
            label: "국내 핵심"
            description: "대형주"
            symbols:
              - "005930.KS"
              - "000660.KS"
          us_ai:
            label: "미국 AI"
            description: "AI/빅테크"
            symbols:
              - "NVDA"
              - "MSFT"
          avoid:
            label: "제외"
            description: ""
            symbols:
              - "BADCO"

        risk_profile:
          prefer_sectors:
            - "AI"
            - "반도체"
          avoid_themes:
            - "리딩방"
          max_candidates_per_report: 8
          show_watchlist_first: true

        schedule_context:
          "07":
            label: "아침 브리핑"
            focus:
              - "전일 미국장"
              - "한국장 개장 전"
          "23":
            label: "미국장 개장"
            focus:
              - "빅테크"
              - "ETF"

        disclaimer:
          text: "개인 리서치 보조용"
        """
    )
    p = tmp_path / "watchlist.yml"
    p.write_text(content, encoding="utf-8")
    return p


@pytest.fixture()
def cfg(watchlist_yml: Path) -> WatchlistConfig:
    result = load_watchlist(watchlist_yml)
    assert result is not None
    return result


# ---------------------------------------------------------------------------
# Load tests
# ---------------------------------------------------------------------------


def test_load_watchlist_returns_config(cfg: WatchlistConfig) -> None:
    assert len(cfg.groups) == 3


def test_load_watchlist_nonexistent(tmp_path: Path) -> None:
    result = load_watchlist(tmp_path / "not_exist.yml")
    assert result is None


def test_groups_parsed(cfg: WatchlistConfig) -> None:
    assert "core_kr" in cfg.groups
    assert "us_ai" in cfg.groups
    assert "avoid" in cfg.groups


def test_symbols_loaded(cfg: WatchlistConfig) -> None:
    assert "005930.KS" in cfg.groups["core_kr"].symbols
    assert "NVDA" in cfg.groups["us_ai"].symbols


def test_avoid_group_loaded(cfg: WatchlistConfig) -> None:
    assert "BADCO" in cfg.groups["avoid"].symbols


def test_prefer_sectors(cfg: WatchlistConfig) -> None:
    assert "AI" in cfg.prefer_sectors
    assert "반도체" in cfg.prefer_sectors


def test_schedule_context(cfg: WatchlistConfig) -> None:
    assert "07" in cfg.schedule_context
    assert "23" in cfg.schedule_context
    assert "전일 미국장" in cfg.schedule_context["07"]["focus"]


def test_disclaimer(cfg: WatchlistConfig) -> None:
    assert "개인 리서치" in cfg.disclaimer


# ---------------------------------------------------------------------------
# group_for_symbol / is_watchlist / is_avoid tests
# ---------------------------------------------------------------------------


def test_group_for_symbol_core(cfg: WatchlistConfig) -> None:
    assert group_for_symbol("005930.KS", cfg) == "core_kr"


def test_group_for_symbol_us(cfg: WatchlistConfig) -> None:
    assert group_for_symbol("NVDA", cfg) == "us_ai"


def test_group_for_symbol_avoid(cfg: WatchlistConfig) -> None:
    assert group_for_symbol("BADCO", cfg) == "avoid"


def test_group_for_symbol_unknown(cfg: WatchlistConfig) -> None:
    assert group_for_symbol("UNKNOWN", cfg) is None


def test_is_watchlist_symbol_true(cfg: WatchlistConfig) -> None:
    assert is_watchlist_symbol("NVDA", cfg) is True
    assert is_watchlist_symbol("005930.KS", cfg) is True


def test_is_watchlist_symbol_avoid_false(cfg: WatchlistConfig) -> None:
    assert is_watchlist_symbol("BADCO", cfg) is False


def test_is_watchlist_symbol_unknown_false(cfg: WatchlistConfig) -> None:
    assert is_watchlist_symbol("UNKNOWN", cfg) is False


def test_is_avoid_symbol_true(cfg: WatchlistConfig) -> None:
    assert is_avoid_symbol("BADCO", cfg) is True


def test_is_avoid_symbol_false_for_watchlist(cfg: WatchlistConfig) -> None:
    assert is_avoid_symbol("NVDA", cfg) is False


# ---------------------------------------------------------------------------
# preferred_sector_bonus
# ---------------------------------------------------------------------------


def test_preferred_sector_bonus_match(cfg: WatchlistConfig) -> None:
    assert preferred_sector_bonus("AI", cfg) > 0
    assert preferred_sector_bonus("반도체", cfg) > 0


def test_preferred_sector_bonus_no_match(cfg: WatchlistConfig) -> None:
    assert preferred_sector_bonus("부동산", cfg) == 0.0


# ---------------------------------------------------------------------------
# report_focus_for_hour
# ---------------------------------------------------------------------------


def test_report_focus_exact_match(cfg: WatchlistConfig) -> None:
    focus = report_focus_for_hour(7, cfg)
    assert "전일 미국장" in focus.get("focus", [])


def test_report_focus_closest(cfg: WatchlistConfig) -> None:
    # hour=8 → closest is 7
    focus = report_focus_for_hour(8, cfg)
    assert focus  # returns some dict


def test_report_focus_23(cfg: WatchlistConfig) -> None:
    focus = report_focus_for_hour(23, cfg)
    assert "빅테크" in focus.get("focus", [])


# ---------------------------------------------------------------------------
# watchlist_symbols / avoid_symbols
# ---------------------------------------------------------------------------


def test_watchlist_symbols_excludes_avoid(cfg: WatchlistConfig) -> None:
    wl = cfg.watchlist_symbols()
    assert "NVDA" in wl
    assert "BADCO" not in wl


def test_avoid_symbols(cfg: WatchlistConfig) -> None:
    avoids = cfg.avoid_symbols()
    assert "BADCO" in avoids


def test_all_symbols_includes_avoid(cfg: WatchlistConfig) -> None:
    all_syms = cfg.all_symbols()
    assert "BADCO" in all_syms
    assert "NVDA" in all_syms
