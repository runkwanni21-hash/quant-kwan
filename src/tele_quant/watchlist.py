from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class WatchlistGroup:
    key: str
    label: str
    description: str
    symbols: list[str] = field(default_factory=list)


@dataclass
class WatchlistConfig:
    groups: dict[str, WatchlistGroup] = field(default_factory=dict)
    prefer_sectors: list[str] = field(default_factory=list)
    avoid_themes: list[str] = field(default_factory=list)
    schedule_context: dict[str, dict[str, Any]] = field(default_factory=dict)
    disclaimer: str = ""
    max_candidates: int = 8
    show_watchlist_first: bool = True

    def all_symbols(self) -> set[str]:
        result: set[str] = set()
        for grp in self.groups.values():
            result.update(grp.symbols)
        return result

    def watchlist_symbols(self) -> set[str]:
        """All non-avoid symbols."""
        result: set[str] = set()
        for key, grp in self.groups.items():
            if key != "avoid":
                result.update(grp.symbols)
        return result

    def avoid_symbols(self) -> set[str]:
        avoid_grp = self.groups.get("avoid")
        return set(avoid_grp.symbols) if avoid_grp else set()


def load_watchlist(path: str | Path = "config/watchlist.yml") -> WatchlistConfig | None:
    """Load watchlist.yml. Returns None if file not found or parse error."""
    try:
        import yaml
    except ImportError:
        log.warning("[watchlist] pyyaml not installed, watchlist disabled")
        return None

    p = Path(path)
    if not p.exists():
        log.warning("[watchlist] file not found: %s", p)
        return None

    try:
        with open(p, encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except Exception as exc:
        log.warning("[watchlist] parse error: %s", exc)
        return None

    if not isinstance(data, dict):
        return None

    groups: dict[str, WatchlistGroup] = {}
    raw_groups = data.get("groups") or {}
    for key, grp_data in raw_groups.items():
        if not isinstance(grp_data, dict):
            continue
        groups[key] = WatchlistGroup(
            key=key,
            label=grp_data.get("label", key),
            description=grp_data.get("description", ""),
            symbols=[str(s) for s in (grp_data.get("symbols") or [])],
        )

    risk = data.get("risk_profile") or {}
    schedule = data.get("schedule_context") or {}
    disclaimer_data = data.get("disclaimer") or {}
    if isinstance(disclaimer_data, dict):
        disclaimer_text = disclaimer_data.get("text", "")
    else:
        disclaimer_text = str(disclaimer_data)

    return WatchlistConfig(
        groups=groups,
        prefer_sectors=list(risk.get("prefer_sectors") or []),
        avoid_themes=list(risk.get("avoid_themes") or []),
        schedule_context={
            str(k): dict(v) if isinstance(v, dict) else {} for k, v in schedule.items()
        },
        disclaimer=disclaimer_text,
        max_candidates=int(risk.get("max_candidates_per_report", 8)),
        show_watchlist_first=bool(risk.get("show_watchlist_first", True)),
    )


def group_for_symbol(symbol: str, config: WatchlistConfig) -> str | None:
    """Return group key if symbol is in watchlist, else None."""
    for key, grp in config.groups.items():
        if symbol in grp.symbols:
            return key
    return None


def is_watchlist_symbol(symbol: str, config: WatchlistConfig) -> bool:
    """True if symbol is in any non-avoid group."""
    grp = group_for_symbol(symbol, config)
    return grp is not None and grp != "avoid"


def is_avoid_symbol(symbol: str, config: WatchlistConfig) -> bool:
    """True if symbol is in the avoid group."""
    return symbol in config.avoid_symbols()


def preferred_sector_bonus(theme: str, config: WatchlistConfig) -> float:
    """Return small bonus (0-1.5) if theme matches a preferred sector."""
    for sector in config.prefer_sectors:
        if sector in theme or theme in sector:
            return 1.5
    return 0.0


def report_focus_for_hour(hour: int, config: WatchlistConfig) -> dict[str, Any]:
    """Return schedule_context entry for given hour (0-23). Falls back to closest hour."""
    key = f"{hour:02d}"
    if key in config.schedule_context:
        return config.schedule_context[key]

    int_keys = []
    for k in config.schedule_context:
        with __import__("contextlib").suppress(ValueError):
            int_keys.append(int(k))

    if not int_keys:
        return {}

    closest = min(int_keys, key=lambda h: min((hour - h) % 24, (h - hour) % 24))
    return config.schedule_context.get(f"{closest:02d}", {})
