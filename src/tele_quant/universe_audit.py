"""Universe / pair-watch / supply-chain data integrity audit."""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_KR_TICKER_RE = re.compile(r"^\d{6}\.(KS|KQ)$")
_KOREAN_RE = re.compile(r"[가-힣]")
# Short tickers that are legitimate stocks but need context gates in text matching
_SHORT_TICKERS: frozenset[str] = frozenset({"F", "MS", "ON", "BE", "APP", "GS", "C"})
# Broker-associated tickers — extra care needed when they appear in pair_watch targets
_BROKER_TICKERS: frozenset[str] = frozenset({"GS", "JPM", "MS", "C", "BAC", "UBS"})


@dataclass
class UniverseAuditEntry:
    check: str
    target: str
    detail: str
    severity: str  # HIGH | MEDIUM | LOW


def _load_yaml(path: Path) -> Any:
    with open(path) as f:
        return yaml.safe_load(f)


def _load_relation_feed() -> tuple[list[str], list[str], dict[str, str], dict[str, str]]:
    from tele_quant.relation_feed import _NAME_MAP, _SECTOR_MAP, _UNIVERSE_KR, _UNIVERSE_US

    return _UNIVERSE_US, _UNIVERSE_KR, _NAME_MAP, _SECTOR_MAP


def _all_rule_symbols(rules: list[dict[str, Any]]) -> list[tuple[str, str, str]]:
    """Return (rule_id, field, symbol) for every source/target in rules list."""
    out = []
    for rule in rules:
        rid = rule.get("id", "?")
        src = rule.get("source")
        if src:
            out.append((rid, "source", str(src)))
        for t in rule.get("targets", []):
            out.append((rid, "target", str(t)))
    return out


def _all_supply_symbols(rules: list[dict[str, Any]]) -> list[tuple[str, str, str]]:
    """Return (rule_id, field, symbol) for supply-chain rules.

    Handles both flat list and nested {symbol, name} / {symbols: [...]} structures.
    """
    out = []
    for rule in rules:
        rid = rule.get("id", "?")
        # source_symbols: [{symbol: ..., name: ...}]
        for entry in rule.get("source_symbols", []):
            if isinstance(entry, dict):
                sym = entry.get("symbol")
                if sym:
                    out.append((rid, "source_symbols", str(sym)))
            elif isinstance(entry, str):
                out.append((rid, "source_symbols", entry))
        # beneficiaries/victims_on_bearish: [{relation_type, sector, symbols: [{symbol, name}]}]
        for field in ("beneficiaries", "victims_on_bearish", "victims"):
            for group in rule.get(field, []):
                if isinstance(group, dict):
                    for sym_entry in group.get("symbols", []):
                        if isinstance(sym_entry, dict):
                            sym = sym_entry.get("symbol")
                            if sym:
                                out.append((rid, field, str(sym)))
                        elif isinstance(sym_entry, str):
                            out.append((rid, field, sym_entry))
                elif isinstance(group, str):
                    out.append((rid, field, group))
    return out


def run_universe_audit(
    config_dir: Path | None = None,
) -> list[UniverseAuditEntry]:
    """Run comprehensive audit and return all findings."""
    entries: list[UniverseAuditEntry] = []
    root = Path(__file__).parent.parent.parent  # src/tele_quant → src → project root
    cfg = config_dir or (root / "config")

    # ── Load relation_feed data ──────────────────────────────────────────────
    universe_us, universe_kr, name_map, sector_map = _load_relation_feed()
    universe_all = set(universe_us) | set(universe_kr)

    # ── Load config files ────────────────────────────────────────────────────
    pw_rules_path = cfg / "pair_watch_rules.yml"
    pw_uni_path = cfg / "pair_watch_universe.yml"
    sc_rules_path = cfg / "supply_chain_rules.yml"

    pw_rules_data = _load_yaml(pw_rules_path)
    pw_uni_data = _load_yaml(pw_uni_path)
    sc_rules_data = _load_yaml(sc_rules_path)

    pw_rules: list[dict] = pw_rules_data.get("rules", []) if isinstance(pw_rules_data, dict) else []
    pw_uni_stocks: list[dict] = pw_uni_data.get("stocks", []) if isinstance(pw_uni_data, dict) else []
    sc_rules: list[dict] = sc_rules_data.get("rules", []) if isinstance(sc_rules_data, dict) else []

    pw_uni_syms = {str(s.get("ticker", "")) for s in pw_uni_stocks if s.get("ticker")}

    # ── 1. Universe symbols missing NAME_MAP ─────────────────────────────────
    for sym in sorted(universe_all - set(name_map)):
        entries.append(UniverseAuditEntry(
            check="missing_name",
            target=sym,
            detail="relation_feed universe에 있으나 _NAME_MAP 없음",
            severity="MEDIUM",
        ))

    # ── 2. Universe symbols missing SECTOR_MAP ───────────────────────────────
    for sym in sorted(universe_all - set(sector_map)):
        entries.append(UniverseAuditEntry(
            check="missing_sector",
            target=sym,
            detail="relation_feed universe에 있으나 _SECTOR_MAP 없음",
            severity="MEDIUM",
        ))

    # ── 3. KR ticker format errors ───────────────────────────────────────────
    all_rule_syms = _all_rule_symbols(pw_rules)
    all_sc_syms = _all_supply_symbols(sc_rules)
    all_syms = (
        [(s, "universe", s) for s in universe_all]
        + [(rid, fld, sym) for rid, fld, sym in all_rule_syms]
        + [(rid, fld, sym) for rid, fld, sym in all_sc_syms]
        + [("pw_universe", "ticker", s) for s in pw_uni_syms]
    )
    for ctx, fld, sym in all_syms:
        if "." in sym and not _KR_TICKER_RE.match(sym):
            entries.append(UniverseAuditEntry(
                check="kr_format_error",
                target=sym,
                detail=f"[{ctx}].{fld}: KR 티커 형식 오류 (^dddddd.(KS|KQ)$ 아님)",
                severity="HIGH",
            ))

    # ── 4. Korean name placeholders in rules ─────────────────────────────────
    for rid, fld, sym in all_rule_syms + all_sc_syms:
        if _KOREAN_RE.search(sym):
            entries.append(UniverseAuditEntry(
                check="placeholder_symbol",
                target=sym,
                detail=f"[{rid}].{fld}: 한글 이름이 티커 자리에 — 실제 티커로 교체 필요 (HIGH)",
                severity="HIGH",
            ))

    # ── 5. Self-loops (source == target) ─────────────────────────────────────
    for rule in pw_rules:
        rid = rule.get("id", "?")
        src = str(rule.get("source", ""))
        for t in rule.get("targets", []):
            if str(t) == src:
                entries.append(UniverseAuditEntry(
                    check="self_loop",
                    target=f"{rid}:{src}",
                    detail=f"[{rid}] source == target = '{src}'",
                    severity="HIGH",
                ))

    # ── 6. Duplicate rule IDs ─────────────────────────────────────────────────
    seen_ids: dict[str, int] = {}
    for rule in pw_rules:
        rid = rule.get("id", "?")
        seen_ids[rid] = seen_ids.get(rid, 0) + 1
    for rid, count in seen_ids.items():
        if count > 1:
            entries.append(UniverseAuditEntry(
                check="duplicate_rule_id",
                target=rid,
                detail=f"rule id '{rid}' 가 {count}번 반복됨",
                severity="HIGH",
            ))

    # ── 7. Duplicate source-target-direction ─────────────────────────────────
    seen_triples: dict[tuple[str, str, str], str] = {}
    for rule in pw_rules:
        rid = rule.get("id", "?")
        src = str(rule.get("source", ""))
        direction = rule.get("direction", "")
        for t in rule.get("targets", []):
            key = (src, str(t), direction)
            if key in seen_triples:
                entries.append(UniverseAuditEntry(
                    check="duplicate_src_tgt_dir",
                    target=f"{src}→{t}@{direction}",
                    detail=f"[{rid}] 가 [{seen_triples[key]}] 와 동일 source-target-direction",
                    severity="LOW",
                ))
            else:
                seen_triples[key] = rid

    # ── 8. Missing min_source_move_pct ───────────────────────────────────────
    for rule in pw_rules:
        rid = rule.get("id", "?")
        v = rule.get("min_source_move_pct")
        if v is None:
            entries.append(UniverseAuditEntry(
                check="missing_move_pct",
                target=rid,
                detail=f"[{rid}] min_source_move_pct 누락",
                severity="MEDIUM",
            ))
        elif not isinstance(v, (int, float)) or v <= 0:
            entries.append(UniverseAuditEntry(
                check="invalid_move_pct",
                target=rid,
                detail=f"[{rid}] min_source_move_pct={v!r} 비정상",
                severity="HIGH",
            ))

    # ── 9. Missing sector/theme/note ─────────────────────────────────────────
    for rule in pw_rules:
        rid = rule.get("id", "?")
        for field in ("sector", "theme", "note"):
            if not rule.get(field):
                entries.append(UniverseAuditEntry(
                    check=f"missing_{field}",
                    target=rid,
                    detail=f"[{rid}] {field} 누락 또는 비어있음",
                    severity="LOW",
                ))

    # ── 10. Unresolved pair_watch_rules symbols (not in NAME_MAP) ────────────
    name_map_keys = set(name_map)
    for rid, fld, sym in all_rule_syms:
        if sym not in name_map_keys and not _KOREAN_RE.search(sym):
            entries.append(UniverseAuditEntry(
                check="unresolved_symbol",
                target=sym,
                detail=f"[{rid}].{fld}: NAME_MAP에 없음 — 2차 티커이면 무시 가능",
                severity="LOW",
            ))

    # ── 11. Unresolved supply_chain symbols ──────────────────────────────────
    for rid, fld, sym in all_sc_syms:
        if sym not in name_map_keys and not _KOREAN_RE.search(sym):
            entries.append(UniverseAuditEntry(
                check="unresolved_supply_symbol",
                target=sym,
                detail=f"[{rid}].{fld}: NAME_MAP에 없음",
                severity="LOW",
            ))

    # ── 12. Short ticker risk in universe ────────────────────────────────────
    for sym in sorted(universe_all & _SHORT_TICKERS):
        entries.append(UniverseAuditEntry(
            check="short_ticker_risk",
            target=sym,
            detail=f"'{sym}' 는 짧은 티커 — alias 시스템 context gate 적용 필수",
            severity="MEDIUM",
        ))

    # ── 13. Broker ticker as pair_watch target ────────────────────────────────
    for rid, fld, sym in all_rule_syms:
        if sym in _BROKER_TICKERS and fld == "target":
            entries.append(UniverseAuditEntry(
                check="broker_as_target",
                target=f"{rid}:{sym}",
                detail=f"[{rid}].target='{sym}' 는 브로커 연관 티커 — 오탐 여부 주의",
                severity="LOW",
            ))

    # ── 14. pair_watch_universe stocks missing pair_watch_rules coverage ──────
    # (INFO only — not all universe stocks need rules)

    # ── Sort: HIGH first ──────────────────────────────────────────────────────
    order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    entries.sort(key=lambda e: order.get(e.severity, 3))
    return entries


def audit_summary(entries: list[UniverseAuditEntry]) -> dict[str, int]:
    counts: dict[str, int] = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for e in entries:
        counts[e.severity] = counts.get(e.severity, 0) + 1
    return counts
