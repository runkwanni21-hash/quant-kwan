from __future__ import annotations

from tele_quant.analysis.aliases import load_alias_config


def test_bio_us_tickers_present():
    book = load_alias_config()
    all_syms = {s.symbol for s in book.all_symbols}
    for sym in [
        "LLY",
        "MRK",
        "UNH",
        "PFE",
        "MRNA",
        "NVO",
        "ABBV",
        "AMGN",
        "REGN",
        "ISRG",
        "TMO",
        "DHR",
    ]:
        assert sym in all_syms, f"{sym} missing from ticker_aliases.yml"


def test_bio_kr_tickers_present():
    book = load_alias_config()
    all_syms = {s.symbol for s in book.all_symbols}
    for sym in ["207940.KS", "068270.KS", "196170.KQ", "128940.KS", "028300.KQ"]:
        assert sym in all_syms, f"{sym} missing from ticker_aliases.yml"


def test_lly_aliases_include_glp1():
    book = load_alias_config()
    lly_def = next((s for s in book.all_symbols if s.symbol == "LLY"), None)
    assert lly_def is not None
    aliases_lower = [a.lower() for a in lly_def.aliases]
    assert any(
        "glp" in a or "mounjaro" in a.lower() or "zepbound" in a.lower() for a in aliases_lower
    )


def test_nvo_aliases_include_ozempic():
    book = load_alias_config()
    nvo_def = next((s for s in book.all_symbols if s.symbol == "NVO"), None)
    assert nvo_def is not None
    aliases_lower = [a.lower() for a in nvo_def.aliases]
    assert any("ozempic" in a or "wegovy" in a for a in aliases_lower)
