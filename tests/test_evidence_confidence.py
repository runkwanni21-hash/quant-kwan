from __future__ import annotations

from tele_quant.candidate_expansion import EvidenceConfidence, build_evidence_confidence


def test_build_evidence_confidence_basic():
    conf = build_evidence_confidence(source_count=7, direct_mentions=3, official_reports=2)
    assert conf.confidence_label == "높음"
    assert conf.direct_mentions == 3
    assert conf.official_report_count == 2


def test_build_evidence_confidence_low():
    conf = build_evidence_confidence(source_count=1, direct_mentions=0, official_reports=0)
    assert conf.confidence_label == "낮음"


def test_build_evidence_confidence_medium():
    conf = build_evidence_confidence(source_count=3, direct_mentions=1, official_reports=0)
    assert conf.confidence_label == "보통"


def test_build_evidence_confidence_api_verified_boost():
    conf = build_evidence_confidence(
        source_count=1, direct_mentions=0, official_reports=0, api_verified=True
    )
    # 1 + 2 = 3, should be 보통
    assert conf.confidence_label in ("보통", "높음")
    assert conf.api_verified is True


def test_to_report_line_shows_mentions():
    conf = EvidenceConfidence(
        source_count=5,
        direct_mentions=3,
        official_report_count=2,
        api_verified=False,
        confidence_label="높음",
    )
    line = conf.to_report_line()
    assert "직접 언급 3건" in line
    assert "리포트 2건" in line
    assert "높음" in line


def test_to_report_line_single_source():
    conf = EvidenceConfidence(confidence_label="낮음")
    line = conf.to_report_line()
    assert "단일 출처" in line
