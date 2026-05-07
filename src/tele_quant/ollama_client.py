from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import httpx

from tele_quant.models import RawItem, RunStats, utc_now
from tele_quant.settings import Settings
from tele_quant.textutil import truncate

# Avoid circular import at module load; imported inside methods
# from tele_quant.evidence import EvidenceCluster, split_clusters

log = logging.getLogger(__name__)


def _hours_label(hours: float) -> str:
    if hours == int(hours):
        return f"{int(hours)}시간"
    return f"{hours:.1f}시간"


DIGEST_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "one_line": {"type": "string"},
        "market_temperature": {
            "type": "object",
            "properties": {
                "us": {"type": "string"},
                "kr": {"type": "string"},
                "fx_rate": {"type": "string"},
                "risk_appetite": {"type": "string"},
            },
            "required": ["us", "kr", "fx_rate", "risk_appetite"],
        },
        "catalysts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "content": {"type": "string"},
                    "sectors": {"type": "array", "items": {"type": "string"}},
                    "tickers": {"type": "array", "items": {"type": "string"}},
                    "why_important": {"type": "string"},
                },
                "required": ["content", "sectors", "tickers", "why_important"],
            },
        },
        "risks": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "content": {"type": "string"},
                    "sectors": {"type": "array", "items": {"type": "string"}},
                    "tickers": {"type": "array", "items": {"type": "string"}},
                    "why_concerning": {"type": "string"},
                },
                "required": ["content", "sectors", "tickers", "why_concerning"],
            },
        },
        "strong_sectors": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "sector": {"type": "string"},
                    "reason": {"type": "string"},
                },
                "required": ["sector", "reason"],
            },
        },
        "weak_sectors": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "sector": {"type": "string"},
                    "reason": {"type": "string"},
                },
                "required": ["sector", "reason"],
            },
        },
        "mentioned_tickers": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string"},
                    "sentiment": {
                        "type": "string",
                        "enum": ["positive", "negative", "mixed", "neutral"],
                    },
                    "summary": {"type": "string"},
                },
                "required": ["ticker", "sentiment", "summary"],
            },
        },
        "checkpoints": {"type": "array", "items": {"type": "string"}},
    },
    "required": [
        "one_line",
        "market_temperature",
        "catalysts",
        "risks",
        "strong_sectors",
        "weak_sectors",
        "mentioned_tickers",
        "checkpoints",
    ],
}


class OllamaClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = settings.ollama_host.rstrip("/")

    def _timeout(self, read: float | None = None) -> httpx.Timeout:
        return httpx.Timeout(
            connect=30,
            read=read if read is not None else self.settings.ollama_timeout_seconds,
            write=120,
            pool=30,
        )

    async def health(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(10)) as client:
                resp = await client.get(f"{self.base_url}/api/tags")
                return resp.status_code == 200
        except Exception:
            return False

    async def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        payload = {"model": self.settings.ollama_embed_model, "input": texts}
        async with httpx.AsyncClient(timeout=self._timeout()) as client:
            resp = await client.post(f"{self.base_url}/api/embed", json=payload)
            resp.raise_for_status()
            data = resp.json()
        return data.get("embeddings") or []

    async def generate_text(self, prompt: str, system: str = "", max_ctx: int = 8192) -> str:
        """Simple single-turn text generation (no JSON schema)."""
        messages: list[dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        payload: dict[str, Any] = {
            "model": self.settings.ollama_chat_model,
            "messages": messages,
            "stream": False,
            "options": {"temperature": 0.1, "num_ctx": max_ctx},
        }
        try:
            async with httpx.AsyncClient(timeout=self._timeout(read=120)) as client:
                resp = await client.post(f"{self.base_url}/api/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
            return data.get("message", {}).get("content", "") or ""
        except Exception as exc:
            log.warning("[ollama] generate_text failed: %s", exc)
            return ""

    # ------------------------------------------------------------------
    # Main summarize entry point
    # ------------------------------------------------------------------

    async def summarize(
        self,
        items: list[RawItem],
        market_snapshot: list[dict[str, Any]],
        stats: RunStats,
        hours: float = 4.0,
    ) -> str:
        if not items:
            return self._format_empty(stats, hours)

        selected = items[: self.settings.max_items_for_digest]

        if self.settings.digest_chunk_summaries:
            return await self._map_reduce(selected, market_snapshot, stats, hours)
        return await self._single_pass(selected, market_snapshot, stats, hours)

    # ------------------------------------------------------------------
    # Evidence-cluster based summarization (preferred path)
    # ------------------------------------------------------------------

    async def summarize_from_clusters(
        self,
        clusters: list[Any],  # list[EvidenceCluster]
        market_snapshot: list[dict[str, Any]],
        stats: RunStats,
        hours: float = 4.0,
    ) -> str:
        """Summarize from EvidenceCluster list instead of raw RawItems.

        Only headline/summary_hint/tickers/themes/polarity/source_count are sent to
        Ollama — raw text fragments never appear in the prompt.
        """
        from tele_quant.evidence import split_clusters

        if not clusters:
            return self._format_empty(stats, hours)

        macro, pos_stock, neg_stock = split_clusters(clusters, self.settings)

        evidence_payload: dict[str, Any] = {
            "macro_evidence": [c.to_ollama_dict() for c in macro],
            "positive_stock_evidence": [c.to_ollama_dict() for c in pos_stock],
            "negative_stock_evidence": [c.to_ollama_dict() for c in neg_stock],
            "market_snapshot": market_snapshot,
            "rules": [
                "반드시 JSON schema에 맞춰 출력",
                "매수/매도 추천 절대 금지",
                "불확실한 것은 '확인 필요' 표시",
                "자기참조(손절/무효화, 관심 진입 등 기존 시나리오 문구) 포함 증거 무시",
                "각 호재/악재에 sectors와 tickers 포함",
            ],
        }

        log.info(
            "[ollama] evidence-pack: macro=%d pos=%d neg=%d",
            len(macro),
            len(pos_stock),
            len(neg_stock),
        )

        parsed = await self._synthesize_from_evidence(evidence_payload)
        if not parsed:
            log.warning("[ollama] evidence synthesis failed, falling back to cluster text")
            return self._format_cluster_fallback(clusters[:10], stats, market_snapshot, hours)
        return self._format_digest(parsed, stats, market_snapshot, hours)

    async def _synthesize_from_evidence(
        self,
        evidence_payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Call Ollama with structured evidence pack (no raw text)."""
        system_prompt = self._load_system_prompt()
        user_content = {
            "now": utc_now().isoformat(),
            "task": "아래 압축된 증거 묶음을 분석하여 핵심 브리핑을 JSON으로 출력.",
            **evidence_payload,
            "note": (
                "각 evidence의 headline/summary_hint/source_count를 근거로 분석. "
                "원문 조각을 그대로 가져오지 말 것. source_count가 높을수록 중요한 이슈."
            ),
        }
        payload: dict[str, Any] = {
            "model": self.settings.ollama_chat_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_content, ensure_ascii=False)},
            ],
            "stream": False,
            "format": DIGEST_SCHEMA,
            "options": {
                "temperature": self.settings.ollama_temperature,
                "num_ctx": self.settings.ollama_num_ctx,
            },
        }
        try:
            async with httpx.AsyncClient(timeout=self._timeout()) as client:
                resp = await client.post(f"{self.base_url}/api/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
            return self._loads_json(data.get("message", {}).get("content", ""))
        except httpx.ReadTimeout:
            log.warning("[ollama] evidence synthesis ReadTimeout")
        except Exception as exc:
            log.warning("[ollama] evidence synthesis failed: %s", exc)
        return None

    def _format_cluster_fallback(
        self,
        clusters: list[Any],
        stats: RunStats,
        market_snapshot: list[dict[str, Any]],
        hours: float = 4.0,
    ) -> str:
        label = _hours_label(hours)
        lines = [
            f"🧠 Tele Quant {label} 핵심요약 (간이)",
            f"수집: 텔레그램 {stats.telegram_items}건 · 네이버 리포트 {stats.report_items}건 · 클러스터 {len(clusters)}개",
            "",
        ]
        for c in clusters[:10]:
            pol = {"positive": "📈", "negative": "📉", "neutral": "📌"}.get(c.polarity, "")
            lines.append(f"{pol} {c.headline[:100]}")
        lines += [
            "",
            f"출처: 텔레그램 {stats.telegram_items}건, 네이버 리서치 {stats.report_items}건",
            "주의: 공개 정보 기반 개인 리서치 보조용이며 매수/매도 추천이 아님.",
        ]
        return "\n".join(lines).strip()

    # ------------------------------------------------------------------
    # Map-reduce path
    # ------------------------------------------------------------------

    async def _map_reduce(
        self,
        items: list[RawItem],
        market_snapshot: list[dict[str, Any]],
        stats: RunStats,
        hours: float,
    ) -> str:
        chunk_size = self.settings.digest_chunk_size
        chunks = [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]
        log.info("[ollama] map-reduce: %d items → %d chunks", len(items), len(chunks))

        partial_summaries: list[str] = []
        for idx, chunk in enumerate(chunks, 1):
            text = await self._chunk_summary(chunk, idx)
            if text:
                partial_summaries.append(f"[청크{idx}]\n{text}")

        if not partial_summaries:
            return self._format_fallback(items[:10], stats, hours)

        combined = "\n\n".join(partial_summaries)
        parsed = await self._synthesize(combined, market_snapshot)
        if not parsed:
            log.warning("[ollama] synthesis failed, using chunk text fallback")
            return self._format_combined_fallback(partial_summaries, stats, market_snapshot, hours)
        return self._format_digest(parsed, stats, market_snapshot, hours)

    async def _chunk_summary(self, items: list[RawItem], chunk_idx: int) -> str:
        """Summarize one chunk of items into bullet-point text."""

        async def _call(chunk: list[RawItem]) -> str:
            blocks = [
                f"[{chunk_idx}-{i}] {item.source_name}: {truncate(item.compact_text, 600)}"
                for i, item in enumerate(chunk, 1)
            ]
            messages = [
                {
                    "role": "system",
                    "content": "한국어 금융정보 편집장. 핵심 호재/악재/언급 종목만 bullet point 10줄 이내. 추측 금지. /no_think",
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "task": "아래 금융정보의 핵심을 bullet point로 요약. 호재·악재·언급 종목 중심.",
                            "sources": "\n\n".join(blocks),
                        },
                        ensure_ascii=False,
                    ),
                },
            ]
            payload: dict[str, Any] = {
                "model": self.settings.ollama_chat_model,
                "messages": messages,
                "stream": False,
                "options": {
                    "temperature": 0.1,
                    "num_ctx": min(self.settings.ollama_num_ctx, 16384),
                },
            }
            async with httpx.AsyncClient(timeout=self._timeout()) as client:
                resp = await client.post(f"{self.base_url}/api/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
            return data.get("message", {}).get("content", "") or ""

        try:
            return await _call(items)
        except httpx.ReadTimeout:
            log.warning("[ollama] chunk %d ReadTimeout → retrying with half size", chunk_idx)
            half = items[: max(len(items) // 2, 1)]
            try:
                return await _call(half)
            except Exception as exc:
                log.warning("[ollama] chunk %d retry failed: %s", chunk_idx, exc)
        except Exception as exc:
            log.warning("[ollama] chunk %d failed: %s", chunk_idx, exc)

        return "\n".join(f"- {truncate(item.compact_text, 80)}" for item in items[:5])

    async def _synthesize(
        self,
        combined: str,
        market_snapshot: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Final JSON synthesis from partial text summaries."""
        system_prompt = self._load_system_prompt()
        user_content = {
            "now": utc_now().isoformat(),
            "task": "부분 요약들을 하나의 종합 핵심 브리핑으로 최종 합성. JSON schema 준수.",
            "market_snapshot": market_snapshot,
            "partial_summaries": combined[:12000],
            "output_rules": [
                "반드시 JSON schema에 맞춰 출력",
                "매수/매도 추천 금지",
                "불확실한 것은 확인 필요 표시",
                "market_temperature는 수집 정보 기반으로 판단",
                "각 호재/악재에 sectors와 tickers 포함",
            ],
        }
        payload: dict[str, Any] = {
            "model": self.settings.ollama_chat_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_content, ensure_ascii=False)},
            ],
            "stream": False,
            "format": DIGEST_SCHEMA,
            "options": {
                "temperature": self.settings.ollama_temperature,
                "num_ctx": self.settings.ollama_num_ctx,
            },
        }
        try:
            async with httpx.AsyncClient(timeout=self._timeout()) as client:
                resp = await client.post(f"{self.base_url}/api/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
            return self._loads_json(data.get("message", {}).get("content", ""))
        except httpx.ReadTimeout:
            log.warning("[ollama] synthesis ReadTimeout")
        except Exception as exc:
            log.warning("[ollama] synthesis failed: %s", exc)
        return None

    # ------------------------------------------------------------------
    # Single-pass path (legacy / fast mode)
    # ------------------------------------------------------------------

    async def _single_pass(
        self,
        items: list[RawItem],
        market_snapshot: list[dict[str, Any]],
        stats: RunStats,
        hours: float,
    ) -> str:
        blocks = [
            f"[S{i}] type={item.source_type} source={item.source_name} date={item.published_at.isoformat()}\n{truncate(item.compact_text, 1200)}"
            for i, item in enumerate(items, 1)
        ]
        user_content = {
            "now": utc_now().isoformat(),
            "task": "최근 금융정보를 한국어 핵심 브리핑으로 압축해줘.",
            "market_snapshot": market_snapshot,
            "sources": "\n\n".join(blocks),
            "output_rules": [
                "반드시 JSON schema에 맞춰 출력",
                "매수/매도 추천 금지",
                "불확실한 것은 확인 필요 표시",
            ],
        }
        payload: dict[str, Any] = {
            "model": self.settings.ollama_chat_model,
            "messages": [
                {"role": "system", "content": self._load_system_prompt()},
                {"role": "user", "content": json.dumps(user_content, ensure_ascii=False)},
            ],
            "stream": False,
            "format": DIGEST_SCHEMA,
            "options": {
                "temperature": self.settings.ollama_temperature,
                "num_ctx": self.settings.ollama_num_ctx,
            },
        }
        try:
            async with httpx.AsyncClient(timeout=self._timeout()) as client:
                resp = await client.post(f"{self.base_url}/api/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
            parsed = self._loads_json(data.get("message", {}).get("content", ""))
            if parsed:
                return self._format_digest(parsed, stats, market_snapshot, hours)
        except httpx.ReadTimeout:
            log.warning("[ollama] single-pass ReadTimeout")
        except Exception as exc:
            log.warning("[ollama] single-pass failed: %s", exc)
        return self._format_fallback(items[:10], stats, hours)

    # ------------------------------------------------------------------
    # Polish-only entry points (fast / no_llm modes)
    # ------------------------------------------------------------------

    async def polish_digest(self, digest_text: str) -> str:
        """Smooth out a deterministic digest. Returns original on failure or timeout."""
        text = digest_text[:6000]
        payload: dict[str, Any] = {
            "model": self.settings.ollama_chat_model,
            "messages": [
                {"role": "system", "content": "한국어 금융 리포트 편집자. /no_think"},
                {
                    "role": "user",
                    "content": (
                        "아래 리포트를 한국어로 더 읽기 쉽게 다듬어라. "
                        "숫자, 티커, 등급, 롱/숏 구분은 절대 바꾸지 마라. "
                        "새 사실을 추가하지 마라. 매수/매도 확정 표현 금지.\n\n" + text
                    ),
                },
            ],
            "stream": False,
            "options": {"temperature": 0.1, "num_ctx": 8192},
        }
        try:
            timeout = self.settings.ollama_final_timeout_seconds
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(connect=30, read=timeout, write=60, pool=30)
            ) as client:
                resp = await client.post(f"{self.base_url}/api/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
            result = data.get("message", {}).get("content", "") or ""
            return result.strip() or digest_text
        except httpx.ReadTimeout:
            log.warning("[ollama] polish_digest timeout → original")
        except Exception as exc:
            log.warning("[ollama] polish_digest failed: %s → original", exc)
        return digest_text

    async def polish_analysis(self, report_text: str) -> str:
        """Smooth out a deterministic analysis report. Returns original on failure."""
        text = report_text[:6000]
        payload: dict[str, Any] = {
            "model": self.settings.ollama_chat_model,
            "messages": [
                {"role": "system", "content": "한국어 금융 리포트 편집자. /no_think"},
                {
                    "role": "user",
                    "content": (
                        "아래 종목 시나리오 리포트를 한국어로 더 읽기 쉽게 다듬어라. "
                        "숫자, 티커, 점수, 롱/숏/관망 분류는 절대 바꾸지 마라. "
                        "새 사실을 추가하지 마라. 매수/매도 확정 표현 금지.\n\n" + text
                    ),
                },
            ],
            "stream": False,
            "options": {"temperature": 0.1, "num_ctx": 8192},
        }
        try:
            timeout = self.settings.ollama_final_timeout_seconds
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(connect=30, read=timeout, write=60, pool=30)
            ) as client:
                resp = await client.post(f"{self.base_url}/api/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
            result = data.get("message", {}).get("content", "") or ""
            return result.strip() or report_text
        except httpx.ReadTimeout:
            log.warning("[ollama] polish_analysis timeout → original")
        except Exception as exc:
            log.warning("[ollama] polish_analysis failed: %s → original", exc)
        return report_text

    async def polish_weekly_report(self, weekly_text: str) -> str:
        """Polish a deterministic weekly report. Returns original on failure or timeout."""
        text = weekly_text[:8000]
        payload: dict[str, Any] = {
            "model": self.settings.ollama_chat_model,
            "messages": [
                {"role": "system", "content": "한국어 금융 주간 리포트 편집자. /no_think"},
                {
                    "role": "user",
                    "content": (
                        "아래는 이번 주 4시간 단위 리포트들을 규칙 기반으로 집계한 주간 요약 초안이다.\n"
                        "문장을 더 자연스럽게 다듬고, 이번 주 시장의 핵심 흐름과 다음 주 시나리오를 명확하게 정리하라.\n"
                        "숫자, 티커, 후보 분류를 바꾸지 마라.\n"
                        "새 사실을 추가하지 마라.\n"
                        "다음 주 전망은 확정 표현이 아니라 시나리오로 작성하라.\n\n" + text
                    ),
                },
            ],
            "stream": False,
            "options": {
                "temperature": 0.1,
                "num_ctx": self.settings.ollama_num_ctx,
            },
        }
        try:
            timeout = self.settings.weekly_ollama_timeout_seconds
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(connect=30, read=timeout, write=60, pool=30)
            ) as client:
                resp = await client.post(f"{self.base_url}/api/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
            result = data.get("message", {}).get("content", "") or ""
            if len(result) > 200:
                return result.strip()
        except httpx.ReadTimeout:
            log.warning("[ollama] polish_weekly timeout → deterministic fallback")
        except Exception as exc:
            log.warning("[ollama] polish_weekly failed: %s → deterministic fallback", exc)
        return weekly_text

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_system_prompt(self) -> str:
        path = Path("prompts/digest.md")
        if path.exists():
            return path.read_text(encoding="utf-8")
        return "한국어 금융시장 요약가. JSON only. /no_think"

    def _loads_json(self, text: str) -> dict[str, Any] | None:
        text = (text or "").strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            start, end = text.find("{"), text.rfind("}")
            if start >= 0 and end > start:
                try:
                    return json.loads(text[start : end + 1])
                except json.JSONDecodeError:
                    pass
        return None

    # ------------------------------------------------------------------
    # Formatters
    # ------------------------------------------------------------------

    def _format_empty(self, stats: RunStats, hours: float = 4.0) -> str:
        label = _hours_label(hours)
        return (
            f"🧠 Tele Quant {label} 핵심요약\n\n"
            "새로 요약할 만한 핵심 정보가 거의 없습니다.\n"
            f"수집: 텔레그램 {stats.telegram_items}건 · 네이버 리포트 {stats.report_items}건"
        )

    def _format_fallback(self, items: list[RawItem], stats: RunStats, hours: float = 4.0) -> str:
        label = _hours_label(hours)
        lines = [
            f"🧠 Tele Quant {label} 핵심요약 (간이)",
            f"수집: 텔레그램 {stats.telegram_items}건 · 네이버 리포트 {stats.report_items}건 · 중복 제거 후 핵심 {stats.kept_items}건",
            "",
        ]
        for item in items[:10]:
            lines.append(f"- {truncate(item.compact_text, 150)}")
        lines += [
            "",
            f"출처: 텔레그램 {stats.telegram_items}건, 네이버 리서치 {stats.report_items}건",
            "주의: 공개 정보 기반 개인 리서치 보조용이며 매수/매도 추천이 아님.",
        ]
        return "\n".join(lines).strip()

    def _format_combined_fallback(
        self,
        partial_summaries: list[str],
        stats: RunStats,
        market_snapshot: list[dict[str, Any]],
        hours: float = 4.0,
    ) -> str:
        label = _hours_label(hours)
        lines = [
            f"🧠 Tele Quant {label} 핵심요약 (부분 요약)",
            f"수집: 텔레그램 {stats.telegram_items}건 · 네이버 리포트 {stats.report_items}건 · 중복 제거 후 핵심 {stats.kept_items}건",
            "",
            truncate("\n\n".join(partial_summaries), 2800),
            "",
            f"출처: 텔레그램 {stats.telegram_items}건, 네이버 리서치 {stats.report_items}건, Yahoo Finance 시세",
            "주의: 공개 정보 기반 개인 리서치 보조용이며 매수/매도 추천이 아님.",
        ]
        return "\n".join(lines).strip()

    def _format_digest(
        self,
        data: dict[str, Any],
        stats: RunStats,
        market_snapshot: list[dict[str, Any]],
        hours: float = 4.0,
    ) -> str:
        label = _hours_label(hours)
        lines: list[str] = [
            f"🧠 Tele Quant {label} 핵심요약",
            f"수집: 텔레그램 {stats.telegram_items}건 · 네이버 리포트 {stats.report_items}건 · 중복 제거 후 핵심 {stats.kept_items}건",
            "",
        ]

        one_line = str(data.get("one_line") or "").strip()
        if one_line:
            lines += ["한 줄 결론:", f"- {one_line}", ""]

        mt = data.get("market_temperature") or {}
        if isinstance(mt, dict) and any(mt.values()):
            lines.append("🌡 시장 온도:")
            for key, label_kr in [
                ("us", "미국"),
                ("kr", "한국"),
                ("fx_rate", "환율/금리"),
                ("risk_appetite", "위험자산 심리"),
            ]:
                val = str(mt.get(key) or "").strip()
                if val:
                    lines.append(f"- {label_kr}: {val}")
            lines.append("")

        from tele_quant.analysis.quality import reclassify_catalysts_risks

        raw_cats = list(data.get("catalysts") or [])
        raw_risks = list(data.get("risks") or [])
        catalysts, risks_corrected = reclassify_catalysts_risks(raw_cats, raw_risks)
        if catalysts:
            lines.append("🔥 핵심 호재:")
            for idx, c in enumerate(catalysts[:5], 1):
                content = str(c.get("content", "")).strip()
                if not content:
                    continue
                lines.append(f"{idx}. {content}")
                if sectors := c.get("sectors"):
                    lines.append(f"   - 관련 섹터: {', '.join(sectors)}")
                if tickers := c.get("tickers"):
                    lines.append(f"   - 관련 종목: {', '.join(tickers)}")
                if why := str(c.get("why_important", "")).strip():
                    lines.append(f"   - 왜 중요한가: {why}")
            lines.append("")

        risks = risks_corrected
        if risks:
            lines.append("⚠️ 핵심 악재:")
            for idx, r in enumerate(risks[:5], 1):
                content = str(r.get("content", "")).strip()
                if not content:
                    continue
                lines.append(f"{idx}. {content}")
                if sectors := r.get("sectors"):
                    lines.append(f"   - 관련 섹터: {', '.join(sectors)}")
                if tickers := r.get("tickers"):
                    lines.append(f"   - 관련 종목: {', '.join(tickers)}")
                if why := str(r.get("why_concerning", "")).strip():
                    lines.append(f"   - 왜 부담인가: {why}")
            lines.append("")

        if strong := data.get("strong_sectors") or []:
            lines.append("📌 강한 섹터:")
            for s in strong[:6]:
                sector = str(s.get("sector", "")).strip()
                reason = str(s.get("reason", "")).strip()
                if sector:
                    lines.append(f"- {sector}: {reason}" if reason else f"- {sector}")
            lines.append("")

        if weak := data.get("weak_sectors") or []:
            lines.append("📉 약한 섹터:")
            for s in weak[:6]:
                sector = str(s.get("sector", "")).strip()
                reason = str(s.get("reason", "")).strip()
                if sector:
                    lines.append(f"- {sector}: {reason}" if reason else f"- {sector}")
            lines.append("")

        _sentiment_kr = {"positive": "호재", "negative": "악재", "mixed": "혼조", "neutral": "중립"}
        if mentioned := data.get("mentioned_tickers") or []:
            lines.append("🧾 많이 언급된 종목:")
            for t in mentioned[:10]:
                ticker = str(t.get("ticker", "")).strip()
                if not ticker:
                    continue
                sentiment = _sentiment_kr.get(str(t.get("sentiment", "")), "중립")
                summary = str(t.get("summary", "")).strip()
                lines.append(f"- {ticker}: {sentiment}" + (f" - {summary}" if summary else ""))
            lines.append("")

        if checkpoints := data.get("checkpoints") or []:
            lines.append("👀 다음 체크포인트:")
            for cp in checkpoints[:6]:
                lines.append(f"- {cp}")
            lines.append("")

        lines += [
            f"출처: 텔레그램 {stats.telegram_items}건, 네이버 리서치 {stats.report_items}건, Yahoo Finance 시세",
            "주의: 공개 정보 기반 개인 리서치 보조용이며 매수/매도 추천이 아님.",
        ]
        return "\n".join(lines).strip()
