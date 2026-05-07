from __future__ import annotations

import logging
import math
from datetime import datetime

from rapidfuzz import fuzz

from tele_quant.models import RawItem
from tele_quant.ollama_client import OllamaClient
from tele_quant.settings import Settings
from tele_quant.textutil import normalize_for_hash, truncate

log = logging.getLogger(__name__)


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


class Deduper:
    def __init__(self, settings: Settings, ollama: OllamaClient | None = None) -> None:
        self.settings = settings
        self.ollama = ollama

    async def dedupe(self, items: list[RawItem]) -> list[RawItem]:
        if not items:
            return []
        exact = self._exact(items)
        fuzzy = self._fuzzy(exact)
        if (
            self.settings.embedding_dedupe
            and self.ollama
            and len(fuzzy) <= self.settings.embedding_max_items
        ):
            try:
                return await self._semantic(fuzzy)
            except Exception as exc:
                log.warning("[dedupe] semantic dedupe failed, fallback fuzzy: %s", exc)
        return fuzzy

    def _exact(self, items: list[RawItem]) -> list[RawItem]:
        seen: set[str] = set()
        out: list[RawItem] = []
        for item in self._sort(items):
            key = normalize_for_hash(item.compact_text)
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(item)
        return out

    def _fuzzy(self, items: list[RawItem]) -> list[RawItem]:
        reps: list[tuple[str, RawItem]] = []
        for item in self._sort(items):
            norm = normalize_for_hash(item.compact_text)
            if not norm:
                continue
            if any(
                fuzz.token_set_ratio(norm, rep_norm) >= self.settings.fuzzy_dedupe_threshold
                for rep_norm, _ in reps
            ):
                continue
            reps.append((norm, item))
        return [item for _, item in reps]

    async def _semantic(self, items: list[RawItem]) -> list[RawItem]:
        texts = [truncate(normalize_for_hash(item.compact_text), 1800) for item in items]
        vectors = await self.ollama.embed(texts)
        if len(vectors) != len(items):
            log.warning("[dedupe] embedding count mismatch: %d vs %d", len(vectors), len(items))
            return items

        kept_items: list[RawItem] = []
        kept_vecs: list[list[float]] = []
        for item, vec in zip(items, vectors, strict=False):
            if any(
                _cosine(vec, kept) >= self.settings.embedding_dedupe_threshold for kept in kept_vecs
            ):
                continue
            kept_items.append(item)
            kept_vecs.append(vec)
        return kept_items

    def _sort(self, items: list[RawItem]) -> list[RawItem]:
        def key(item: RawItem) -> tuple[datetime, int]:
            # 긴 메시지일수록 원문 정보가 많아서 대표로 남기기 좋다.
            return (item.published_at, len(item.text))

        return sorted(items, key=key, reverse=True)
