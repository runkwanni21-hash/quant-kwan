from __future__ import annotations

import hashlib
import logging
import re
from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import Any
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from pypdf import PdfReader

from tele_quant.models import RawItem, utc_now
from tele_quant.settings import Settings
from tele_quant.textutil import clean_text, truncate

log = logging.getLogger(__name__)

BASE = "https://finance.naver.com"
ENDPOINTS = {
    "company": "/research/company_list.naver",
    "industry": "/research/industry_list.naver",
    "economy": "/research/economy_list.naver",
    "market": "/research/market_info_list.naver",
    "invest": "/research/invest_list.naver",
    "debenture": "/research/debenture_list.naver",
}
DATE_RE = re.compile(r"(20\d{2})[.\-/](\d{1,2})[.\-/](\d{1,2})")


async def fetch_naver_reports(settings: Settings, hours: float | None = None) -> list[RawItem]:
    if not settings.naver_reports_enabled:
        return []
    lookback = hours if hours is not None else settings.fetch_lookback_hours
    since = utc_now() - timedelta(hours=lookback)
    items: list[RawItem] = []
    pdf_downloads = 0

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; TeleQuant/0.1; personal research bot)",
        "Referer": BASE,
    }
    async with httpx.AsyncClient(timeout=30, headers=headers, follow_redirects=True) as client:
        for category in settings.naver_categories:
            path = ENDPOINTS.get(category)
            if not path:
                continue
            try:
                html = await _get_text(client, urljoin(BASE, path))
                parsed = _parse_report_list(html, category, settings.naver_reports_per_category)
            except Exception as exc:
                log.warning("[naver] list fetch failed %s: %s", category, exc)
                continue

            for row in parsed:
                published_at = row.get("published_at") or utc_now()
                if published_at < since:
                    # Naver 리포트는 하루 단위라 4시간 기준보다 조금 넉넉히 볼 수도 있지만 기본은 필터.
                    continue
                text_parts = [
                    f"카테고리: {category}",
                    f"제목: {row.get('title', '')}",
                    f"증권사/출처: {row.get('broker', '')}",
                    f"원문/목록 텍스트: {row.get('row_text', '')}",
                ]
                pdf_url = row.get("pdf_url")
                if (
                    settings.naver_download_pdfs
                    and pdf_url
                    and pdf_downloads < settings.naver_max_pdfs_per_run
                ):
                    pdf_text = await _download_pdf_text(client, pdf_url, settings)
                    if pdf_text:
                        text_parts.append(f"PDF 일부 텍스트: {pdf_text}")
                        pdf_downloads += 1

                title = clean_text(str(row.get("title") or ""))
                url = row.get("url") or pdf_url or urljoin(BASE, path)
                external_raw = f"naver:{category}:{title}:{row.get('date_text', '')}:{url}"
                external_id = hashlib.sha256(external_raw.encode("utf-8")).hexdigest()[:24]
                items.append(
                    RawItem(
                        source_type="naver_report",
                        source_name=f"Naver Research/{category}",
                        external_id=external_id,
                        published_at=published_at,
                        title=title,
                        text=clean_text("\n".join(text_parts)),
                        url=url,
                        meta={
                            "category": category,
                            "pdf_url": pdf_url,
                            "broker": row.get("broker"),
                        },
                    )
                )
    log.info("[naver] %d reports", len(items))
    return items


async def _get_text(client: httpx.AsyncClient, url: str) -> str:
    resp = await client.get(url)
    resp.raise_for_status()
    # Naver Finance legacy pages are often EUC-KR/CP949.
    if not resp.encoding or resp.encoding.lower() in {"ascii", "utf-8"}:
        resp.encoding = "euc-kr"
    return resp.text


def _parse_report_list(html: str, category: str, limit: int) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, Any]] = []
    for tr in soup.select("tr"):
        row_text = clean_text(" ".join(td.get_text(" ", strip=True) for td in tr.select("td")))
        if not row_text or len(row_text) < 10:
            continue
        date_text = _find_date_text(row_text)
        published_at = _parse_date(date_text)
        anchors = tr.select("a[href]")
        title = ""
        url = ""
        pdf_url = ""
        for a in anchors:
            href = a.get("href") or ""
            label = clean_text(a.get_text(" ", strip=True))
            full = urljoin(BASE, href)
            if ".pdf" in href.lower() or "file" in href.lower():
                pdf_url = full
            elif not title and label and label.lower() not in {"pdf", "download", "다운로드"}:
                title = label
                url = full
        if not title:
            cells = [
                clean_text(td.get_text(" ", strip=True))
                for td in tr.select("td")
                if clean_text(td.get_text(" ", strip=True))
            ]
            title = max(cells, key=len) if cells else row_text[:80]

        broker = _guess_broker(row_text, title, date_text)
        rows.append(
            {
                "category": category,
                "title": truncate(title, 180),
                "broker": broker,
                "date_text": date_text,
                "published_at": published_at,
                "row_text": truncate(row_text, 700),
                "url": url,
                "pdf_url": pdf_url,
            }
        )
        if len(rows) >= limit:
            break
    return rows


def _find_date_text(text: str) -> str:
    match = DATE_RE.search(text)
    return match.group(0) if match else ""


def _parse_date(text: str) -> datetime | None:
    match = DATE_RE.search(text or "")
    if not match:
        return None
    year, month, day = map(int, match.groups())
    return datetime(year, month, day, tzinfo=UTC)


def _guess_broker(row_text: str, title: str, date_text: str) -> str:
    text = row_text.replace(title, "").replace(date_text, "")
    # 흔한 증권사명 후보. 못 맞춰도 요약에는 큰 문제 없음.
    brokers = [
        "키움증권",
        "하나증권",
        "메리츠증권",
        "유안타증권",
        "다올투자증권",
        "현대차증권",
        "SK증권",
        "신한투자증권",
        "삼성증권",
        "미래에셋증권",
        "한국투자증권",
        "NH투자증권",
        "KB증권",
        "대신증권",
        "IBK투자증권",
        "DS투자증권",
        "교보증권",
        "한화투자증권",
        "DB금융투자",
        "LS증권",
        "iM증권",
    ]
    for broker in brokers:
        if broker in text:
            return broker
    parts = [p.strip() for p in text.split() if p.strip()]
    return parts[-1] if parts else ""


async def _download_pdf_text(client: httpx.AsyncClient, url: str, settings: Settings) -> str:
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        reader = PdfReader(BytesIO(resp.content))
        chunks: list[str] = []
        for page in reader.pages[: settings.naver_pdf_max_pages]:
            chunks.append(page.extract_text() or "")
        return truncate(clean_text("\n".join(chunks)), settings.naver_pdf_max_chars)
    except Exception as exc:
        log.warning("[naver] pdf parse failed: %s", exc)
        return ""
