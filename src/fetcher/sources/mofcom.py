"""MOFCOM (Ministry of Commerce) scraper.

Scrapes the English-language MOFCOM site at
http://english.mofcom.gov.cn/ for trade policy announcements
relevant to Canada-China relations.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from fetcher.config import SourceConfig
from fetcher.http import request_with_retry

logger = logging.getLogger(__name__)

DEFAULT_URL = "http://english.mofcom.gov.cn/"

CANADA_KEYWORDS = [
    "Canada",
    "Canadian",
    "canola",
    "pork",
    "lobster",
    "tariff",
    "anti-dumping",
    "countervailing",
]

TRADE_KEYWORDS = [
    "tariff",
    "trade",
    "import",
    "export",
    "sanction",
    "restriction",
    "anti-dumping",
    "countervailing",
    "quota",
    "subsid",
    "rare earth",
    "semiconductor",
    "investigation",
    "trade barrier",
    "market access",
]


def _extract_articles_from_html(html: str, base_url: str) -> list[dict[str, Any]]:
    """Extract article links and titles from MOFCOM pages."""
    soup = BeautifulSoup(html, "html.parser")
    articles: list[dict[str, Any]] = []
    seen_titles: set[str] = set()

    for link in soup.find_all("a", href=True):
        title = link.get_text(strip=True)
        href = link["href"]

        if not title or len(title) < 10:
            continue

        # MOFCOM article URLs typically contain /article/ or date patterns
        if not re.search(r"(?:/article/|/\d{6}/\d{8}/)", href):
            continue

        if title in seen_titles:
            continue
        seen_titles.add(title)

        source_url = urljoin(base_url, href)
        articles.append({
            "title": title,
            "source_url": source_url,
            "source": "MOFCOM",
            "body": "",
            "date": "",
        })

    return articles


def _kw_match(keyword: str, text: str) -> bool:
    """Case-insensitive keyword match with word boundaries."""
    return bool(re.search(rf"\b{re.escape(keyword)}\b", text, re.IGNORECASE))


def _filter_relevant(
    articles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Filter articles for Canada/trade relevance."""
    relevant: list[dict[str, Any]] = []

    for article in articles:
        text = f"{article['title']} {article.get('body', '')}"
        tags: list[str] = []

        for kw in CANADA_KEYWORDS:
            if _kw_match(kw, text):
                tags.append(f"canada:{kw}")

        for kw in TRADE_KEYWORDS:
            if _kw_match(kw, text):
                tags.append(f"trade:{kw}")

        if tags:
            article["relevance_tags"] = tags
            relevant.append(article)

    return relevant


async def _fetch_article_body(
    client: httpx.AsyncClient,
    url: str,
    timeout: int,
) -> str:
    """Fetch and extract body text from a MOFCOM article page."""
    try:
        resp = await client.get(url, timeout=timeout)
        resp.raise_for_status()
    except (httpx.HTTPStatusError, httpx.RequestError) as exc:
        logger.warning("Failed to fetch article %s: %s", url, exc)
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")

    for selector in [".art_con", ".article-body", "article", "#zoom"]:
        container = soup.select_one(selector)
        if container:
            paragraphs = container.find_all("p")
            text = " ".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
            if text:
                return text[:3000]

    return ""


async def fetch(config: SourceConfig, date: str) -> dict[str, Any]:
    """Fetch MOFCOM trade policy articles.

    Args:
        config: Source configuration with URL and retry settings.
        date: Target date string (YYYY-MM-DD).

    Returns:
        Dict with articles, counts, and metadata.
    """
    url = config.get("url", DEFAULT_URL)
    result: dict[str, Any] = {
        "date": date,
        "articles": [],
        "total_scraped": 0,
        "total_relevant": 0,
        "source_url": url,
    }

    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await request_with_retry(
                client, "GET", url, retry=config.retry, timeout=config.timeout
            )
            resp.raise_for_status()

            articles = _extract_articles_from_html(resp.text, url)
            result["total_scraped"] = len(articles)

            for article in articles:
                article["body"] = await _fetch_article_body(
                    client, article["source_url"], config.timeout
                )

            relevant = _filter_relevant(articles)
            result["total_relevant"] = len(relevant)
            result["articles"] = relevant

    except httpx.HTTPStatusError as exc:
        logger.error("MOFCOM HTTP error: %s", exc)
        result["error"] = f"HTTP {exc.response.status_code}"
    except httpx.RequestError as exc:
        logger.error("MOFCOM request error: %s", exc)
        result["error"] = str(exc)

    return result
