"""MOFCOM (Ministry of Commerce) scraper.

Scrapes the English-language MOFCOM site at
http://english.mofcom.gov.cn/ for all trade policy announcements
from the last 24 hours.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from fetcher.config import SourceConfig
from fetcher.sources._registry import register_source

logger = logging.getLogger(__name__)

DEFAULT_URL = "http://english.mofcom.gov.cn/"


def _extract_timestamps(html: str) -> dict[str, datetime]:
    """Extract article URL to timestamp mapping from parseToData() calls.

    The MOFCOM page embeds timestamps in JavaScript calls like:
    parseToData(1768995462538,1) which are milliseconds since Unix epoch.

    Returns a dict mapping article URLs to their publication datetimes.
    """
    url_to_date: dict[str, datetime] = {}

    # Find all parseToData timestamps and their positions
    timestamp_pattern = re.compile(r"parseToData\((\d+),")
    url_pattern = re.compile(r'/art/\d{4}/art_[a-f0-9]+\.html')

    # Split HTML into sections to associate URLs with timestamps
    # Each section typically contains article link(s) followed by a timestamp
    sections = re.split(r"<section[^>]*>", html)

    for section in sections:
        # Find all article URLs in this section
        urls = url_pattern.findall(section)
        # Find timestamp in this section
        ts_match = timestamp_pattern.search(section)

        if urls and ts_match:
            timestamp_ms = int(ts_match.group(1))
            article_date = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
            for url in urls:
                if url not in url_to_date:
                    url_to_date[url] = article_date

    return url_to_date


def _extract_articles_from_html(
    html: str, base_url: str, cutoff: datetime
) -> list[dict[str, Any]]:
    """Extract article links and titles from MOFCOM pages.

    Only includes articles published after the cutoff datetime.
    """
    soup = BeautifulSoup(html, "html.parser")
    articles: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    # Get URL to timestamp mapping
    url_to_date = _extract_timestamps(html)

    for link in soup.find_all("a", href=True):
        title = link.get_text(strip=True)
        href = link["href"]

        if not title or len(title) < 10:
            continue

        # MOFCOM article URLs: /News/.../art/YYYY/art_*.html or /Policies/.../art/YYYY/art_*.html
        url_match = re.search(r"/art/\d{4}/art_[a-f0-9]+\.html", href)
        if not url_match:
            continue

        url_path = url_match.group(0)

        # Skip if we've already seen this URL
        if url_path in seen_urls:
            continue
        seen_urls.add(url_path)

        # Get article date from timestamp mapping
        article_date = url_to_date.get(url_path)

        # Filter: only include articles from last 24 hours
        # Require a confirmed timestamp for accurate filtering
        if not article_date:
            # No timestamp found - skip for 24h filtering
            continue

        if article_date < cutoff:
            continue

        date_str = article_date.strftime("%Y-%m-%d")

        source_url = urljoin(base_url, href)
        articles.append({
            "title": title,
            "source_url": source_url,
            "source": "MOFCOM",
            "body": "",
            "body_text": "",
            "date": date_str,
        })

    return articles


async def _fetch_article_body(url: str, timeout: int = 30) -> str:
    """Fetch and extract body text from a MOFCOM article page."""
    import httpx

    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(url, timeout=timeout)
            resp.raise_for_status()
            html = resp.text

            soup = BeautifulSoup(html, "html.parser")
            selectors = [".art_con", ".TRS_Editor", ".article-body", "article", "#zoom", ".content"]
            for selector in selectors:
                container = soup.select_one(selector)
                if container:
                    paragraphs = container.find_all("p")
                    texts = [p.get_text(strip=True) for p in paragraphs]
                    text = " ".join(t for t in texts if t)
                    if text and len(text) > 50:
                        return text[:10000]  # Full article for Chinese government sources
            return ""
    except Exception as exc:
        logger.warning("MOFCOM article fetch failed for %s: %s", url, exc)
        return ""


@register_source("mofcom")
async def fetch(config: SourceConfig, date: str, *, client=None, **kwargs) -> dict[str, Any]:
    """Fetch all MOFCOM trade policy articles from the last 24 hours.

    Args:
        config: Source configuration with URL and retry settings.
        date: Target date string (YYYY-MM-DD).
        client: Optional shared httpx.AsyncClient.

    Returns:
        Dict with articles, counts, and metadata.
    """
    import httpx

    url = config.get("url", DEFAULT_URL)
    result: dict[str, Any] = {
        "date": date,
        "articles": [],
        "total_scraped": 0,
        "total_fetched": 0,
        "source_url": url,
    }

    # Calculate 24-hour cutoff (midnight UTC of target date minus 1 day)
    target_date = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=UTC)
    cutoff = target_date - timedelta(days=1)

    try:
        # MOFCOM listing page is server-rendered, no JS needed
        should_close = client is None
        _client = client or httpx.AsyncClient(follow_redirects=True, timeout=config.timeout)
        try:
            resp = await _client.get(url, timeout=config.timeout)
            resp.raise_for_status()
            html = resp.text
        finally:
            if should_close:
                await _client.aclose()

        articles = _extract_articles_from_html(html, url, cutoff)
        result["total_scraped"] = len(articles)

        if not articles:
            logger.info("MOFCOM: no articles found in last 24 hours")
            return result

        logger.info("MOFCOM: fetching %d article bodies", len(articles))

        # Fetch all article bodies from last 24 hours
        for article in articles:
            body = await _fetch_article_body(article["source_url"], config.timeout)
            article["body_text"] = body
            article["body"] = body[:500] if body else ""
            # Small delay between requests
            await asyncio.sleep(1)

        result["total_fetched"] = len(articles)
        result["articles"] = articles
        logger.info("MOFCOM: fetched %d articles from last 24 hours", len(articles))

    except Exception as exc:
        logger.error("MOFCOM error: %s", exc)
        result["error"] = str(exc)

    return result
