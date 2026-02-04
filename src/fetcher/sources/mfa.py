"""MFA (Ministry of Foreign Affairs) press conference scraper.

Scrapes the English-language press conference listing at
https://www.fmprc.gov.cn/eng/xw/fyrbt/lxjzh/ for all announcements
from the last 24 hours.

Uses Playwright for JavaScript-rendered content.
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

logger = logging.getLogger(__name__)

DEFAULT_URL = "https://www.fmprc.gov.cn/eng/xw/fyrbt/lxjzh/"


def _parse_date_from_url(href: str) -> datetime | None:
    """Extract date from MFA URL pattern like /202601/t20260130_."""
    match = re.search(r"/\d{6}/t(\d{8})_", href)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d").replace(tzinfo=UTC)
    except ValueError:
        return None


def _extract_articles_from_html(
    html: str, base_url: str, cutoff: datetime
) -> list[dict[str, Any]]:
    """Extract article links and titles from MFA listing page.

    Only includes articles published after the cutoff datetime.
    """
    soup = BeautifulSoup(html, "html.parser")
    articles: list[dict[str, Any]] = []
    seen_titles: set[str] = set()

    for link in soup.find_all("a", href=True):
        title = link.get_text(strip=True)
        href = link["href"]

        if not title or len(title) < 10:
            continue

        # MFA URLs typically contain date patterns like /202601/t20260130_
        article_date = _parse_date_from_url(href)
        if not article_date:
            continue

        # Filter: only include articles from last 24 hours
        if article_date < cutoff:
            continue

        if title in seen_titles:
            continue
        seen_titles.add(title)

        source_url = urljoin(base_url, href)
        articles.append({
            "title": title,
            "source_url": source_url,
            "source": "MFA China",
            "body": "",
            "body_text": "",
            "date": article_date.strftime("%Y-%m-%d"),
        })

    return articles


async def _fetch_article_with_playwright(url: str, timeout: int = 30000) -> str:
    """Fetch article body using Playwright for JS-rendered content."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.warning("Playwright not installed, skipping JS-rendered content")
        return ""

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, timeout=timeout)
            # Wait for content to load
            selectors = ".content_text, #News_Body_Txt_A, .TRS_Editor"
            await page.wait_for_selector(selectors, timeout=10000)
            content = await page.content()
            await browser.close()

            soup = BeautifulSoup(content, "html.parser")
            for selector in [".content_text", "#News_Body_Txt_A", ".TRS_Editor"]:
                container = soup.select_one(selector)
                if container:
                    paragraphs = container.find_all("p")
                    texts = [p.get_text(strip=True) for p in paragraphs]
                    text = " ".join(t for t in texts if t)
                    if text and len(text) > 100:
                        return text[:5000]
            return ""
    except Exception as exc:
        logger.warning("Playwright fetch failed for %s: %s", url, exc)
        return ""


async def fetch(config: SourceConfig, date: str) -> dict[str, Any]:
    """Fetch all MFA press conference articles from the last 24 hours.

    Uses Playwright to handle JavaScript-rendered content.

    Args:
        config: Source configuration with URL and retry settings.
        date: Target date string (YYYY-MM-DD).

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
        # Fetch listing page (doesn't need JS)
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(url, timeout=config.timeout)
            resp.raise_for_status()
            articles = _extract_articles_from_html(resp.text, url, cutoff)
            result["total_scraped"] = len(articles)

        if not articles:
            logger.info("MFA: no articles found in last 24 hours")
            return result

        logger.info("MFA: fetching %d article bodies with Playwright", len(articles))

        # Fetch all article bodies from last 24 hours
        for article in articles:
            body = await _fetch_article_with_playwright(article["source_url"])
            article["body_text"] = body
            article["body"] = body[:500] if body else ""
            # Small delay between requests
            await asyncio.sleep(1)

        result["total_fetched"] = len(articles)
        result["articles"] = articles
        logger.info("MFA: fetched %d articles from last 24 hours", len(articles))

    except httpx.HTTPStatusError as exc:
        logger.error("MFA HTTP error: %s", exc)
        result["error"] = f"HTTP {exc.response.status_code}"
    except Exception as exc:
        logger.error("MFA error: %s", exc)
        result["error"] = str(exc)

    return result
