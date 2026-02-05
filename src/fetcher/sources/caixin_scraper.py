"""Caixin (财新) web scraper.

Scrapes articles directly from caixin.com since RSS feeds are unreliable.
Focuses on China economy, finance, and policy coverage.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Any

import httpx
from bs4 import BeautifulSoup

from fetcher.config import SourceConfig
from fetcher.http import request_with_retry

logger = logging.getLogger(__name__)

# Caixin section URLs to scrape
CAIXIN_SECTIONS = [
    {"url": "https://economy.caixin.com/", "name": "财新经济"},
    {"url": "https://finance.caixin.com/", "name": "财新金融"},
    {"url": "https://international.caixin.com/", "name": "财新国际"},
]

# Keywords for filtering relevant articles
RELEVANCE_KEYWORDS = [
    "加拿大", "canada", "canadian",
    "贸易", "trade", "tariff", "关税",
    "外交", "diplomatic", "外交部",
    "半导体", "semiconductor", "芯片",
    "稀土", "rare earth",
    "华为", "huawei", "tiktok", "抖音",
    "油菜籽", "canola",
    "一带一路", "belt and road",
    "印太", "indo-pacific",
    "台湾", "taiwan", "台海",
    "香港", "hong kong",
    "新疆", "xinjiang",
    "制裁", "sanction",
]


def _is_relevant(text: str) -> bool:
    """Check if article text contains relevant keywords."""
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in RELEVANCE_KEYWORDS)


def _parse_caixin_date(date_str: str) -> datetime | None:
    """Parse Caixin date formats."""
    # Common formats: "2026年02月05日" or "02月05日 10:30"
    patterns = [
        (r"(\d{4})年(\d{2})月(\d{2})日", "%Y-%m-%d"),
        (r"(\d{2})月(\d{2})日", None),  # Need to add current year
    ]

    for pattern, fmt in patterns:
        match = re.search(pattern, date_str)
        if match:
            if fmt:
                try:
                    return datetime.strptime(f"{match.group(1)}-{match.group(2)}-{match.group(3)}", fmt)
                except ValueError:
                    continue
            else:
                # Add current year
                try:
                    now = datetime.now()
                    return datetime(now.year, int(match.group(1)), int(match.group(2)))
                except ValueError:
                    continue
    return None


async def _scrape_section(
    client: httpx.AsyncClient,
    section: dict[str, str],
    timeout: int,
) -> list[dict[str, Any]]:
    """Scrape articles from a Caixin section page."""
    articles = []

    try:
        resp = await client.get(section["url"], timeout=timeout)
        resp.raise_for_status()
    except (httpx.HTTPStatusError, httpx.RequestError) as exc:
        logger.warning("Failed to fetch Caixin section %s: %s", section["name"], exc)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find article links - Caixin uses various article containers
    for article_elem in soup.select("a[href*='/articles/'], a[href*='/article/']"):
        href = article_elem.get("href", "")
        if not href:
            continue

        # Normalize URL
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = "https://www.caixin.com" + href

        title = article_elem.get_text(strip=True)
        if not title or len(title) < 5:
            continue

        # Skip if we've seen this URL
        if any(a["url"] == href for a in articles):
            continue

        articles.append({
            "title": title,
            "url": href,
            "source": section["name"],
            "language": "zh",
            "region": "mainland",
        })

    return articles[:20]  # Limit per section


async def _fetch_article_body(
    client: httpx.AsyncClient,
    article: dict[str, Any],
    timeout: int,
) -> None:
    """Fetch and extract the full article body."""
    try:
        resp = await client.get(article["url"], timeout=timeout)
        resp.raise_for_status()
    except (httpx.HTTPStatusError, httpx.RequestError) as exc:
        logger.debug("Failed to fetch article %s: %s", article["url"], exc)
        return

    soup = BeautifulSoup(resp.text, "html.parser")

    # Remove unwanted elements
    for tag in soup.find_all(["script", "style", "nav", "footer", "aside", "iframe"]):
        tag.decompose()

    # Try to find article content
    content_selectors = [
        "#Main_Content_Val",
        ".article-content",
        ".content",
        "article",
    ]

    body_text = ""
    for selector in content_selectors:
        container = soup.select_one(selector)
        if container:
            paragraphs = container.find_all("p")
            body_text = " ".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
            if body_text:
                break

    if body_text:
        article["body_text"] = body_text[:10000]
        article["body_snippet"] = body_text[:500]

    # Try to find publish date
    date_elem = soup.select_one(".time, .date, .publish-time, time")
    if date_elem:
        date_str = date_elem.get_text(strip=True)
        parsed = _parse_caixin_date(date_str)
        if parsed:
            article["date"] = parsed.strftime("%Y-%m-%d")


async def fetch(config: SourceConfig, date: str) -> dict[str, Any]:
    """Fetch articles from Caixin by web scraping.

    Args:
        config: Source configuration.
        date: Target date string (YYYY-MM-DD).

    Returns:
        Dict with articles, counts, and metadata.
    """
    result: dict[str, Any] = {
        "date": date,
        "articles": [],
        "total_articles": 0,
        "sections_checked": 0,
        "errors": [],
    }

    all_articles: list[dict[str, Any]] = []

    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
    ) as client:
        # Scrape each section
        for section in CAIXIN_SECTIONS:
            result["sections_checked"] += 1
            section_articles = await _scrape_section(client, section, config.timeout)
            all_articles.extend(section_articles)
            await asyncio.sleep(1)  # Rate limiting

        # Fetch article bodies concurrently (with semaphore)
        sem = asyncio.Semaphore(5)

        async def fetch_with_sem(article: dict[str, Any]) -> None:
            async with sem:
                await _fetch_article_body(client, article, config.timeout)
                await asyncio.sleep(0.5)

        await asyncio.gather(*[fetch_with_sem(a) for a in all_articles])

    # Filter for relevant articles
    relevant = []
    for article in all_articles:
        text = f"{article.get('title', '')} {article.get('body_snippet', '')}"
        if _is_relevant(text):
            relevant.append(article)

    result["articles"] = relevant
    result["total_articles"] = len(relevant)
    result["scraped_total"] = len(all_articles)

    return result
