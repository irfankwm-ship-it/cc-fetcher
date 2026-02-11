"""The Paper (澎湃新闻) web scraper.

Scrapes articles directly from thepaper.cn since RSS feeds are unreliable.
Covers Chinese domestic politics, society, and international affairs.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime
from typing import Any

import httpx
from bs4 import BeautifulSoup

from fetcher.config import SourceConfig
from fetcher.sources._registry import register_source

logger = logging.getLogger(__name__)

# The Paper uses a JSON API for article listings
THEPAPER_API = "https://www.thepaper.cn/load_index.jsp"

# Channel IDs for relevant sections
CHANNELS = [
    {"id": "25950", "name": "澎湃国际"},  # International
    {"id": "25951", "name": "澎湃财经"},  # Finance
    {"id": "26916", "name": "澎湃科技"},  # Technology
]

# Keywords for filtering relevant articles
RELEVANCE_KEYWORDS = [
    "加拿大", "canada", "渥太华", "ottawa",
    "贸易", "trade", "关税", "tariff",
    "外交", "中美", "中欧",
    "半导体", "芯片", "chip",
    "稀土", "rare earth",
    "华为", "huawei", "字节跳动", "tiktok",
    "一带一路", "belt and road",
    "台湾", "台海", "两岸",
    "香港", "新疆", "西藏",
    "制裁", "sanction",
    "出口管制", "export control",
    "科技战", "tech war",
    "脱钩", "decoupling",
]


def _is_relevant(text: str) -> bool:
    """Check if article text contains relevant keywords."""
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in RELEVANCE_KEYWORDS)


async def _fetch_channel_articles(
    client: httpx.AsyncClient,
    channel: dict[str, str],
    timeout: int,
) -> list[dict[str, Any]]:
    """Fetch articles from a channel using The Paper's API."""
    articles = []

    # The Paper uses a custom API endpoint
    url = f"https://www.thepaper.cn/channel_{channel['id']}"

    try:
        resp = await client.get(url, timeout=timeout)
        resp.raise_for_status()
    except (httpx.HTTPStatusError, httpx.RequestError) as exc:
        logger.warning("Failed to fetch The Paper channel %s: %s", channel["name"], exc)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find article links
    for link in soup.select("a[href*='newsDetail']"):
        href = link.get("href", "")
        if not href:
            continue

        # Normalize URL
        if href.startswith("/"):
            href = "https://www.thepaper.cn" + href

        title = link.get_text(strip=True)
        if not title or len(title) < 5:
            continue

        # Skip duplicates
        if any(a["url"] == href for a in articles):
            continue

        articles.append({
            "title": title,
            "url": href,
            "source": channel["name"],
            "language": "zh",
            "region": "mainland",
        })

    return articles[:15]  # Limit per channel


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
    for tag in soup.find_all(["script", "style", "nav", "footer", "aside"]):
        tag.decompose()

    # Try to find article content
    content_selectors = [
        ".news_txt",
        ".newsDetail_content",
        ".content_txt",
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
    date_patterns = [
        r"(\d{4})-(\d{2})-(\d{2})",
        r"(\d{4})年(\d{2})月(\d{2})日",
    ]

    page_text = soup.get_text()
    for pattern in date_patterns:
        match = re.search(pattern, page_text)
        if match:
            try:
                if "年" in pattern:
                    article["date"] = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
                else:
                    article["date"] = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
                break
            except (ValueError, IndexError):
                continue


@register_source("thepaper")
async def fetch(config: SourceConfig, date: str, **kwargs) -> dict[str, Any]:
    """Fetch articles from The Paper by web scraping.

    Creates its own client with custom User-Agent headers required
    by The Paper. The shared ``client`` kwarg is accepted but not used.

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
        "channels_checked": 0,
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
        # Fetch each channel
        for channel in CHANNELS:
            result["channels_checked"] += 1
            channel_articles = await _fetch_channel_articles(client, channel, config.timeout)
            all_articles.extend(channel_articles)
            await asyncio.sleep(1)  # Rate limiting

        # Fetch article bodies concurrently
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
