"""Fetcher for Xinhua state media content.

Scrapes Xinhua English (and optionally Chinese) pages for:
  - Canada-related content
  - Major domestic/foreign policy announcements
  - Belt and Road / economic initiative coverage

Uses BeautifulSoup for HTML parsing.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import httpx
from bs4 import BeautifulSoup

from fetcher.config import SourceConfig

logger = logging.getLogger(__name__)

DEFAULT_URL = "http://www.xinhuanet.com/english/"

CANADA_KEYWORDS = [
    "Canada", "Canadian", "Ottawa", "Trudeau",
    "canola", "Huawei", "Meng Wanzhou",
    "Arctic", "NORAD", "Five Eyes",
]

POLICY_KEYWORDS = [
    "Belt and Road", "BRI", "Xi Jinping", "State Council",
    "foreign policy", "trade war", "sanctions", "tariff",
    "BRICS", "SCO", "RCEP", "ASEAN",
    "Taiwan", "South China Sea", "military",
    "semiconductor", "technology", "AI",
    "economy", "GDP", "trade", "export",
    "Hong Kong", "Xinjiang", "Tibet",
    "NPC", "CPPCC", "CPC", "Communist Party",
    "diplomacy", "ambassador", "minister",
]


def _extract_articles_from_html(html: str, base_url: str) -> list[dict[str, Any]]:
    """Parse Xinhua HTML page and extract article data.

    Args:
        html: Raw HTML content.
        base_url: Base URL for resolving relative links.

    Returns:
        List of article dictionaries.
    """
    soup = BeautifulSoup(html, "html.parser")
    articles: list[dict[str, Any]] = []

    # Xinhua uses various container patterns; try common selectors
    article_selectors = [
        "div.news_item",
        "div.tit",
        "li.clearfix",
        "div.part_01 li",
        "div.dataList li",
        "article",
        "div.story",
    ]

    elements: list = []
    for selector in article_selectors:
        elements = soup.select(selector)
        if elements:
            break

    # Fallback: find all links that look like article links
    if not elements:
        elements = soup.find_all("a", href=re.compile(r"/\d{4}-\d{2}/\d{2}/"))

    for elem in elements:
        title = ""
        url = ""
        body = ""
        date = ""

        if elem.name == "a":
            title = elem.get_text(strip=True)
            url = elem.get("href", "")
        else:
            link_tag = elem.find("a")
            if link_tag:
                title = link_tag.get_text(strip=True)
                url = link_tag.get("href", "")
            else:
                title = elem.get_text(strip=True)

            # Try to find a body snippet
            body_tag = elem.find("p") or elem.find("div", class_="des")
            if body_tag:
                body = body_tag.get_text(strip=True)

            # Try to find a date
            date_tag = elem.find("span", class_="time") or elem.find("em")
            if date_tag:
                date = date_tag.get_text(strip=True)

        if not title:
            continue

        # Resolve relative URLs
        if url and not url.startswith("http"):
            url = base_url.rstrip("/") + "/" + url.lstrip("/")

        articles.append({
            "title": title,
            "body": body[:500] if body else "",
            "date": date,
            "source_url": url,
            "source": "Xinhua",
        })

    return articles


def _filter_relevant(
    articles: list[dict[str, Any]],
    canada_keywords: list[str],
    policy_keywords: list[str],
) -> list[dict[str, Any]]:
    """Filter articles for Canada-related and major policy content.

    Each matching article gets a 'relevance' tag indicating why it matched.
    """
    filtered: list[dict[str, Any]] = []

    for article in articles:
        searchable = f"{article['title']} {article.get('body', '')}".lower()
        tags: list[str] = []

        for kw in canada_keywords:
            if kw.lower() in searchable:
                tags.append(f"canada:{kw}")
                break

        for kw in policy_keywords:
            if kw.lower() in searchable:
                tags.append(f"policy:{kw}")
                break

        if tags:
            article["relevance_tags"] = tags
            filtered.append(article)

    return filtered


async def fetch(config: SourceConfig, date: str) -> dict[str, Any]:
    """Fetch and filter Xinhua content.

    Args:
        config: Source configuration with URL and timeout.
        date: Target date (YYYY-MM-DD).

    Returns:
        Dictionary with filtered articles and metadata.
    """
    url = config.get("url", DEFAULT_URL)
    timeout = config.timeout

    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            resp = await client.get(url, timeout=timeout)
            resp.raise_for_status()
            html = resp.text
        except httpx.HTTPStatusError as exc:
            logger.error("Xinhua HTTP error: %s", exc.response.status_code)
            return {
                "date": date,
                "error": f"HTTP {exc.response.status_code}",
                "articles": [],
            }
        except httpx.RequestError as exc:
            logger.error("Xinhua request failed: %s", exc)
            return {
                "date": date,
                "error": str(exc),
                "articles": [],
            }

    all_articles = _extract_articles_from_html(html, url)
    relevant = _filter_relevant(all_articles, CANADA_KEYWORDS, POLICY_KEYWORDS)

    return {
        "date": date,
        "articles": relevant,
        "total_scraped": len(all_articles),
        "total_relevant": len(relevant),
        "source_url": url,
    }
