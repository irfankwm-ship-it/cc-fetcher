"""MFA (Ministry of Foreign Affairs) press conference scraper.

Scrapes the English-language press conference listing at
https://www.fmprc.gov.cn/eng/xw/fyrbt/lxjzh/ for articles
relevant to Canada-China bilateral relations.
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

DEFAULT_URL = "https://www.fmprc.gov.cn/eng/xw/fyrbt/lxjzh/"

CANADA_KEYWORDS = [
    "Canada",
    "Canadian",
    "Ottawa",
    "Trudeau",
    "canola",
    "Meng Wanzhou",
    "two Michaels",
    "Kovrig",
    "Spavor",
]

BILATERAL_KEYWORDS = [
    "tariff",
    "sanction",
    "trade war",
    "Indo-Pacific",
    "Taiwan",
    "Hong Kong",
    "Xinjiang",
    "Uyghur",
    "Tibet",
    "South China Sea",
    "Belt and Road",
    "BRICS",
    "foreign interference",
    "rare earth",
    "semiconductor",
    "TikTok",
    "Huawei",
]


def _extract_articles_from_html(html: str, base_url: str) -> list[dict[str, Any]]:
    """Extract article links and titles from MFA listing page."""
    soup = BeautifulSoup(html, "html.parser")
    articles: list[dict[str, Any]] = []
    seen_titles: set[str] = set()

    # MFA listing uses <li> elements with <a> links inside content divs
    for link in soup.find_all("a", href=True):
        title = link.get_text(strip=True)
        href = link["href"]

        if not title or len(title) < 10:
            continue

        # MFA URLs typically contain date patterns like /202601/t20260130_
        if not re.search(r"/\d{6}/t\d{8}_", href):
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
            "date": "",
        })

    return articles


def _kw_match(keyword: str, text: str) -> bool:
    """Case-insensitive keyword match with word boundaries."""
    return bool(re.search(rf"\b{re.escape(keyword)}\b", text, re.IGNORECASE))


def _filter_relevant(
    articles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Filter articles for Canada/bilateral relevance."""
    relevant: list[dict[str, Any]] = []

    for article in articles:
        text = f"{article['title']} {article.get('body', '')}"
        tags: list[str] = []

        for kw in CANADA_KEYWORDS:
            if _kw_match(kw, text):
                tags.append(f"canada:{kw}")

        for kw in BILATERAL_KEYWORDS:
            if _kw_match(kw, text):
                tags.append(f"bilateral:{kw}")

        if tags:
            article["relevance_tags"] = tags
            relevant.append(article)

    return relevant


async def _fetch_article_body(
    client: httpx.AsyncClient,
    url: str,
    timeout: int,
) -> str:
    """Fetch and extract body text from an MFA article page."""
    try:
        resp = await client.get(url, timeout=timeout)
        resp.raise_for_status()
    except (httpx.HTTPStatusError, httpx.RequestError) as exc:
        logger.warning("Failed to fetch article %s: %s", url, exc)
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")

    # MFA article body is typically in a div with id "News_Body_Txt_A"
    # or class "TRS_Editor"
    for selector in ["#News_Body_Txt_A", ".TRS_Editor", "article", ".article-body"]:
        container = soup.select_one(selector)
        if container:
            paragraphs = container.find_all("p")
            text = " ".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
            if text:
                return text[:3000]

    return ""


async def fetch(config: SourceConfig, date: str) -> dict[str, Any]:
    """Fetch MFA press conference articles.

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

            # Fetch article bodies for filtering context
            for article in articles:
                article["body"] = await _fetch_article_body(
                    client, article["source_url"], config.timeout
                )

            relevant = _filter_relevant(articles)
            result["total_relevant"] = len(relevant)
            result["articles"] = relevant

    except httpx.HTTPStatusError as exc:
        logger.error("MFA HTTP error: %s", exc)
        result["error"] = f"HTTP {exc.response.status_code}"
    except httpx.RequestError as exc:
        logger.error("MFA request error: %s", exc)
        result["error"] = str(exc)

    return result
