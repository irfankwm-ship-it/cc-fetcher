"""Fetcher for Xinhua state media content.

Scrapes Xinhua English section pages (english.news.cn) for:
  - China domestic politics and government statements
  - Foreign policy and geopolitical news
  - Economic and infrastructure announcements

Uses BeautifulSoup for HTML parsing.
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
from fetcher.sources._registry import register_source

logger = logging.getLogger(__name__)

DEFAULT_URL = "http://english.news.cn/"

# Section pages to scrape for broader coverage
SECTION_URLS = [
    "http://english.news.cn/china/index.htm",
    "http://english.news.cn/world/index.htm",
    "http://english.news.cn/",
]

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
    "semiconductor",
    "Hong Kong", "Xinjiang", "Tibet",
    "NPC", "CPPCC", "CPC", "Communist Party",
]

# Articles must mention at least one China indicator word (unless they
# matched a CANADA_KEYWORD, which is inherently bilateral).
CHINA_INDICATORS = [
    "China", "Chinese", "Beijing", "PRC", "mainland",
]


def _extract_article_body(html: str) -> str:
    """Extract article body text from a Xinhua article page.

    Args:
        html: Raw HTML content of article page.

    Returns:
        Extracted body text, or empty string if not found.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Xinhua article body selectors (in order of preference)
    body_selectors = [
        "div.detail_con",  # Main article container
        "div#detail",
        "div.article-body",
        "div.content",
        "article p",
        "div.story p",
    ]

    for selector in body_selectors:
        container = soup.select_one(selector)
        if container:
            # Get all paragraph text
            paragraphs = container.find_all("p")
            if paragraphs:
                text = " ".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
                if len(text) > 50:  # Meaningful content
                    return text[:10000]  # Cap at 10000 chars for full Chinese articles

    # Fallback: find any substantial paragraph content
    all_paragraphs = soup.find_all("p")
    texts = [p.get_text(strip=True) for p in all_paragraphs if len(p.get_text(strip=True)) > 50]
    if texts:
        return " ".join(texts[:10])[:10000]  # More paragraphs, higher limit for Chinese

    return ""


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

        # Resolve relative URLs using proper URL joining (handles ../ paths)
        if url and not url.startswith("http"):
            url = urljoin(base_url, url)

        # Try to extract date from URL pattern like /2026-01/29/c_xxxxx.htm
        if not date and url:
            url_date_match = re.search(r"/(\d{4}-\d{2})/(\d{2})/", url)
            if url_date_match:
                date = f"{url_date_match.group(1)}-{url_date_match.group(2)}"

        articles.append({
            "title": title,
            "body": body[:500] if body else "",
            "date": date,
            "source_url": url,
            "source": "Xinhua",
        })

    return articles


async def _fetch_article_body(
    client: httpx.AsyncClient,
    article: dict[str, Any],
    timeout: float,
) -> None:
    """Fetch full article body if not already present.

    Modifies article dict in place, adding body_text field.
    """
    if article.get("body") and len(article["body"]) > 100:
        # Already have substantial body from index page
        article["body_text"] = article["body"]
        return

    url = article.get("source_url", "")
    if not url:
        return

    try:
        resp = await client.get(url, timeout=timeout)
        resp.raise_for_status()
        body_text = _extract_article_body(resp.text)
        if body_text:
            article["body_text"] = body_text
            # Also update body snippet for filtering
            if not article.get("body"):
                article["body"] = body_text[:500]
    except (httpx.HTTPStatusError, httpx.RequestError) as exc:
        logger.debug("Failed to fetch article body from %s: %s", url, exc)


def _kw_match(keyword: str, text: str) -> bool:
    """Match a keyword against text using word boundaries."""
    return bool(re.search(r'\b' + re.escape(keyword) + r'\b', text, re.IGNORECASE))


def _filter_relevant(
    articles: list[dict[str, Any]],
    canada_keywords: list[str],
    policy_keywords: list[str],
) -> list[dict[str, Any]]:
    """Filter articles for Canada-related and major policy content.

    Each matching article gets a 'relevance' tag indicating why it matched.
    Articles must also pass a China-context check (mention China/Chinese/
    Beijing/PRC/mainland) unless they matched a Canada keyword.
    """
    filtered: list[dict[str, Any]] = []

    for article in articles:
        searchable = f"{article['title']} {article.get('body', '')}"
        tags: list[str] = []
        has_canada = False

        for kw in canada_keywords:
            if _kw_match(kw, searchable):
                tags.append(f"canada:{kw}")
                has_canada = True
                break

        for kw in policy_keywords:
            if _kw_match(kw, searchable):
                tags.append(f"policy:{kw}")
                break

        if not tags:
            continue

        # China-context gate: unless the article matched a Canada keyword
        # (inherently bilateral), it must also mention a China indicator.
        if not has_canada:
            has_china = any(
                _kw_match(ci, searchable) for ci in CHINA_INDICATORS
            )
            if not has_china:
                continue

        article["relevance_tags"] = tags
        filtered.append(article)

    return filtered


@register_source("xinhua")
async def fetch(config: SourceConfig, date: str, *, client=None, **kwargs) -> dict[str, Any]:
    """Fetch and filter Xinhua content from multiple section pages.

    Args:
        config: Source configuration with URL and timeout.
        date: Target date (YYYY-MM-DD).
        client: Optional shared httpx.AsyncClient.

    Returns:
        Dictionary with filtered articles and metadata.
    """
    import asyncio

    urls = config.get("section_urls", SECTION_URLS)
    timeout = config.timeout

    all_articles: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    errors: list[str] = []

    should_close = client is None
    _client = client or httpx.AsyncClient(follow_redirects=True, timeout=timeout)
    try:
        for url in urls:
            try:
                resp = await request_with_retry(
                    _client, "GET", url,
                    retry=config.retry,
                    timeout=timeout,
                )
                html = resp.text
                page_articles = _extract_articles_from_html(html, url)
                for article in page_articles:
                    title = article.get("title", "")
                    if title and title not in seen_titles:
                        seen_titles.add(title)
                        all_articles.append(article)
                logger.info("Xinhua %s: %d articles", url, len(page_articles))
            except httpx.HTTPStatusError as exc:
                logger.warning("Xinhua %s HTTP error: %s", url, exc.response.status_code)
                errors.append(f"{url}: HTTP {exc.response.status_code}")
            except httpx.RequestError as exc:
                logger.warning("Xinhua %s request failed: %s", url, exc)
                errors.append(f"{url}: {exc}")

        if not all_articles and errors:
            return {
                "date": date,
                "error": "; ".join(errors),
                "articles": [],
            }

        # Filter for relevance first (before fetching bodies to minimize requests)
        relevant = _filter_relevant(all_articles, CANADA_KEYWORDS, POLICY_KEYWORDS)

        # Fetch full article bodies for relevant articles (concurrently, max 5 at a time)
        semaphore = asyncio.Semaphore(5)

        async def fetch_with_semaphore(article: dict[str, Any]) -> None:
            async with semaphore:
                await _fetch_article_body(_client, article, timeout)

        await asyncio.gather(*[fetch_with_semaphore(a) for a in relevant])
        logger.info("Xinhua: fetched bodies for %d relevant articles", len(relevant))
    finally:
        if should_close:
            await _client.aclose()

    return {
        "date": date,
        "articles": relevant,
        "total_scraped": len(all_articles),
        "total_relevant": len(relevant),
        "source_url": urls[0] if urls else DEFAULT_URL,
    }
