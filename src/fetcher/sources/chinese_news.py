"""Chinese-language news RSS fetcher.

Fetches Chinese-language RSS feeds (Xinhua ZH, People's Daily ZH,
Huanqiu/Global Times ZH, CDT ZH) and filters by China-related keywords
using Chinese substring matching. Tags all articles with language: "zh".
"""

from __future__ import annotations

import asyncio
import logging
import re
from difflib import SequenceMatcher
from typing import Any

import feedparser
import httpx
from bs4 import BeautifulSoup

from fetcher.config import SourceConfig
from fetcher.http import request_with_retry

logger = logging.getLogger(__name__)

DEFAULT_FEEDS: list[dict[str, str]] = [
    {"url": "http://www.xinhuanet.com/politics/xhll.xml", "name": "新华社"},
    {"url": "http://www.people.com.cn/rss/politics.xml", "name": "人民日报"},
    {"url": "https://www.huanqiu.com/rss.xml", "name": "环球时报"},
    {"url": "https://chinadigitaltimes.net/chinese/feed", "name": "中国数字时代"},
]

# Chinese keywords for filtering — uses exact substring matching (no word boundaries)
CHINESE_KEYWORDS: list[str] = [
    "加拿大",   # Canada
    "渥太华",   # Ottawa
    "特鲁多",   # Trudeau
    "油菜籽",   # canola
    "关税",     # tariff
    "制裁",     # sanction
    "贸易战",   # trade war
    "台湾",     # Taiwan
    "香港",     # Hong Kong
    "新疆",     # Xinjiang
    "维吾尔",   # Uyghur
    "西藏",     # Tibet
    "南海",     # South China Sea
    "一带一路", # Belt and Road
    "半导体",   # semiconductor
    "稀土",     # rare earth
    "华为",     # Huawei
    "印太",     # Indo-Pacific
    "金砖",     # BRICS
    "外交",     # diplomacy
    "国防",     # national defense
    "军事",     # military
]

DEDUP_THRESHOLD = 0.75


def _matches_keywords(text: str, keywords: list[str]) -> list[str]:
    """Check if text contains any Chinese keywords (exact substring)."""
    matched: list[str] = []
    for kw in keywords:
        if kw in text:
            matched.append(kw)
    return matched


def _is_duplicate(title: str, seen_titles: list[str]) -> bool:
    """Check if a title is a near-duplicate of any seen title."""
    clean = re.sub(r"[^\w]", "", title)
    for seen in seen_titles:
        ratio = SequenceMatcher(None, clean, seen).ratio()
        if ratio >= DEDUP_THRESHOLD:
            return True
    return False


def _parse_feed(feed_content: str, feed_name: str) -> list[dict[str, Any]]:
    """Parse an RSS feed and return a list of article dicts."""
    parsed = feedparser.parse(feed_content)
    articles: list[dict[str, Any]] = []

    for entry in parsed.entries:
        title = entry.get("title", "").strip()
        if not title:
            continue

        link = entry.get("link", "")
        summary = entry.get("summary", "")
        published = entry.get("published", "")

        articles.append({
            "title": title,
            "url": link,
            "source": feed_name,
            "body_snippet": BeautifulSoup(summary, "html.parser").get_text(strip=True)[:500],
            "date": published,
            "language": "zh",
        })

    return articles


async def _fetch_article_body(
    client: httpx.AsyncClient,
    url: str,
    timeout: int,
) -> str:
    """Fetch and extract body text from a Chinese news article."""
    if not url:
        return ""

    try:
        resp = await client.get(url, timeout=timeout)
        resp.raise_for_status()
    except (httpx.HTTPStatusError, httpx.RequestError) as exc:
        logger.warning("Failed to fetch article %s: %s", url, exc)
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")

    # Remove noise elements
    for tag in soup.find_all(["script", "style", "nav", "footer", "aside"]):
        tag.decompose()

    # Try common Chinese news site selectors
    for selector in [
        "#detail",
        ".article",
        ".rm_txt_con",
        ".article-content",
        ".content",
        "article",
    ]:
        container = soup.select_one(selector)
        if container:
            paragraphs = container.find_all("p")
            text = " ".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
            if text:
                return text[:3000]

    # Fallback: all paragraphs
    paragraphs = soup.find_all("p")
    text = " ".join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20)
    return text[:3000]


async def _enrich_articles_with_body(
    client: httpx.AsyncClient,
    articles: list[dict[str, Any]],
    timeout: int,
    concurrency: int = 10,
) -> None:
    """Fetch full article bodies concurrently."""
    sem = asyncio.Semaphore(concurrency)

    async def _fetch_one(article: dict[str, Any]) -> None:
        async with sem:
            body = await _fetch_article_body(client, article.get("url", ""), timeout)
            if body:
                article["body_text"] = body

    await asyncio.gather(*[_fetch_one(a) for a in articles])


async def fetch(config: SourceConfig, date: str) -> dict[str, Any]:
    """Fetch Chinese-language news articles from RSS feeds.

    Args:
        config: Source configuration with feeds and keyword settings.
        date: Target date string (YYYY-MM-DD).

    Returns:
        Dict with filtered articles, counts, and metadata.
    """
    feeds = config.get("feeds", DEFAULT_FEEDS)
    keywords = config.get("keywords", CHINESE_KEYWORDS)

    result: dict[str, Any] = {
        "date": date,
        "articles": [],
        "total_articles": 0,
        "feeds_checked": 0,
        "feed_errors": [],
        "keywords_used": keywords,
    }

    all_articles: list[dict[str, Any]] = []
    seen_titles: list[str] = []

    async with httpx.AsyncClient(follow_redirects=True) as client:
        for feed_info in feeds:
            feed_url = feed_info["url"]
            feed_name = feed_info.get("name", feed_url)
            result["feeds_checked"] += 1

            try:
                resp = await request_with_retry(
                    client, "GET", feed_url, retry=config.retry, timeout=config.timeout
                )
                resp.raise_for_status()
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                logger.warning("Failed to fetch feed %s: %s", feed_name, exc)
                result["feed_errors"].append({"feed": feed_name, "error": str(exc)})
                continue

            articles = _parse_feed(resp.text, feed_name)

            for article in articles:
                title = article["title"]
                text = f"{title} {article.get('body_snippet', '')}"
                matched = _matches_keywords(text, keywords)

                if not matched:
                    continue

                if _is_duplicate(title, seen_titles):
                    continue

                clean_title = re.sub(r"[^\w]", "", title)
                seen_titles.append(clean_title)

                article["matched_keywords"] = matched
                all_articles.append(article)

        # Enrich with full body text
        if all_articles:
            await _enrich_articles_with_body(client, all_articles, config.timeout)

    result["articles"] = all_articles
    result["total_articles"] = len(all_articles)

    return result
