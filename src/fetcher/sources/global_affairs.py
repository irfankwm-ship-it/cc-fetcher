"""Fetcher for Global Affairs Canada news releases.

Uses the canada.ca news API (io-server) to fetch recent news releases
from the Department of Foreign Affairs, Trade and Development, then
filters for China-related content using bilingual keyword matching.

API endpoint:
  https://api.io.canada.ca/io-server/gc/news/{lang}/v2
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import httpx

from fetcher.config import SourceConfig
from fetcher.http import request_with_retry

logger = logging.getLogger(__name__)

NEWS_API_BASE = "https://api.io.canada.ca/io-server/gc/news"
GAC_DEPT = "departmentofforeignaffairstradeanddevelopment"

CHINA_KEYWORDS = [
    # English
    "China", "Chinese", "Beijing", "PRC", "People's Republic",
    "Hong Kong", "Taiwan", "Taipei", "Xinjiang", "Tibet",
    "Indo-Pacific", "Asia-Pacific",
    "Huawei", "canola", "Uyghur",
    "Xi Jinping",
    # French
    "Chine", "chinois", "Pékin", "RPC", "République populaire",
    "Indo-Pacifique", "Asie-Pacifique",
]


def _extract_articles_from_api(
    entries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Convert canada.ca news API entries to article records."""
    articles: list[dict[str, Any]] = []

    for entry in entries:
        title = entry.get("title", "")
        teaser = entry.get("teaser", "")
        link = entry.get("link", "")
        date = entry.get("publishedDate", "")

        # Normalize date to YYYY-MM-DD
        if date and len(date) >= 10:
            date = date[:10]

        articles.append({
            "title": title,
            "body_snippet": teaser[:500] if teaser else "",
            "date": date,
            "source_url": link,
            "source": "Global Affairs Canada",
            "content_type": entry.get("type", "news_release"),
        })

    return articles


def _filter_china_related(
    articles: list[dict[str, Any]],
    keywords: list[str],
) -> list[dict[str, Any]]:
    """Filter articles for China-related content.

    Matches keywords against title and body snippet (case-insensitive).
    Adds 'matched_keywords' to each matching article.
    """
    filtered: list[dict[str, Any]] = []

    for article in articles:
        searchable = f"{article['title']} {article.get('body_snippet', '')}".lower()
        matched = [kw for kw in keywords if kw.lower() in searchable]

        if matched:
            article["matched_keywords"] = matched
            filtered.append(article)

    return filtered


async def fetch(config: SourceConfig, date: str) -> dict[str, Any]:
    """Fetch and filter Global Affairs Canada news.

    Args:
        config: Source configuration with API URL and timeout.
        date: Target date (YYYY-MM-DD).

    Returns:
        Dictionary with filtered articles and metadata.
    """
    api_base = config.get("api_base", NEWS_API_BASE)
    dept = config.get("dept", GAC_DEPT)
    limit = config.get("limit", 50)
    timeout = config.timeout

    # Fetch English and French news
    all_articles: list[dict[str, Any]] = []

    async with httpx.AsyncClient(follow_redirects=True) as client:
        for lang in ("en", "fr"):
            url = f"{api_base}/{lang}/v2"
            params = {
                "dept": dept,
                "type": "newsreleases",
                "limit": limit,
                "sort": "publishedDate",
                "orderBy": "desc",
            }

            try:
                resp = await request_with_retry(
                    client, "GET", url,
                    retry=config.retry,
                    params=params, timeout=timeout,
                )
                data = resp.json()
                entries = data.get("feed", {}).get("entry", [])
                articles = _extract_articles_from_api(entries)
                all_articles.extend(articles)
                logger.info("GAC %s: %d entries fetched", lang.upper(), len(articles))
            except httpx.HTTPStatusError as exc:
                logger.warning("GAC %s HTTP error: %s", lang, exc.response.status_code)
            except httpx.RequestError as exc:
                logger.warning("GAC %s request error: %s", lang, exc)

    # Filter by recency — only keep articles from the last 48 hours
    cutoff = datetime.strptime(date, "%Y-%m-%d") - timedelta(hours=48)
    recent_articles: list[dict[str, Any]] = []
    for article in all_articles:
        article_date = article.get("date", "")
        if article_date and len(article_date) >= 10:
            try:
                dt = datetime.strptime(article_date[:10], "%Y-%m-%d")
                if dt >= cutoff:
                    recent_articles.append(article)
            except ValueError:
                recent_articles.append(article)  # keep if unparseable
        else:
            recent_articles.append(article)  # keep if no date

    # Filter for China-related content
    keywords = config.get("keywords", CHINA_KEYWORDS)
    relevant = _filter_china_related(recent_articles, keywords)

    return {
        "date": date,
        "articles": relevant,
        "total_scraped": len(all_articles),
        "total_recent": len(recent_articles),
        "total_relevant": len(relevant),
        "source_url": f"{api_base}/en/v2?dept={dept}",
    }
