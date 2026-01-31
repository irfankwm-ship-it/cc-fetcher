"""RSS feed scraper with keyword filtering for Canada-China news.

Fetches from configurable RSS feeds (Reuters, AP, Globe and Mail, CBC,
South China Morning Post) and filters for Canada-China relevant content
using bilingual keyword lists.

Categories: diplomatic, trade, military, technology, political, economic, social, legal
"""

from __future__ import annotations

import logging
import re
from difflib import SequenceMatcher
from typing import Any

import feedparser
import httpx

from fetcher.config import SourceConfig

logger = logging.getLogger(__name__)

DEFAULT_FEEDS = [
    {"url": "https://www.theglobeandmail.com/arc/outboundfeeds/rss/category/world/", "name": "Globe and Mail"},
    {"url": "https://www.cbc.ca/webfeed/rss/rss-world", "name": "CBC"},
    {"url": "https://www.scmp.com/rss/4/feed", "name": "SCMP"},
]

DEFAULT_KEYWORDS = ["China", "Beijing", "Canada-China", "PRC"]

# Bilingual keyword sets for category classification
CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "diplomatic": [
        "ambassador", "embassy", "diplomatic", "consul", "foreign affairs",
        "ambassadeur", "ambassade", "diplomatique", "affaires etrangeres",
    ],
    "trade": [
        "trade", "tariff", "export", "import", "canola", "commerce",
        "tarif", "exportation", "importation", "commerce bilateral",
    ],
    "military": [
        "military", "defense", "navy", "army", "NORAD", "NATO",
        "militaire", "defense", "marine", "armee",
    ],
    "technology": [
        "Huawei", "5G", "technology", "cyber", "AI", "semiconductor",
        "technologie", "cybersecurite", "intelligence artificielle",
    ],
    "political": [
        "parliament", "election", "Trudeau", "Xi Jinping", "CPC", "communist",
        "parlement", "election", "parti communiste",
    ],
    "economic": [
        "economy", "GDP", "investment", "market", "stock", "yuan", "currency",
        "economie", "PIB", "investissement", "marche", "devise",
    ],
    "social": [
        "Uyghur", "Hong Kong", "human rights", "detention", "Meng Wanzhou",
        "droits de la personne", "detention",
    ],
    "legal": [
        "sanctions", "ban", "restriction", "extradition", "espionage",
        "interdiction", "restriction", "espionnage",
    ],
}


def _matches_keywords(text: str, keywords: list[str]) -> bool:
    """Check if text contains any of the given keywords (case-insensitive)."""
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in keywords)


def _classify_article(text: str) -> list[str]:
    """Classify an article into categories based on keyword matching."""
    categories: list[str] = []
    text_lower = text.lower()
    for category, kws in CATEGORY_KEYWORDS.items():
        if any(kw.lower() in text_lower for kw in kws):
            categories.append(category)
    return categories or ["general"]


def _is_duplicate(title: str, seen_titles: list[str], threshold: float = 0.75) -> bool:
    """Check if a title is too similar to any previously seen title."""
    title_clean = re.sub(r"[^\w\s]", "", title.lower())
    for seen in seen_titles:
        seen_clean = re.sub(r"[^\w\s]", "", seen.lower())
        ratio = SequenceMatcher(None, title_clean, seen_clean).ratio()
        if ratio >= threshold:
            return True
    return False


def _parse_feed(feed_content: str, feed_name: str) -> list[dict[str, Any]]:
    """Parse RSS feed content into article records."""
    parsed = feedparser.parse(feed_content)
    articles: list[dict[str, Any]] = []

    for entry in parsed.entries:
        title = entry.get("title", "")
        summary = entry.get("summary", entry.get("description", ""))
        link = entry.get("link", "")
        published = entry.get("published", entry.get("updated", ""))

        # Strip HTML from summary
        clean_summary = re.sub(r"<[^>]+>", "", summary).strip()
        # Truncate to snippet
        snippet = clean_summary[:500] if clean_summary else ""

        articles.append({
            "title": title,
            "source": feed_name,
            "date": published,
            "body_snippet": snippet,
            "url": link,
        })

    return articles


async def fetch(config: SourceConfig, date: str) -> dict[str, Any]:
    """Fetch and filter news articles from RSS feeds.

    Args:
        config: Source configuration with feeds list and keywords.
        date: Target date (YYYY-MM-DD).

    Returns:
        Dictionary with filtered articles array and metadata.
    """
    feeds = config.get("feeds", DEFAULT_FEEDS)
    keywords = config.get("keywords", DEFAULT_KEYWORDS)
    timeout = config.timeout

    all_articles: list[dict[str, Any]] = []
    seen_titles: list[str] = []
    feed_errors: list[dict[str, str]] = []

    async with httpx.AsyncClient() as client:
        for feed_cfg in feeds:
            feed_url = feed_cfg.get("url", "")
            feed_name = feed_cfg.get("name", feed_url)

            try:
                resp = await client.get(feed_url, timeout=timeout)
                resp.raise_for_status()
                raw_articles = _parse_feed(resp.text, feed_name)

                for article in raw_articles:
                    searchable = f"{article['title']} {article['body_snippet']}"

                    # Keyword filter
                    if not _matches_keywords(searchable, keywords):
                        continue

                    # Deduplication
                    if _is_duplicate(article["title"], seen_titles):
                        continue

                    # Classify
                    article["categories"] = _classify_article(searchable)
                    all_articles.append(article)
                    seen_titles.append(article["title"])

            except httpx.HTTPStatusError as exc:
                logger.warning("Feed %s HTTP error: %s", feed_name, exc.response.status_code)
                feed_errors.append({"feed": feed_name, "error": f"HTTP {exc.response.status_code}"})
            except httpx.RequestError as exc:
                logger.warning("Feed %s request error: %s", feed_name, exc)
                feed_errors.append({"feed": feed_name, "error": str(exc)})

    return {
        "date": date,
        "articles": all_articles,
        "total_articles": len(all_articles),
        "feeds_checked": len(feeds),
        "feed_errors": feed_errors,
        "keywords_used": keywords,
    }
