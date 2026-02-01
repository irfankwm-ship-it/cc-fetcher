"""RSS feed scraper with keyword filtering for China and Canada-China news.

Fetches from configurable RSS feeds covering political, government,
business, infrastructure, and geopolitical news about China.
Filters for China-relevant content using keyword matching.
After filtering, fetches each article's full page to extract body text
for proper summarization.

Categories: diplomatic, trade, military, technology, political, economic, social, legal
"""

from __future__ import annotations

import asyncio
import logging
import re
from difflib import SequenceMatcher
from typing import Any

# Regex patterns for paywall / CTA / newsletter-signup boilerplate.
# If any pattern matches a paragraph, that paragraph is skipped.
_BOILERPLATE_PATTERNS = [
    re.compile(p)
    for p in [
        r"(?i)members?\s+of\s+\w+\$?\d+.{0,30}(?:unlock|benefit|join|subscribe)",
        r"(?i)you'?ve\s+read\s+\d+\s+article",
        r"(?i)subscribe\s+(?:now|today|to)\s+(?:read|unlock|access|get)",
        r"(?i)sign\s+up\s+(?:for|to)\s+(?:our|the|a)\s+(?:newsletter|daily|free)",
        r"(?i)(?:join|become)\s+(?:a\s+)?(?:member|subscriber|patron)",
        r"(?i)this\s+(?:article|story|content)\s+is\s+(?:for|available\s+to)\s+(?:premium|paid|subscriber)",
        r"(?i)(?:free|premium)\s+(?:trial|access|membership)",
        r"(?i)already\s+(?:a\s+)?(?:member|subscriber)\??\s*(?:log|sign)\s*in",
        r"(?i)support\s+(?:our|independent|quality)\s+(?:journalism|reporting|team)",
        r"(?i)(?:click|tap)\s+here\s+to\s+(?:subscribe|read|join|sign)",
        r"(?i)member\s+benefits?",
        r"(?i)(?:not\s+a\s+)?paywall.{0,30}(?:member|free|independent|thanks)",
        r"(?i)thanks\s+to\s+(?:our\s+)?members",
        r"(?i)no\s+ads.{0,20}no\s+pop.?ups",
    ]
]

import feedparser
import httpx
from bs4 import BeautifulSoup

from fetcher.config import RetryConfig, SourceConfig
from fetcher.http import request_with_retry

# Lighter retry for individual article fetches (many URLs, don't wait too long)
DEFAULT_ARTICLE_RETRY = RetryConfig(max_retries=1, backoff_factor=0.3)

logger = logging.getLogger(__name__)

DEFAULT_FEEDS = [
    # -- Canadian media (bilateral coverage) --
    {
        "url": "https://www.theglobeandmail.com/arc/outboundfeeds/rss/category/world/",
        "name": "Globe and Mail",
    },
    {"url": "https://www.cbc.ca/webfeed/rss/rss-world", "name": "CBC"},
    # -- SCMP section feeds (broad China coverage) --
    {"url": "https://www.scmp.com/rss/4/feed", "name": "SCMP"},
    {"url": "https://www.scmp.com/rss/318198/feed", "name": "SCMP Politics"},
    {"url": "https://www.scmp.com/rss/318199/feed", "name": "SCMP Diplomacy"},
    {"url": "https://www.scmp.com/rss/318421/feed", "name": "SCMP Economy"},
    {"url": "https://www.scmp.com/rss/92/feed", "name": "SCMP Business"},
    {"url": "https://www.scmp.com/rss/320663/feed", "name": "SCMP Tech"},
    # -- International outlets --
    {"url": "https://feeds.bbci.co.uk/news/world/asia/china/rss.xml", "name": "BBC"},
    {"url": "https://thediplomat.com/feed/", "name": "The Diplomat"},
    {"url": "https://asiatimes.com/feed/", "name": "Asia Times"},
    {"url": "https://asia.nikkei.com/rss/feed/nar", "name": "Nikkei Asia"},
]

DEFAULT_KEYWORDS = [
    # Core country / government — unambiguous
    "China", "Chinese", "Beijing", "Xi Jinping",
    "State Council", "Communist Party",
    # Bilateral
    "Canada-China",
    # Geopolitical
    "Taiwan", "South China Sea", "Hong Kong", "Xinjiang", "Tibet",
    "Belt and Road",
    # Economic / business — China-specific terms
    "yuan", "renminbi", "Huawei", "Shanghai", "Shenzhen",
    "People's Liberation Army",
]

# Short acronyms that need word-boundary matching to avoid false positives
# (e.g. "PRC" in "prices", "BRI" in "British", "NPC" in "NPC votes")
_ACRONYM_KEYWORDS = ["PRC", "BRI", "CPC", "NPC", "PLA", "CPPCC"]

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
    """Check if text contains any of the given keywords (case-insensitive).

    Short acronyms in _ACRONYM_KEYWORDS use word-boundary matching
    to avoid false positives (e.g. "PRC" inside "prices").
    """
    text_lower = text.lower()
    if any(kw.lower() in text_lower for kw in keywords):
        return True
    # Word-boundary check for acronyms
    for acr in _ACRONYM_KEYWORDS:
        if re.search(rf"\b{re.escape(acr)}\b", text, re.IGNORECASE):
            return True
    return False


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


def _is_boilerplate(text: str) -> bool:
    """Return True if text matches a paywall/CTA boilerplate pattern."""
    return any(pat.search(text) for pat in _BOILERPLATE_PATTERNS)


def _extract_article_body(html: str) -> str:
    """Extract the main article body text from an HTML page.

    Tries common article container selectors, then falls back to
    collecting all <p> tags from the page.  Returns cleaned plain text.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove noise elements (including paywall/CTA containers)
    for tag in soup.select(
        "script, style, nav, footer, header, aside, "
        ".ad, .ads, .sidebar, "
        ".paywall, .subscription, .membership, .cta, "
        ".newsletter-signup, .subscribe-box, .piano-offer, "
        "[data-paywall], [data-piano], [data-subscriber]"
    ):
        tag.decompose()

    # Try common article body selectors (ordered by specificity)
    selectors = [
        "article .article-body",
        "article .story-body",
        "div.article__body",
        "div.article-body",
        "div.story-body",
        "div[itemprop='articleBody']",
        "div.post-content",
        "div.entry-content",
        "div.article-content",
        "div.content__body",
        "article",
        "main",
    ]

    container = None
    for sel in selectors:
        container = soup.select_one(sel)
        if container:
            break

    scope = container or soup

    # Collect text from paragraphs, headings, and list items
    seen_text: set[str] = set()
    parts: list[str] = []
    for tag in scope.find_all(["h2", "h3", "h4", "li", "p"]):
        text = tag.get_text(strip=True)
        if len(text) < 20 or text in seen_text:
            continue
        # Skip emoji-prefixed lines (e.g. HKFP "💡You've read...")
        if re.match(r'^[\U0001F300-\U0001FAD6\u2600-\u27BF]', text):
            continue
        # Skip paywall / CTA boilerplate
        if _is_boilerplate(text):
            continue
        seen_text.add(text)
        # Prefix headings/list items so the summarizer can identify them
        if tag.name in ("h2", "h3", "h4"):
            parts.append(f"[heading] {text}")
        elif tag.name == "li":
            parts.append(f"[item] {text}")
        else:
            parts.append(text)

    return "\n".join(parts)


async def _fetch_article_body(
    client: httpx.AsyncClient,
    url: str,
    timeout: float = 10,
) -> str:
    """Fetch a single article URL and extract body text."""
    if not url:
        return ""
    try:
        resp = await request_with_retry(
            client, "GET", url,
            retry=DEFAULT_ARTICLE_RETRY,
            timeout=timeout,
        )
        return _extract_article_body(resp.text)
    except (httpx.HTTPStatusError, httpx.RequestError, Exception) as exc:
        logger.debug("Failed to fetch article %s: %s", url[:80], exc)
        return ""


async def _enrich_articles_with_body(
    client: httpx.AsyncClient,
    articles: list[dict[str, Any]],
    concurrency: int = 10,
) -> None:
    """Fetch full article bodies for a batch of articles.

    Updates each article's ``body_text`` in place.  Uses a semaphore
    to limit concurrent requests.
    """
    sem = asyncio.Semaphore(concurrency)

    async def _fetch_one(article: dict[str, Any]) -> None:
        async with sem:
            body = await _fetch_article_body(client, article.get("url", ""))
            if body:
                article["body_text"] = body[:3000]

    await asyncio.gather(*[_fetch_one(a) for a in articles])


async def fetch(config: SourceConfig, date: str) -> dict[str, Any]:
    """Fetch and filter news articles from RSS feeds.

    After keyword filtering, fetches each article's full page to
    extract body text for proper summarization downstream.

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

    async with httpx.AsyncClient(follow_redirects=True) as client:
        for feed_cfg in feeds:
            feed_url = feed_cfg.get("url", "")
            feed_name = feed_cfg.get("name", feed_url)

            try:
                resp = await request_with_retry(
                    client, "GET", feed_url,
                    retry=config.retry,
                    timeout=timeout,
                )
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

        # Fetch full article bodies for all matched articles
        logger.info("Fetching full article bodies for %d articles...", len(all_articles))
        await _enrich_articles_with_body(client, all_articles)
        enriched = sum(1 for a in all_articles if a.get("body_text"))
        logger.info("Enriched %d/%d articles with full body text", enriched, len(all_articles))

    return {
        "date": date,
        "articles": all_articles,
        "total_articles": len(all_articles),
        "feeds_checked": len(feeds),
        "feed_errors": feed_errors,
        "keywords_used": keywords,
    }
