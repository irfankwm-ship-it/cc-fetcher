"""Chinese-language news RSS fetcher.

Fetches Chinese-language RSS feeds from:
- Mainland China: Xinhua, People's Daily, Global Times, China Digital Times
- Taiwan: Liberty Times (自由時報)
- Hong Kong: RTHK (香港電台)

Filters by China/Canada-related keywords using Chinese substring matching.
Tags all articles with language: "zh" and region: "mainland"/"taiwan"/"hongkong".
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
from fetcher.sources._registry import register_source

logger = logging.getLogger(__name__)

DEFAULT_FEEDS: list[dict[str, str]] = [
    # ── Mainland China (Simplified Chinese) — Official/State ──
    {"url": "http://www.xinhuanet.com/politics/xhll.xml", "name": "新华社", "region": "mainland"},
    {"url": "http://www.people.com.cn/rss/politics.xml", "name": "人民日报", "region": "mainland"},
    {"url": "https://www.huanqiu.com/rss.xml", "name": "环球时报", "region": "mainland"},
    # ── Mainland China (Simplified Chinese) — Business/Tech ──
    {"url": "https://36kr.com/feed", "name": "36氪", "region": "mainland"},
    # ── Mainland China (Simplified Chinese) — Independent/Critical ──
    {"url": "https://chinadigitaltimes.net/chinese/feed", "name": "中国数字时代", "region": "mainland"},
    # ── Taiwan (Traditional Chinese) ──
    {"url": "https://news.ltn.com.tw/rss/politics.xml", "name": "自由時報", "region": "taiwan"},
    {"url": "https://news.ltn.com.tw/rss/world.xml", "name": "自由時報國際", "region": "taiwan"},
    # ── Hong Kong (Traditional Chinese) ──
    {"url": "http://rthk9.rthk.hk/rthk/news/rss/c_expressnews_clocal.xml", "name": "香港電台", "region": "hongkong"},
    {"url": "http://rthk9.rthk.hk/rthk/news/rss/c_expressnews_cgreaterchina.xml", "name": "香港電台兩岸", "region": "hongkong"},
    # ── International Chinese-Language Media ──
    {"url": "https://www.bbc.com/zhongwen/simp/index.xml", "name": "BBC中文", "region": "international"},
    {"url": "https://rss.dw.com/xml/rss-chi-all", "name": "德国之声", "region": "international"},
]

# Chinese keywords for filtering — uses exact substring matching (no word boundaries)
# Includes both Simplified (mainland) and Traditional (Taiwan/HK) variants
CHINESE_KEYWORDS: list[str] = [
    # ── Canada-specific ──
    "加拿大",   # Canada (same in both)
    "渥太华",   # Ottawa (Simplified)
    "渥太華",   # Ottawa (Traditional)
    "特鲁多",   # Trudeau (Simplified)
    "特魯多",   # Trudeau (Traditional)
    "卡尼",     # Carney (transliteration)
    "油菜籽",   # canola (same in both)
    # ── Trade/Economic ──
    "关税",     # tariff (Simplified)
    "關稅",     # tariff (Traditional)
    "制裁",     # sanction (same in both)
    "贸易战",   # trade war (Simplified)
    "貿易戰",   # trade war (Traditional)
    "半导体",   # semiconductor (Simplified)
    "半導體",   # semiconductor (Traditional)
    "稀土",     # rare earth (same in both)
    "华为",     # Huawei (Simplified)
    "華為",     # Huawei (Traditional)
    # ── Geopolitical ──
    "台湾",     # Taiwan (Simplified)
    "臺灣",     # Taiwan (Traditional)
    "台灣",     # Taiwan (Traditional alternate)
    "香港",     # Hong Kong (same in both)
    "新疆",     # Xinjiang (same in both)
    "维吾尔",   # Uyghur (Simplified)
    "維吾爾",   # Uyghur (Traditional)
    "西藏",     # Tibet (same in both)
    "南海",     # South China Sea (same in both)
    "一带一路", # Belt and Road (Simplified)
    "一帶一路", # Belt and Road (Traditional)
    "印太",     # Indo-Pacific (same in both)
    "金砖",     # BRICS (Simplified)
    "金磚",     # BRICS (Traditional)
    # ── Political/Military ──
    "外交",     # diplomacy (same in both)
    "国防",     # national defense (Simplified)
    "國防",     # national defense (Traditional)
    "军事",     # military (Simplified)
    "軍事",     # military (Traditional)
    "两岸",     # cross-strait (Simplified)
    "兩岸",     # cross-strait (Traditional)
    "统一",     # unification (Simplified)
    "統一",     # unification (Traditional)
    "独立",     # independence (Simplified)
    "獨立",     # independence (Traditional)
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


def _parse_feed(feed_content: str, feed_name: str, region: str = "mainland") -> list[dict[str, Any]]:
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
            "region": region,  # mainland, taiwan, or hongkong
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
                return text[:10000]  # Full article body for Chinese sources

    # Fallback: all paragraphs
    paragraphs = soup.find_all("p")
    text = " ".join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20)
    return text[:10000]  # Full article body for Chinese sources


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


@register_source("chinese_news")
async def fetch(config: SourceConfig, date: str, *, client=None, **kwargs) -> dict[str, Any]:
    """Fetch Chinese-language news articles from RSS feeds.

    Args:
        config: Source configuration with feeds and keyword settings.
        date: Target date string (YYYY-MM-DD).
        client: Optional shared httpx.AsyncClient.

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

    should_close = client is None
    _client = client or httpx.AsyncClient(follow_redirects=True, timeout=config.timeout)
    try:
        for feed_info in feeds:
            feed_url = feed_info["url"]
            feed_name = feed_info.get("name", feed_url)
            feed_region = feed_info.get("region", "mainland")
            result["feeds_checked"] += 1

            try:
                resp = await request_with_retry(
                    _client, "GET", feed_url, retry=config.retry, timeout=config.timeout
                )
                resp.raise_for_status()
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                logger.warning("Failed to fetch feed %s: %s", feed_name, exc)
                result["feed_errors"].append({"feed": feed_name, "error": str(exc)})
                continue

            articles = _parse_feed(resp.text, feed_name, feed_region)

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
            await _enrich_articles_with_body(_client, all_articles, config.timeout)
    finally:
        if should_close:
            await _client.aclose()

    result["articles"] = all_articles
    result["total_articles"] = len(all_articles)

    return result
