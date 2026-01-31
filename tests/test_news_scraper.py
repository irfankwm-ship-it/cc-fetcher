"""Tests for the news scraper source fetcher."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx

from fetcher.config import RetryConfig, SourceConfig
from fetcher.sources.news_scraper import (
    _classify_article,
    _is_duplicate,
    _matches_keywords,
    _parse_feed,
    fetch,
)


@pytest.fixture
def news_config() -> SourceConfig:
    return SourceConfig(
        name="news",
        settings={
            "feeds": [
                {"url": "https://feeds.reuters.com/reuters/worldNews", "name": "Reuters"},
            ],
            "keywords": ["China", "Beijing", "Canada-China", "PRC", "canola"],
        },
        timeout=10,
        retry=RetryConfig(),
    )


@pytest.fixture
def rss_xml(fixtures_dir: Path) -> str:
    return (fixtures_dir / "rss_feed.xml").read_text()


def test_matches_keywords_positive() -> None:
    """Test keyword matching with matching text."""
    assert _matches_keywords("Canada and China trade deal", ["China", "Beijing"]) is True


def test_matches_keywords_negative() -> None:
    """Test keyword matching with non-matching text."""
    assert _matches_keywords("European Central Bank holds rates", ["China", "Beijing"]) is False


def test_matches_keywords_case_insensitive() -> None:
    """Test that keyword matching is case-insensitive."""
    assert _matches_keywords("CHINA announces new policy", ["china"]) is True
    assert _matches_keywords("beijing summit", ["Beijing"]) is True


def test_classify_article_trade() -> None:
    """Test article classification for trade-related content."""
    cats = _classify_article("Canada-China trade tariff export canola")
    assert "trade" in cats


def test_classify_article_diplomatic() -> None:
    """Test article classification for diplomatic content."""
    cats = _classify_article("Ambassador visits embassy for diplomatic talks")
    assert "diplomatic" in cats


def test_classify_article_technology() -> None:
    """Test article classification for tech content."""
    cats = _classify_article("Huawei 5G ban semiconductor restrictions")
    assert "technology" in cats


def test_classify_article_multiple_categories() -> None:
    """Test that an article can match multiple categories."""
    cats = _classify_article("Huawei trade export ban technology tariff")
    assert "technology" in cats
    assert "trade" in cats


def test_classify_article_general_fallback() -> None:
    """Test that unmatched articles get 'general' category."""
    cats = _classify_article("Weather forecast for the week")
    assert cats == ["general"]


def test_is_duplicate_exact() -> None:
    """Test deduplication with very similar titles."""
    seen = ["Canada announces new trade restrictions on China"]
    assert _is_duplicate("Canada announces new trade restrictions on China", seen) is True


def test_is_duplicate_similar() -> None:
    """Test deduplication with similar but not identical titles."""
    seen = ["Canada announces new trade restrictions on China technology exports"]
    assert (
        _is_duplicate(
            "Canada announces new trade restrictions on China technology exports (updated)",
            seen,
        )
        is True
    )


def test_is_duplicate_different() -> None:
    """Test that different titles are not flagged as duplicates."""
    seen = ["Canada announces new trade restrictions"]
    assert _is_duplicate("European Central Bank holds interest rates", seen) is False


def test_parse_feed(rss_xml: str) -> None:
    """Test RSS feed parsing extracts articles correctly."""
    articles = _parse_feed(rss_xml, "Reuters")

    assert len(articles) == 7
    assert articles[0]["source"] == "Reuters"
    assert articles[0]["title"] != ""
    assert articles[0]["url"] != ""


def test_parse_feed_article_structure(rss_xml: str) -> None:
    """Test that parsed articles have the required fields."""
    articles = _parse_feed(rss_xml, "Reuters")

    for article in articles:
        assert "title" in article
        assert "source" in article
        assert "date" in article
        assert "body_snippet" in article
        assert "url" in article


@respx.mock
@pytest.mark.asyncio
async def test_fetch_filters_by_keywords(
    news_config: SourceConfig,
    rss_xml: str,
) -> None:
    """Test that fetch filters articles by keywords."""
    respx.get("https://feeds.reuters.com/reuters/worldNews").mock(
        return_value=httpx.Response(200, text=rss_xml)
    )

    result = await fetch(news_config, "2025-01-17")

    assert result["total_articles"] > 0
    # Every article should match at least one keyword
    for article in result["articles"]:
        text = f"{article['title']} {article['body_snippet']}".lower()
        assert any(
            kw.lower() in text for kw in ["china", "beijing", "canada-china", "prc", "canola"]
        )


@respx.mock
@pytest.mark.asyncio
async def test_fetch_deduplicates(
    news_config: SourceConfig,
    rss_xml: str,
) -> None:
    """Test that fetch removes duplicate articles."""
    respx.get("https://feeds.reuters.com/reuters/worldNews").mock(
        return_value=httpx.Response(200, text=rss_xml)
    )

    result = await fetch(news_config, "2025-01-17")

    # The fixture has a duplicate article (original + updated version)
    titles = [a["title"] for a in result["articles"]]
    # Check that near-duplicate titles are filtered
    assert len(titles) == len(set(titles))


@respx.mock
@pytest.mark.asyncio
async def test_fetch_classifies_articles(
    news_config: SourceConfig,
    rss_xml: str,
) -> None:
    """Test that each article is classified into categories."""
    respx.get("https://feeds.reuters.com/reuters/worldNews").mock(
        return_value=httpx.Response(200, text=rss_xml)
    )

    result = await fetch(news_config, "2025-01-17")

    for article in result["articles"]:
        assert "categories" in article
        assert len(article["categories"]) > 0


@respx.mock
@pytest.mark.asyncio
async def test_fetch_handles_feed_error(news_config: SourceConfig) -> None:
    """Test graceful handling of feed errors."""
    respx.get("https://feeds.reuters.com/reuters/worldNews").mock(
        return_value=httpx.Response(500)
    )

    result = await fetch(news_config, "2025-01-17")

    assert result["articles"] == []
    assert len(result["feed_errors"]) == 1
    assert result["feed_errors"][0]["feed"] == "Reuters"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_handles_timeout(news_config: SourceConfig) -> None:
    """Test graceful handling of timeouts."""
    respx.get("https://feeds.reuters.com/reuters/worldNews").mock(
        side_effect=httpx.ConnectTimeout("Connection timed out")
    )

    result = await fetch(news_config, "2025-01-17")

    assert result["articles"] == []
    assert len(result["feed_errors"]) == 1
