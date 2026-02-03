"""Tests for the Xinhua source fetcher."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx

from fetcher.config import RetryConfig, SourceConfig
from fetcher.sources.xinhua import (
    CANADA_KEYWORDS,
    POLICY_KEYWORDS,
    _extract_articles_from_html,
    _filter_relevant,
    fetch,
)


@pytest.fixture
def xinhua_config() -> SourceConfig:
    return SourceConfig(
        name="xinhua",
        settings={"url": "http://www.xinhuanet.com/english/"},
        timeout=10,
        retry=RetryConfig(),
    )


@pytest.fixture
def xinhua_html(fixtures_dir: Path) -> str:
    return (fixtures_dir / "xinhua_page.html").read_text()


def test_extract_articles_from_html(xinhua_html: str) -> None:
    """Test HTML extraction finds articles."""
    articles = _extract_articles_from_html(xinhua_html, "http://www.xinhuanet.com/english/")

    assert len(articles) > 0
    for article in articles:
        assert "title" in article
        assert "source_url" in article
        assert article["source"] == "Xinhua"


def test_extract_articles_resolves_relative_urls(xinhua_html: str) -> None:
    """Test that relative URLs are resolved to absolute URLs."""
    articles = _extract_articles_from_html(xinhua_html, "http://www.xinhuanet.com/english/")

    for article in articles:
        if article["source_url"]:
            assert article["source_url"].startswith("http")


def test_extract_articles_from_empty_html() -> None:
    """Test extraction from empty/minimal HTML."""
    articles = _extract_articles_from_html("<html><body></body></html>", "http://example.com")
    assert articles == []


def test_filter_relevant_canada_keywords(xinhua_html: str) -> None:
    """Test filtering for Canada-related content."""
    articles = _extract_articles_from_html(xinhua_html, "http://www.xinhuanet.com/english/")
    relevant = _filter_relevant(articles, CANADA_KEYWORDS, POLICY_KEYWORDS)

    # The fixture has articles about Canada-China trade and the ambassador to Ottawa
    assert len(relevant) > 0
    for article in relevant:
        assert "relevance_tags" in article
        assert len(article["relevance_tags"]) > 0


def test_filter_relevant_policy_keywords(xinhua_html: str) -> None:
    """Test filtering for policy-related content."""
    articles = _extract_articles_from_html(xinhua_html, "http://www.xinhuanet.com/english/")
    relevant = _filter_relevant(articles, CANADA_KEYWORDS, POLICY_KEYWORDS)

    # Should find Belt and Road, Xi Jinping, State Council, BRICS articles
    policy_articles = [
        a for a in relevant if any("policy:" in tag for tag in a["relevance_tags"])
    ]
    assert len(policy_articles) > 0


def test_filter_relevant_excludes_irrelevant(xinhua_html: str) -> None:
    """Test that irrelevant articles are excluded."""
    articles = _extract_articles_from_html(xinhua_html, "http://www.xinhuanet.com/english/")
    relevant = _filter_relevant(articles, CANADA_KEYWORDS, POLICY_KEYWORDS)

    # Spring Festival and basketball articles should be excluded
    titles = [a["title"].lower() for a in relevant]
    assert not any("basketball" in t for t in titles)
    assert not any("spring festival" in t for t in titles)


@respx.mock
@pytest.mark.asyncio
async def test_fetch_success(
    xinhua_config: SourceConfig,
    xinhua_html: str,
) -> None:
    """Test successful Xinhua fetch."""
    # Mock all section URLs
    respx.get("http://english.news.cn/china/index.htm").mock(
        return_value=httpx.Response(200, text=xinhua_html)
    )
    respx.get("http://english.news.cn/world/index.htm").mock(
        return_value=httpx.Response(200, text="<html></html>")
    )
    respx.get("http://english.news.cn/").mock(
        return_value=httpx.Response(200, text="<html></html>")
    )
    # Mock article body fetches with a catch-all pattern
    respx.get(url__regex=r".*\.html?$").mock(
        return_value=httpx.Response(200, text="<html><div class='detail_con'><p>Article body text here.</p></div></html>")
    )

    result = await fetch(xinhua_config, "2025-01-17")

    assert result["date"] == "2025-01-17"
    assert result["total_scraped"] > 0
    assert result["total_relevant"] > 0
    assert result["total_relevant"] <= result["total_scraped"]
    assert len(result["articles"]) == result["total_relevant"]


@respx.mock
@pytest.mark.asyncio
async def test_fetch_http_error(xinhua_config: SourceConfig) -> None:
    """Test graceful handling of HTTP errors."""
    # All section URLs return 403
    respx.get("http://english.news.cn/china/index.htm").mock(
        return_value=httpx.Response(403)
    )
    respx.get("http://english.news.cn/world/index.htm").mock(
        return_value=httpx.Response(403)
    )
    respx.get("http://english.news.cn/").mock(
        return_value=httpx.Response(403)
    )

    result = await fetch(xinhua_config, "2025-01-17")

    assert "error" in result
    assert result["articles"] == []


@respx.mock
@pytest.mark.asyncio
async def test_fetch_timeout(xinhua_config: SourceConfig) -> None:
    """Test graceful handling of connection timeouts."""
    # All section URLs timeout
    respx.get("http://english.news.cn/china/index.htm").mock(
        side_effect=httpx.ConnectTimeout("Timed out")
    )
    respx.get("http://english.news.cn/world/index.htm").mock(
        side_effect=httpx.ConnectTimeout("Timed out")
    )
    respx.get("http://english.news.cn/").mock(
        side_effect=httpx.ConnectTimeout("Timed out")
    )

    result = await fetch(xinhua_config, "2025-01-17")

    assert "error" in result
    assert result["articles"] == []


@respx.mock
@pytest.mark.asyncio
async def test_fetch_empty_page(xinhua_config: SourceConfig) -> None:
    """Test handling of a page with no articles."""
    empty_html = "<html><body><p>Maintenance</p></body></html>"
    respx.get("http://english.news.cn/china/index.htm").mock(
        return_value=httpx.Response(200, text=empty_html)
    )
    respx.get("http://english.news.cn/world/index.htm").mock(
        return_value=httpx.Response(200, text=empty_html)
    )
    respx.get("http://english.news.cn/").mock(
        return_value=httpx.Response(200, text=empty_html)
    )

    result = await fetch(xinhua_config, "2025-01-17")

    assert result["total_scraped"] == 0
    assert result["total_relevant"] == 0
    assert result["articles"] == []
