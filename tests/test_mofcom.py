"""Tests for the MOFCOM source fetcher."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx

from fetcher.config import RetryConfig, SourceConfig
from fetcher.sources.mofcom import (
    _extract_articles_from_html,
    _filter_relevant,
    fetch,
)


@pytest.fixture
def mofcom_config() -> SourceConfig:
    return SourceConfig(
        name="mofcom",
        settings={"url": "http://english.mofcom.gov.cn/"},
        timeout=10,
        retry=RetryConfig(),
    )


@pytest.fixture
def mofcom_html(fixtures_dir: Path) -> str:
    return (fixtures_dir / "mofcom_page.html").read_text()


def test_extract_articles_from_html(mofcom_html: str) -> None:
    """Test HTML extraction finds articles with MOFCOM URL patterns."""
    articles = _extract_articles_from_html(mofcom_html, "http://english.mofcom.gov.cn/")

    assert len(articles) > 0
    for article in articles:
        assert "title" in article
        assert "source_url" in article
        assert article["source"] == "MOFCOM"


def test_extract_articles_from_empty_html() -> None:
    """Test extraction from empty HTML."""
    articles = _extract_articles_from_html("<html><body></body></html>", "http://example.com")
    assert articles == []


def test_filter_relevant_canada_keywords(mofcom_html: str) -> None:
    """Test filtering for Canada-related trade content."""
    articles = _extract_articles_from_html(mofcom_html, "http://english.mofcom.gov.cn/")
    relevant = _filter_relevant(articles)

    # Should find the Canadian tariff article
    assert len(relevant) > 0
    titles = [a["title"] for a in relevant]
    assert any("Canadian" in t for t in titles)


def test_filter_relevant_trade_keywords(mofcom_html: str) -> None:
    """Test filtering for trade policy content."""
    articles = _extract_articles_from_html(mofcom_html, "http://english.mofcom.gov.cn/")
    relevant = _filter_relevant(articles)

    # Should find anti-dumping and rare earth articles
    titles = [a["title"].lower() for a in relevant]
    assert any("anti-dumping" in t or "rare earth" in t or "tariff" in t for t in titles)


def test_filter_relevant_excludes_irrelevant(mofcom_html: str) -> None:
    """Test that irrelevant articles are excluded."""
    articles = _extract_articles_from_html(mofcom_html, "http://english.mofcom.gov.cn/")
    relevant = _filter_relevant(articles)

    titles = [a["title"].lower() for a in relevant]
    assert not any("e-commerce development" in t for t in titles)


@respx.mock
@pytest.mark.asyncio
async def test_fetch_success(mofcom_config: SourceConfig, mofcom_html: str) -> None:
    """Test successful MOFCOM fetch."""
    respx.get("http://english.mofcom.gov.cn/").mock(
        return_value=httpx.Response(200, text=mofcom_html)
    )
    # Mock article body fetches
    respx.get(url__regex=r".*mofcom\.gov\.cn.*\.shtml").mock(
        return_value=httpx.Response(200, text="<html><body><p>Trade policy text</p></body></html>")
    )

    result = await fetch(mofcom_config, "2026-01-30")

    assert result["date"] == "2026-01-30"
    assert result["total_scraped"] > 0
    assert result["total_relevant"] <= result["total_scraped"]
    assert "error" not in result


@respx.mock
@pytest.mark.asyncio
async def test_fetch_http_error(mofcom_config: SourceConfig) -> None:
    """Test graceful handling of HTTP errors."""
    respx.get("http://english.mofcom.gov.cn/").mock(
        return_value=httpx.Response(500)
    )

    result = await fetch(mofcom_config, "2026-01-30")

    assert "error" in result
    assert result["articles"] == []


@respx.mock
@pytest.mark.asyncio
async def test_fetch_timeout(mofcom_config: SourceConfig) -> None:
    """Test graceful handling of connection timeouts."""
    respx.get("http://english.mofcom.gov.cn/").mock(
        side_effect=httpx.ConnectTimeout("Timed out")
    )

    result = await fetch(mofcom_config, "2026-01-30")

    assert "error" in result
    assert result["articles"] == []
