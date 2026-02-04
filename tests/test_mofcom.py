"""Tests for the MOFCOM source fetcher."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from fetcher.config import RetryConfig, SourceConfig
from fetcher.sources.mofcom import (
    _extract_articles_from_html,
    _extract_timestamps,
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


def test_extract_timestamps(mofcom_html: str) -> None:
    """Test timestamp extraction from parseToData() calls."""
    timestamps = _extract_timestamps(mofcom_html)

    assert len(timestamps) == 5
    # First article should be Jan 30
    url = "/art/2026/art_edeb404287d4449da08cea65b267519d.html"
    assert url in timestamps
    assert timestamps[url].date() == datetime(2026, 1, 30, tzinfo=UTC).date()


def test_extract_articles_from_html_24h_filter(mofcom_html: str) -> None:
    """Test HTML extraction filters articles from last 24 hours."""
    # Cutoff is Jan 29, so only Jan 30 and Jan 29 articles should be included
    cutoff = datetime(2026, 1, 29, tzinfo=UTC)
    articles = _extract_articles_from_html(
        mofcom_html, "http://english.mofcom.gov.cn/", cutoff
    )

    assert len(articles) == 2
    dates = [a["date"] for a in articles]
    assert "2026-01-30" in dates
    assert "2026-01-29" in dates
    # Older articles should be filtered out
    assert "2026-01-28" not in dates


def test_extract_articles_from_html_all_articles(mofcom_html: str) -> None:
    """Test HTML extraction with old cutoff includes all articles."""
    # Cutoff far in the past should include all articles
    cutoff = datetime(2026, 1, 1, tzinfo=UTC)
    articles = _extract_articles_from_html(
        mofcom_html, "http://english.mofcom.gov.cn/", cutoff
    )

    assert len(articles) == 5
    for article in articles:
        assert "title" in article
        assert "source_url" in article
        assert article["source"] == "MOFCOM"
        assert article["date"]  # Should have date populated


def test_extract_articles_from_empty_html() -> None:
    """Test extraction from empty HTML."""
    cutoff = datetime(2026, 1, 1, tzinfo=UTC)
    articles = _extract_articles_from_html(
        "<html><body></body></html>", "http://example.com", cutoff
    )
    assert articles == []


def test_extract_articles_filters_by_cutoff(mofcom_html: str) -> None:
    """Test that cutoff correctly filters articles."""
    # Set cutoff to Jan 30 - only Jan 30 article should pass
    cutoff = datetime(2026, 1, 30, tzinfo=UTC)
    articles = _extract_articles_from_html(
        mofcom_html, "http://english.mofcom.gov.cn/", cutoff
    )

    assert len(articles) == 1
    assert articles[0]["date"] == "2026-01-30"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_success(mofcom_config: SourceConfig, mofcom_html: str) -> None:
    """Test successful MOFCOM fetch returns all articles from last 24 hours."""
    respx.get("http://english.mofcom.gov.cn/").mock(
        return_value=httpx.Response(200, text=mofcom_html)
    )
    # Mock article body fetches
    respx.get(url__regex=r".*mofcom\.gov\.cn.*\.html").mock(
        return_value=httpx.Response(200, text="<html><body><p>Trade policy text</p></body></html>")
    )

    # Target date 2026-01-30 with 24h cutoff means articles from Jan 29-30
    result = await fetch(mofcom_config, "2026-01-30")

    assert result["date"] == "2026-01-30"
    assert result["total_scraped"] == 2  # Jan 30 and Jan 29 articles
    assert result["total_fetched"] == 2
    assert len(result["articles"]) == 2
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
