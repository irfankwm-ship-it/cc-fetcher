"""Tests for the MFA source fetcher."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from fetcher.config import RetryConfig, SourceConfig
from fetcher.sources.mfa import (
    _extract_articles_from_html,
    _parse_date_from_url,
    fetch,
)


@pytest.fixture
def mfa_config() -> SourceConfig:
    return SourceConfig(
        name="mfa",
        settings={"url": "https://www.fmprc.gov.cn/eng/xw/fyrbt/lxjzh/"},
        timeout=10,
        retry=RetryConfig(),
    )


@pytest.fixture
def mfa_html(fixtures_dir: Path) -> str:
    return (fixtures_dir / "mfa_page.html").read_text()


def test_parse_date_from_url() -> None:
    """Test date extraction from MFA URL patterns."""
    url = "/eng/xw/fyrbt/lxjzh/202601/t20260130_11234567.shtml"
    result = _parse_date_from_url(url)
    assert result == datetime(2026, 1, 30, tzinfo=UTC)


def test_parse_date_from_url_invalid() -> None:
    """Test date extraction returns None for invalid URLs."""
    assert _parse_date_from_url("/some/random/path") is None
    assert _parse_date_from_url("") is None


def test_extract_articles_from_html_24h_filter(mfa_html: str) -> None:
    """Test HTML extraction filters articles from last 24 hours."""
    # Cutoff is Jan 29, so only Jan 30 and Jan 29 articles should be included
    cutoff = datetime(2026, 1, 29, tzinfo=UTC)
    articles = _extract_articles_from_html(
        mfa_html, "https://www.fmprc.gov.cn/eng/xw/fyrbt/lxjzh/", cutoff
    )

    assert len(articles) == 2
    dates = [a["date"] for a in articles]
    assert "2026-01-30" in dates
    assert "2026-01-29" in dates
    # Older articles should be filtered out
    assert "2026-01-28" not in dates


def test_extract_articles_from_html_all_articles(mfa_html: str) -> None:
    """Test HTML extraction with old cutoff includes all articles."""
    # Cutoff far in the past should include all articles
    cutoff = datetime(2026, 1, 1, tzinfo=UTC)
    articles = _extract_articles_from_html(
        mfa_html, "https://www.fmprc.gov.cn/eng/xw/fyrbt/lxjzh/", cutoff
    )

    assert len(articles) == 5
    for article in articles:
        assert "title" in article
        assert "source_url" in article
        assert article["source"] == "MFA China"
        assert article["date"]  # Should have date populated


def test_extract_articles_from_empty_html() -> None:
    """Test extraction from empty HTML."""
    cutoff = datetime(2026, 1, 1, tzinfo=UTC)
    articles = _extract_articles_from_html(
        "<html><body></body></html>", "http://example.com", cutoff
    )
    assert articles == []


def test_extract_articles_filters_by_cutoff(mfa_html: str) -> None:
    """Test that cutoff correctly filters articles."""
    # Set cutoff to Jan 30 - only Jan 30 article should pass
    cutoff = datetime(2026, 1, 30, tzinfo=UTC)
    articles = _extract_articles_from_html(
        mfa_html, "https://www.fmprc.gov.cn/eng/xw/fyrbt/lxjzh/", cutoff
    )

    assert len(articles) == 1
    assert articles[0]["date"] == "2026-01-30"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_success(mfa_config: SourceConfig, mfa_html: str) -> None:
    """Test successful MFA fetch returns all articles from last 24 hours."""
    respx.get("https://www.fmprc.gov.cn/eng/xw/fyrbt/lxjzh/").mock(
        return_value=httpx.Response(200, text=mfa_html)
    )
    # Mock article body fetches
    respx.get(url__regex=r".*fmprc\.gov\.cn.*\.shtml").mock(
        return_value=httpx.Response(200, text="<html><body><p>Article body</p></body></html>")
    )

    # Target date 2026-01-30 with 24h cutoff means articles from Jan 29-30
    result = await fetch(mfa_config, "2026-01-30")

    assert result["date"] == "2026-01-30"
    assert result["total_scraped"] == 2  # Jan 30 and Jan 29 articles
    assert result["total_fetched"] == 2
    assert len(result["articles"]) == 2
    assert "error" not in result


@respx.mock
@pytest.mark.asyncio
async def test_fetch_http_error(mfa_config: SourceConfig) -> None:
    """Test graceful handling of HTTP errors."""
    respx.get("https://www.fmprc.gov.cn/eng/xw/fyrbt/lxjzh/").mock(
        return_value=httpx.Response(403)
    )

    result = await fetch(mfa_config, "2026-01-30")

    assert "error" in result
    assert result["articles"] == []


@respx.mock
@pytest.mark.asyncio
async def test_fetch_timeout(mfa_config: SourceConfig) -> None:
    """Test graceful handling of connection timeouts."""
    respx.get("https://www.fmprc.gov.cn/eng/xw/fyrbt/lxjzh/").mock(
        side_effect=httpx.ConnectTimeout("Timed out")
    )

    result = await fetch(mfa_config, "2026-01-30")

    assert "error" in result
    assert result["articles"] == []
