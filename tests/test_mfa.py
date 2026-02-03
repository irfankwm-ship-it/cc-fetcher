"""Tests for the MFA source fetcher."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx

from fetcher.config import RetryConfig, SourceConfig
from fetcher.sources.mfa import (
    _extract_articles_from_html,
    _filter_relevant,
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


def test_extract_articles_from_html(mfa_html: str) -> None:
    """Test HTML extraction finds articles with MFA URL patterns."""
    articles = _extract_articles_from_html(
        mfa_html, "https://www.fmprc.gov.cn/eng/xw/fyrbt/lxjzh/"
    )

    assert len(articles) > 0
    for article in articles:
        assert "title" in article
        assert "source_url" in article
        assert article["source"] == "MFA China"


def test_extract_articles_from_empty_html() -> None:
    """Test extraction from empty HTML."""
    articles = _extract_articles_from_html("<html><body></body></html>", "http://example.com")
    assert articles == []


def test_filter_relevant_canada_keywords(mfa_html: str) -> None:
    """Test filtering for Canada-related content."""
    articles = _extract_articles_from_html(
        mfa_html, "https://www.fmprc.gov.cn/eng/xw/fyrbt/lxjzh/"
    )
    relevant = _filter_relevant(articles)

    # Should find the Canada interference article
    assert len(relevant) > 0
    titles = [a["title"] for a in relevant]
    assert any("Canada" in t for t in titles)


def test_filter_relevant_bilateral_keywords(mfa_html: str) -> None:
    """Test filtering for bilateral keyword content."""
    articles = _extract_articles_from_html(
        mfa_html, "https://www.fmprc.gov.cn/eng/xw/fyrbt/lxjzh/"
    )
    relevant = _filter_relevant(articles)

    # Should find Belt and Road and Taiwan articles
    titles = [a["title"].lower() for a in relevant]
    assert any("belt and road" in t or "taiwan" in t for t in titles)


def test_filter_relevant_excludes_irrelevant(mfa_html: str) -> None:
    """Test that irrelevant articles are excluded."""
    articles = _extract_articles_from_html(
        mfa_html, "https://www.fmprc.gov.cn/eng/xw/fyrbt/lxjzh/"
    )
    relevant = _filter_relevant(articles)

    titles = [a["title"].lower() for a in relevant]
    assert not any("spring festival" in t for t in titles)


@respx.mock
@pytest.mark.asyncio
async def test_fetch_success(mfa_config: SourceConfig, mfa_html: str) -> None:
    """Test successful MFA fetch."""
    respx.get("https://www.fmprc.gov.cn/eng/xw/fyrbt/lxjzh/").mock(
        return_value=httpx.Response(200, text=mfa_html)
    )
    # Mock article body fetches
    respx.get(url__regex=r".*fmprc\.gov\.cn.*\.shtml").mock(
        return_value=httpx.Response(200, text="<html><body><p>Article body</p></body></html>")
    )

    result = await fetch(mfa_config, "2026-01-30")

    assert result["date"] == "2026-01-30"
    assert result["total_scraped"] > 0
    assert result["total_relevant"] <= result["total_scraped"]
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
