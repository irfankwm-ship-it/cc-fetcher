"""Tests for the Global Affairs Canada source fetcher."""

from __future__ import annotations

import httpx
import pytest
import respx

from fetcher.config import RetryConfig, SourceConfig
from fetcher.sources.global_affairs import (
    CHINA_KEYWORDS,
    _extract_articles_from_api,
    _filter_china_related,
    fetch,
)


@pytest.fixture
def gac_config() -> SourceConfig:
    return SourceConfig(
        name="global_affairs",
        settings={
            "api_base": "https://api.io.canada.ca/io-server/gc/news",
            "dept": "departmentofforeignaffairstradeanddevelopment",
        },
        timeout=10,
        retry=RetryConfig(),
    )


API_ENTRIES = [
    {
        "title": "Statement on China human rights dialogue",
        "teaser": "The Minister discussed Canada's engagement with China on human rights.",
        "link": "https://canada.ca/en/news/2025/01/china-hr.html",
        "publishedDate": "2025-01-17T12:00:00Z",
        "type": "news_release",
    },
    {
        "title": "Canada updates travel advisory for China",
        "teaser": "GAC updated the travel advisory for the PRC.",
        "link": "https://canada.ca/en/news/2025/01/china-advisory.html",
        "publishedDate": "2025-01-16T09:00:00Z",
        "type": "news_release",
    },
    {
        "title": "Canada-EU joint statement on Indo-Pacific engagement",
        "teaser": "Canada and the EU affirmed commitment to a free Indo-Pacific.",
        "link": "https://canada.ca/en/news/2025/01/eu-indopacific.html",
        "publishedDate": "2025-01-15T10:00:00Z",
        "type": "news_release",
    },
    {
        "title": "Development aid package for Southeast Asia",
        "teaser": "$200 million for climate resilience.",
        "link": "https://canada.ca/en/news/2025/01/sea-aid.html",
        "publishedDate": "2025-01-14T08:00:00Z",
        "type": "news_release",
    },
]


def test_extract_articles_from_api() -> None:
    """Test converting API entries to article records."""
    articles = _extract_articles_from_api(API_ENTRIES)

    assert len(articles) == 4
    for article in articles:
        assert "title" in article
        assert "source_url" in article
        assert article["source"] == "Global Affairs Canada"


def test_extract_articles_normalizes_dates() -> None:
    """Test that dates are normalized to YYYY-MM-DD."""
    articles = _extract_articles_from_api(API_ENTRIES)

    assert articles[0]["date"] == "2025-01-17"
    assert articles[1]["date"] == "2025-01-16"


def test_filter_china_related() -> None:
    """Test filtering for China-related content."""
    articles = _extract_articles_from_api(API_ENTRIES)
    relevant = _filter_china_related(articles, CHINA_KEYWORDS)

    assert len(relevant) >= 2
    for article in relevant:
        assert "matched_keywords" in article
        assert len(article["matched_keywords"]) > 0


def test_filter_china_related_excludes_irrelevant() -> None:
    """Test that non-China articles are excluded."""
    articles = _extract_articles_from_api(API_ENTRIES)
    relevant = _filter_china_related(articles, CHINA_KEYWORDS)

    titles = [a["title"] for a in relevant]
    assert "Development aid package for Southeast Asia" not in titles


def test_filter_bilingual_keywords() -> None:
    """Test that French keywords also match."""
    articles = [
        {
            "title": "Dialogue sur les droits avec la Chine",
            "body_snippet": "La ministre a discuté avec la RPC.",
            "date": "2025-01-17",
            "source_url": "https://example.com",
            "source": "Global Affairs Canada",
            "content_type": "news_release",
        }
    ]
    relevant = _filter_china_related(articles, CHINA_KEYWORDS)

    assert len(relevant) == 1
    assert any(
        "Chine" in kw or "RPC" in kw
        for kw in relevant[0]["matched_keywords"]
    )


def test_extract_empty_entries() -> None:
    """Test extraction from empty list."""
    articles = _extract_articles_from_api([])
    assert articles == []


API_RESPONSE_JSON = {
    "feed": {
        "entry": API_ENTRIES,
    }
}


@respx.mock
@pytest.mark.asyncio
async def test_fetch_success(gac_config: SourceConfig) -> None:
    """Test successful GAC fetch."""
    respx.get(
        "https://api.io.canada.ca/io-server/gc/news/en/v2"
    ).mock(return_value=httpx.Response(200, json=API_RESPONSE_JSON))
    respx.get(
        "https://api.io.canada.ca/io-server/gc/news/fr/v2"
    ).mock(return_value=httpx.Response(200, json={"feed": {"entry": []}}))

    result = await fetch(gac_config, "2025-01-17")

    assert result["date"] == "2025-01-17"
    assert result["total_scraped"] == 4
    assert result["total_relevant"] >= 2
    assert result["total_relevant"] <= result["total_scraped"]


@respx.mock
@pytest.mark.asyncio
async def test_fetch_partial_failure(gac_config: SourceConfig) -> None:
    """Test graceful handling when one language endpoint fails."""
    respx.get(
        "https://api.io.canada.ca/io-server/gc/news/en/v2"
    ).mock(return_value=httpx.Response(200, json=API_RESPONSE_JSON))
    respx.get(
        "https://api.io.canada.ca/io-server/gc/news/fr/v2"
    ).mock(return_value=httpx.Response(500))

    result = await fetch(gac_config, "2025-01-17")

    # Should still have English results
    assert result["total_scraped"] == 4
    assert "error" not in result
