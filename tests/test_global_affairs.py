"""Tests for the Global Affairs Canada source fetcher."""

from __future__ import annotations

import httpx
import pytest
import respx

from fetcher.config import RetryConfig, SourceConfig
from fetcher.sources.global_affairs import (
    CHINA_KEYWORDS,
    _extract_articles_from_html,
    _filter_china_related,
    fetch,
)


@pytest.fixture
def gac_config() -> SourceConfig:
    return SourceConfig(
        name="global_affairs",
        settings={"url": "https://www.international.gc.ca/news-nouvelles/"},
        timeout=10,
        retry=RetryConfig(),
    )


GAC_HTML = """
<!DOCTYPE html>
<html lang="en">
<head><title>Global Affairs Canada - News</title></head>
<body>
<main id="wb-main">
    <article>
        <a href="/news-nouvelles/statements/2025-01-17-china-hr.html">
            Statement by Minister of Foreign Affairs on China human rights dialogue
        </a>
        <p>The Minister issued a statement regarding Canada's engagement
        with China on human rights.</p>
        <time datetime="2025-01-17">January 17, 2025</time>
    </article>
    <article>
        <a href="/news-nouvelles/travel/2025-01-16-china-advisory.html">
            Canada updates travel advisory for China
        </a>
        <p>Global Affairs Canada updated the travel advisory for the PRC.</p>
        <time datetime="2025-01-16">January 16, 2025</time>
    </article>
    <article>
        <a href="/news-nouvelles/press/2025-01-15-eu-indopacific.html">
            Press release: Canada-EU joint statement on Indo-Pacific engagement
        </a>
        <p>Canada and the EU affirmed commitment to a free and open Indo-Pacific region.</p>
        <time datetime="2025-01-15">January 15, 2025</time>
    </article>
    <article>
        <a href="/news-nouvelles/press/2025-01-14-sea-aid.html">
            Canada announces development aid package for Southeast Asia
        </a>
        <p>$200 million development assistance package for climate resilience.</p>
        <time datetime="2025-01-14">January 14, 2025</time>
    </article>
    <article>
        <a href="/news-nouvelles/statements/2025-01-13-arctic.html">
            Minister participates in Arctic Council ministerial meeting
        </a>
        <p>Foreign Affairs Minister at Arctic Council discussing environmental protection.</p>
        <time datetime="2025-01-13">January 13, 2025</time>
    </article>
</main>
</body>
</html>
"""


def test_extract_articles_from_html() -> None:
    """Test HTML extraction from GAC page."""
    articles = _extract_articles_from_html(
        GAC_HTML, "https://www.international.gc.ca/news-nouvelles/"
    )

    assert len(articles) == 5
    for article in articles:
        assert "title" in article
        assert "source_url" in article
        assert article["source"] == "Global Affairs Canada"


def test_extract_articles_has_dates() -> None:
    """Test that dates are extracted from articles."""
    articles = _extract_articles_from_html(
        GAC_HTML, "https://www.international.gc.ca/news-nouvelles/"
    )

    dated = [a for a in articles if a["date"]]
    assert len(dated) > 0


def test_filter_china_related() -> None:
    """Test filtering for China-related content."""
    articles = _extract_articles_from_html(
        GAC_HTML, "https://www.international.gc.ca/news-nouvelles/"
    )
    relevant = _filter_china_related(articles, CHINA_KEYWORDS)

    # Should match: China HR statement, China travel advisory, Indo-Pacific statement
    assert len(relevant) >= 2
    for article in relevant:
        assert "matched_keywords" in article
        assert len(article["matched_keywords"]) > 0


def test_filter_china_related_excludes_irrelevant() -> None:
    """Test that non-China articles are excluded."""
    articles = _extract_articles_from_html(
        GAC_HTML, "https://www.international.gc.ca/news-nouvelles/"
    )
    relevant = _filter_china_related(articles, CHINA_KEYWORDS)

    titles = [a["title"] for a in relevant]
    # Arctic and SE Asia articles should not appear (unless they mention China keywords)
    for title in titles:
        text = f"{title}".lower()
        assert any(kw.lower() in text for kw in CHINA_KEYWORDS) or any(
            kw.lower() in a.get("body_snippet", "").lower()
            for a in relevant
            if a["title"] == title
            for kw in CHINA_KEYWORDS
        )


def test_filter_bilingual_keywords() -> None:
    """Test that French keywords also match."""
    articles = [
        {
            "title": "Dialogue sur les droits avec la Chine",
            "body_snippet": "La ministre a discute avec la RPC.",
            "date": "2025-01-17",
            "source_url": "https://example.com",
            "source": "Global Affairs Canada",
            "content_type": "statement",
        }
    ]
    relevant = _filter_china_related(articles, CHINA_KEYWORDS)

    assert len(relevant) == 1
    assert any("Chine" in kw or "RPC" in kw for kw in relevant[0]["matched_keywords"])


def test_extract_articles_from_empty_html() -> None:
    """Test extraction from empty page."""
    articles = _extract_articles_from_html(
        "<html><body></body></html>", "https://example.com"
    )
    assert articles == []


@respx.mock
@pytest.mark.asyncio
async def test_fetch_success(gac_config: SourceConfig) -> None:
    """Test successful GAC fetch."""
    respx.get("https://www.international.gc.ca/news-nouvelles/").mock(
        return_value=httpx.Response(200, text=GAC_HTML)
    )

    result = await fetch(gac_config, "2025-01-17")

    assert result["date"] == "2025-01-17"
    assert result["total_scraped"] > 0
    assert result["total_relevant"] >= 0
    assert result["total_relevant"] <= result["total_scraped"]


@respx.mock
@pytest.mark.asyncio
async def test_fetch_http_error(gac_config: SourceConfig) -> None:
    """Test graceful handling of HTTP errors."""
    respx.get("https://www.international.gc.ca/news-nouvelles/").mock(
        return_value=httpx.Response(500)
    )

    result = await fetch(gac_config, "2025-01-17")

    assert "error" in result
    assert result["articles"] == []


@respx.mock
@pytest.mark.asyncio
async def test_fetch_timeout(gac_config: SourceConfig) -> None:
    """Test graceful handling of timeouts."""
    respx.get("https://www.international.gc.ca/news-nouvelles/").mock(
        side_effect=httpx.ReadTimeout("Read timed out")
    )

    result = await fetch(gac_config, "2025-01-17")

    assert "error" in result
    assert result["articles"] == []
