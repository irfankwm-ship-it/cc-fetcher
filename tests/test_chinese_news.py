"""Tests for the Chinese-language news RSS fetcher."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx

from fetcher.config import RetryConfig, SourceConfig
from fetcher.sources.chinese_news import (
    _is_duplicate,
    _matches_keywords,
    _parse_feed,
    fetch,
)


@pytest.fixture
def chinese_news_config() -> SourceConfig:
    return SourceConfig(
        name="chinese_news",
        settings={
            "feeds": [
                {"url": "http://example.com/rss.xml", "name": "测试源"},
            ],
            "keywords": ["加拿大", "台湾", "关税", "半导体", "油菜籽"],
        },
        timeout=10,
        retry=RetryConfig(),
    )


@pytest.fixture
def chinese_rss_feed(fixtures_dir: Path) -> str:
    return (fixtures_dir / "chinese_rss_feed.xml").read_text()


def test_matches_keywords_finds_chinese() -> None:
    """Test Chinese keyword substring matching."""
    text = "中国外交部就加拿大涉台言论表态"
    matched = _matches_keywords(text, ["加拿大", "台湾", "香港"])
    assert "加拿大" in matched
    # "台湾" is not in the text (it says "涉台" not "台湾")
    assert "台湾" not in matched


def test_matches_keywords_empty_text() -> None:
    """Test keyword matching with empty text."""
    matched = _matches_keywords("", ["加拿大", "台湾"])
    assert matched == []


def test_is_duplicate_detects_similar() -> None:
    """Test near-duplicate detection."""
    seen = ["中国外交部就加拿大涉台言论表态"]
    assert _is_duplicate("中国外交部就加拿大涉台言论表态了", seen)


def test_is_duplicate_allows_different() -> None:
    """Test that genuinely different titles pass through."""
    seen = ["中国外交部就加拿大涉台言论表态"]
    assert not _is_duplicate("商务部公布对加拿大油菜籽反倾销调查结果", seen)


def test_parse_feed(chinese_rss_feed: str) -> None:
    """Test RSS feed parsing produces article dicts."""
    articles = _parse_feed(chinese_rss_feed, "新华社")

    assert len(articles) == 5
    for article in articles:
        assert "title" in article
        assert "url" in article
        assert article["source"] == "新华社"
        assert article["language"] == "zh"


def test_parse_feed_language_tag(chinese_rss_feed: str) -> None:
    """Test that all parsed articles are tagged with language: zh."""
    articles = _parse_feed(chinese_rss_feed, "测试源")
    for article in articles:
        assert article["language"] == "zh"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_filters_by_keywords(
    chinese_news_config: SourceConfig, chinese_rss_feed: str
) -> None:
    """Test that fetch filters articles by Chinese keywords."""
    respx.get("http://example.com/rss.xml").mock(
        return_value=httpx.Response(200, text=chinese_rss_feed)
    )
    # Mock article body fetches
    respx.get(url__regex=r".*xinhuanet\.com.*").mock(
        return_value=httpx.Response(200, text="<html><body><p>文章内容</p></body></html>")
    )

    result = await fetch(chinese_news_config, "2026-01-30")

    assert result["date"] == "2026-01-30"
    assert result["feeds_checked"] == 1
    # Should find articles with 加拿大, 油菜籽, 半导体 keywords
    assert result["total_articles"] > 0

    # All articles should have language: zh
    for article in result["articles"]:
        assert article["language"] == "zh"

    # Should include the 加拿大 and 油菜籽 articles
    titles = [a["title"] for a in result["articles"]]
    assert any("加拿大" in t for t in titles)


@respx.mock
@pytest.mark.asyncio
async def test_fetch_excludes_irrelevant(
    chinese_news_config: SourceConfig, chinese_rss_feed: str
) -> None:
    """Test that irrelevant articles are excluded."""
    respx.get("http://example.com/rss.xml").mock(
        return_value=httpx.Response(200, text=chinese_rss_feed)
    )
    # Mock article body fetches
    respx.get(url__regex=r".*xinhuanet\.com.*").mock(
        return_value=httpx.Response(200, text="<html><body><p>文章内容</p></body></html>")
    )

    result = await fetch(chinese_news_config, "2026-01-30")

    # Spring festival / tourism articles should not match
    titles = [a["title"] for a in result["articles"]]
    assert not any("旅游" in t for t in titles)
    assert not any("春节联欢" in t for t in titles)


@respx.mock
@pytest.mark.asyncio
async def test_fetch_handles_feed_error(chinese_news_config: SourceConfig) -> None:
    """Test graceful handling of feed errors."""
    respx.get("http://example.com/rss.xml").mock(
        return_value=httpx.Response(500)
    )

    result = await fetch(chinese_news_config, "2026-01-30")

    assert result["total_articles"] == 0
    assert len(result["feed_errors"]) > 0


@respx.mock
@pytest.mark.asyncio
async def test_fetch_handles_timeout(chinese_news_config: SourceConfig) -> None:
    """Test graceful handling of timeouts."""
    respx.get("http://example.com/rss.xml").mock(
        side_effect=httpx.ConnectTimeout("Timed out")
    )

    result = await fetch(chinese_news_config, "2026-01-30")

    assert result["total_articles"] == 0
    assert len(result["feed_errors"]) > 0
