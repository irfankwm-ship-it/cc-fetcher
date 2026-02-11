"""Tests for FetchResult and wrap_result."""

from __future__ import annotations

from fetcher.models import FetchResult, wrap_result


def test_fetch_result_to_dict_articles():
    """Article-type result includes articles at top level."""
    result = FetchResult(
        source="news",
        date="2025-01-17",
        articles=[{"title": "Test article"}],
        data={"total_articles": 1, "date": "2025-01-17"},
    )
    d = result.to_dict()
    assert d["articles"] == [{"title": "Test article"}]
    assert d["total_articles"] == 1
    assert d["date"] == "2025-01-17"


def test_fetch_result_to_dict_structured():
    """Structured-data result (no articles) spreads data at top level."""
    result = FetchResult(
        source="statcan",
        date="2025-01-17",
        data={
            "imports_cad_millions": 100,
            "exports_cad_millions": 50,
            "balance_cad_millions": -50,
        },
    )
    d = result.to_dict()
    assert d["imports_cad_millions"] == 100
    assert d["exports_cad_millions"] == 50
    assert "articles" not in d


def test_fetch_result_to_dict_metadata():
    """Operational metadata appears under fetch_metadata key."""
    result = FetchResult(
        source="news",
        date="2025-01-17",
        metadata={"duration_s": 3.2, "articles_enriched": 5},
    )
    d = result.to_dict()
    assert d["fetch_metadata"]["duration_s"] == 3.2


def test_fetch_result_to_dict_empty():
    """Empty result produces empty dict."""
    result = FetchResult(source="test", date="2025-01-17")
    d = result.to_dict()
    assert d == {}


def test_wrap_result_article_source():
    """wrap_result splits articles out of raw dict."""
    raw = {
        "date": "2025-01-17",
        "articles": [{"title": "A"}, {"title": "B"}],
        "total_articles": 2,
        "feeds_checked": 5,
    }
    result = wrap_result("news", "2025-01-17", raw)
    assert result.source == "news"
    assert len(result.articles) == 2
    assert result.data["total_articles"] == 2
    assert "articles" not in result.data


def test_wrap_result_structured_source():
    """wrap_result keeps everything in data for non-article sources."""
    raw = {
        "date": "2025-01-17",
        "imports_cad_millions": 100,
        "series": {},
        "commodities": [],
    }
    result = wrap_result("statcan", "2025-01-17", raw)
    assert result.source == "statcan"
    assert result.articles == []
    assert result.data["imports_cad_millions"] == 100


def test_wrap_result_roundtrip_articles():
    """to_dict of wrapped article source reproduces original shape."""
    raw = {
        "date": "2025-01-17",
        "articles": [{"title": "Test"}],
        "total_articles": 1,
    }
    # wrap_result pops articles, so we keep a copy
    expected_articles = [{"title": "Test"}]
    result = wrap_result("news", "2025-01-17", raw)
    d = result.to_dict()
    assert d["articles"] == expected_articles
    assert d["total_articles"] == 1
    assert d["date"] == "2025-01-17"


def test_wrap_result_roundtrip_structured():
    """to_dict of wrapped structured source reproduces original shape."""
    raw = {
        "date": "2025-01-17",
        "indices": [{"name": "SSE"}],
        "summary": {"count": 1},
    }
    result = wrap_result("yahoo_finance", "2025-01-17", dict(raw))
    d = result.to_dict()
    assert d["indices"] == [{"name": "SSE"}]
    assert d["summary"] == {"count": 1}
