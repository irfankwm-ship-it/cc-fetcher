"""Shared test fixtures for cc-fetcher tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from fetcher.config import AppConfig, RetryConfig, SourceConfig

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir() -> Path:
    """Path to the test fixtures directory."""
    return FIXTURES_DIR


def _load_json_fixture(name: str) -> dict[str, Any]:
    """Load a JSON fixture file by name."""
    path = FIXTURES_DIR / name
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _load_text_fixture(name: str) -> str:
    """Load a text fixture file by name."""
    path = FIXTURES_DIR / name
    with open(path, encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def parliament_response() -> dict[str, Any]:
    """Fixture: LEGISinfo API response."""
    return _load_json_fixture("parliament_response.json")


@pytest.fixture
def statcan_response() -> dict[str, Any]:
    """Fixture: Statistics Canada API response."""
    return _load_json_fixture("statcan_response.json")


@pytest.fixture
def yahoo_finance_response() -> dict[str, Any]:
    """Fixture: Yahoo Finance response data."""
    return _load_json_fixture("yahoo_finance_response.json")


@pytest.fixture
def rss_feed_xml() -> str:
    """Fixture: RSS feed XML content."""
    return _load_text_fixture("rss_feed.xml")


@pytest.fixture
def xinhua_page_html() -> str:
    """Fixture: Xinhua English page HTML."""
    return _load_text_fixture("xinhua_page.html")


@pytest.fixture
def global_affairs_response() -> dict[str, Any]:
    """Fixture: Global Affairs Canada response."""
    return _load_json_fixture("global_affairs_response.json")


@pytest.fixture
def dev_source_config() -> SourceConfig:
    """A default dev SourceConfig for testing."""
    return SourceConfig(
        name="test_source",
        settings={
            "legisinfo_url": "https://www.parl.ca/legisinfo/en/api/bills",
            "ourcommons_url": "https://api.ourcommons.ca/api/v1",
            "keywords": ["China", "Beijing", "PRC", "Huawei", "canola"],
        },
        timeout=30,
        retry=RetryConfig(),
    )


@pytest.fixture
def dev_app_config(dev_source_config: SourceConfig) -> AppConfig:
    """A dev AppConfig for testing."""
    return AppConfig(
        env="dev",
        sources={
            "parliament": dev_source_config,
            "statcan": SourceConfig(
                name="statcan",
                settings={
                    "base_url": "https://www150.statcan.gc.ca/t1/tbl1/en/dtl",
                    "table_id": "12-10-0011-01",
                },
                timeout=60,
            ),
            "yahoo_finance": SourceConfig(
                name="yahoo_finance",
                settings={
                    "indices": [
                        {"ticker": "000001.SS", "name": "Shanghai Composite"},
                        {"ticker": "^HSI", "name": "Hang Seng"},
                    ],
                },
                timeout=30,
            ),
            "news": SourceConfig(
                name="news",
                settings={
                    "feeds": [
                        {"url": "https://feeds.reuters.com/reuters/worldNews", "name": "Reuters"},
                    ],
                    "keywords": ["China", "Beijing"],
                },
                timeout=30,
            ),
            "xinhua": SourceConfig(
                name="xinhua",
                settings={"url": "http://www.xinhuanet.com/english/"},
                timeout=30,
            ),
            "global_affairs": SourceConfig(
                name="global_affairs",
                settings={"url": "https://www.international.gc.ca/news-nouvelles/"},
                timeout=30,
            ),
        },
    )


@pytest.fixture
def tmp_output_dir(tmp_path: Path) -> Path:
    """Temporary output directory for tests."""
    out = tmp_path / "cc-data" / "raw"
    out.mkdir(parents=True)
    return out
