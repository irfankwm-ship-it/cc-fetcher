"""Data source fetcher modules for the China Compass pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fetcher.config import SourceConfig

# Registry mapping source names to their fetch functions.
# Each fetch function has the signature:
#   async def fetch(config: SourceConfig, date: str) -> dict
SOURCE_REGISTRY: dict[str, str] = {
    "parliament": "fetcher.sources.parliament",
    "statcan": "fetcher.sources.statcan",
    "yahoo_finance": "fetcher.sources.yahoo_finance",
    "news": "fetcher.sources.news_scraper",
    "xinhua": "fetcher.sources.xinhua",
    "global_affairs": "fetcher.sources.global_affairs",
    "mfa": "fetcher.sources.mfa",
    "mofcom": "fetcher.sources.mofcom",
    "chinese_news": "fetcher.sources.chinese_news",
    "caixin": "fetcher.sources.caixin_scraper",
    "thepaper": "fetcher.sources.thepaper_scraper",
}


async def run_source(name: str, config: SourceConfig, date: str) -> dict:
    """Dynamically import and run a source fetcher by name.

    Args:
        name: Registered source name.
        config: Source-specific configuration.
        date: Target date string (YYYY-MM-DD).

    Returns:
        Raw data dictionary from the source.

    Raises:
        KeyError: If the source name is not registered.
    """
    if name not in SOURCE_REGISTRY:
        raise KeyError(f"Unknown source: '{name}'. Available: {list(SOURCE_REGISTRY.keys())}")

    import importlib

    module = importlib.import_module(SOURCE_REGISTRY[name])
    return await module.fetch(config, date)
