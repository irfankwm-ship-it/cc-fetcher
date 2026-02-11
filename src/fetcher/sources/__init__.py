"""Data source fetcher modules for the China Compass pipeline."""

from __future__ import annotations

import importlib
import logging
import pkgutil
from typing import TYPE_CHECKING

from fetcher.models import FetchResult, wrap_result
from fetcher.sources._registry import SOURCE_REGISTRY, register_source

if TYPE_CHECKING:
    from fetcher.config import SourceConfig

logger = logging.getLogger(__name__)

__all__ = ["SOURCE_REGISTRY", "register_source", "run_source", "FetchResult", "wrap_result"]


def _discover_sources() -> None:
    """Auto-import all source modules so their @register_source decorators fire."""
    package_path = __path__  # type: ignore[name-defined]
    for module_info in pkgutil.iter_modules(package_path):
        if module_info.name.startswith("_"):
            continue
        try:
            importlib.import_module(f"fetcher.sources.{module_info.name}")
        except Exception:
            logger.exception("Failed to import source module: %s", module_info.name)


# Populate the registry on first import
_discover_sources()


async def run_source(name: str, config: SourceConfig, date: str, **kwargs) -> FetchResult:
    """Run a source fetcher by name.

    Args:
        name: Registered source name.
        config: Source-specific configuration.
        date: Target date string (YYYY-MM-DD).
        **kwargs: Additional keyword arguments passed to the fetch function
                  (e.g. ``client``, ``limiter``).

    Returns:
        A :class:`FetchResult` wrapping the source data.

    Raises:
        KeyError: If the source name is not registered.
    """
    if name not in SOURCE_REGISTRY:
        raise KeyError(f"Unknown source: '{name}'. Available: {list(SOURCE_REGISTRY.keys())}")

    raw = await SOURCE_REGISTRY[name](config, date, **kwargs)
    return wrap_result(name, date, raw)
