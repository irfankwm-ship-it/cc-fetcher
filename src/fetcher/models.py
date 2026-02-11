"""Structured result types for the fetcher pipeline.

Provides ``FetchResult`` — a typed wrapper around the raw dicts returned
by source ``fetch()`` functions.  The ``to_dict()`` method converts back
to the plain dict shape that ``write_raw`` (and downstream cc-analysis)
already expect, so the change is fully backward-compatible.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FetchResult:
    """Structured result from a source fetch.

    Attributes:
        source: Registered source name (e.g. ``"parliament"``).
        date: Target date string (YYYY-MM-DD).
        articles: List of article dicts (for news-type sources).
        data: Arbitrary structured data dict (for API-type sources
              like statcan, yahoo_finance).
        metadata: Operational metadata (timing, counts, errors).
    """

    source: str
    date: str
    articles: list[dict[str, Any]] = field(default_factory=list)
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to the plain dict shape expected by ``write_raw``.

        Article-type results keep ``articles`` at the top level.
        Structured-data results spread their keys at the top level.
        Operational metadata goes under ``fetch_metadata``.
        """
        result: dict[str, Any] = {}

        # Spread structured data first (statcan, yahoo_finance, parliament)
        if self.data:
            result.update(self.data)

        # Articles go at top level if present
        if self.articles:
            result["articles"] = self.articles

        # Operational metadata under a namespaced key
        if self.metadata:
            result["fetch_metadata"] = self.metadata

        return result


def wrap_result(source: str, date: str, raw: dict[str, Any]) -> FetchResult:
    """Wrap a raw source dict into a ``FetchResult``.

    Detects the shape of the raw data:
      - Has ``articles`` key → article-type source.
      - Otherwise → structured-data source.

    The raw dict is split into ``articles``, ``data``, and leftover keys
    are kept in ``data`` so that ``to_dict()`` reproduces the original.
    """
    articles = raw.pop("articles", []) if isinstance(raw.get("articles"), list) else []

    return FetchResult(
        source=source,
        date=date,
        articles=articles,
        data=raw,  # everything else
    )
