"""Retry-enabled HTTP client for fetchers.

Wraps httpx.AsyncClient with configurable retry logic for
transient failures (429, 500, 502, 503, 504).

Also provides :class:`DomainRateLimiter` for limiting concurrent
requests to the same domain across sources.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import urlparse

import httpx

from fetcher.config import RetryConfig

logger = logging.getLogger(__name__)

DEFAULT_RETRY = RetryConfig()


class DomainRateLimiter:
    """Limit concurrent requests per domain.

    Each domain gets its own :class:`asyncio.Semaphore` capped at
    *max_per_domain* concurrent requests.  Semaphores are created lazily
    on first use and reused thereafter.

    Usage::

        limiter = DomainRateLimiter(max_per_domain=5)
        async with limiter.acquire("https://example.com/page"):
            resp = await client.get(...)
    """

    def __init__(self, max_per_domain: int = 5) -> None:
        self._max = max_per_domain
        self._semaphores: dict[str, asyncio.Semaphore] = {}

    def _domain(self, url: str) -> str:
        """Extract the domain (netloc) from a URL."""
        return urlparse(url).netloc.lower()

    @asynccontextmanager
    async def acquire(self, url: str):
        """Async context manager that acquires the per-domain semaphore."""
        domain = self._domain(url)
        if domain not in self._semaphores:
            self._semaphores[domain] = asyncio.Semaphore(self._max)
        async with self._semaphores[domain]:
            yield


async def request_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    retry: RetryConfig = DEFAULT_RETRY,
    **kwargs: Any,
) -> httpx.Response:
    """Make an HTTP request with retry on transient failures.

    Args:
        client: httpx async client instance.
        method: HTTP method (GET, POST, etc.).
        url: Request URL.
        retry: Retry configuration.
        **kwargs: Additional arguments passed to client.request().

    Returns:
        httpx.Response on success.

    Raises:
        httpx.HTTPStatusError: If all retries exhausted on HTTP error.
        httpx.RequestError: If all retries exhausted on connection error.
    """
    last_exc: Exception | None = None

    for attempt in range(retry.max_retries + 1):
        try:
            resp = await client.request(method, url, **kwargs)

            if resp.status_code in retry.retry_statuses and attempt < retry.max_retries:
                wait = retry.backoff_factor * (2 ** attempt)
                logger.warning(
                    "HTTP %d from %s (attempt %d/%d), retrying in %.1fs",
                    resp.status_code, url[:80], attempt + 1, retry.max_retries + 1, wait,
                )
                await asyncio.sleep(wait)
                continue

            resp.raise_for_status()
            return resp

        except httpx.RequestError as exc:
            last_exc = exc
            if attempt < retry.max_retries:
                wait = retry.backoff_factor * (2 ** attempt)
                logger.warning(
                    "Request error for %s (attempt %d/%d): %s, retrying in %.1fs",
                    url[:80], attempt + 1, retry.max_retries + 1, exc, wait,
                )
                await asyncio.sleep(wait)
            else:
                raise

    # Should not reach here, but just in case
    if last_exc:
        raise last_exc
    raise RuntimeError("Retry loop exited unexpectedly")
