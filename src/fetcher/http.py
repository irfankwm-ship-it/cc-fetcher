"""Retry-enabled HTTP client for fetchers.

Wraps httpx.AsyncClient with configurable retry logic for
transient failures (429, 500, 502, 503, 504).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from fetcher.config import RetryConfig

logger = logging.getLogger(__name__)

DEFAULT_RETRY = RetryConfig()


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
