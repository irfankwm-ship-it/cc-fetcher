"""Tests for DomainRateLimiter."""

from __future__ import annotations

import asyncio

import pytest

from fetcher.http import DomainRateLimiter


@pytest.fixture
def limiter() -> DomainRateLimiter:
    return DomainRateLimiter(max_per_domain=2)


async def test_semaphore_created_per_domain(limiter: DomainRateLimiter) -> None:
    """Each unique domain gets its own semaphore."""
    async with limiter.acquire("https://example.com/a"):
        pass
    async with limiter.acquire("https://other.com/b"):
        pass

    assert "example.com" in limiter._semaphores
    assert "other.com" in limiter._semaphores
    assert len(limiter._semaphores) == 2


async def test_semaphore_reused_for_same_domain(limiter: DomainRateLimiter) -> None:
    """Same domain reuses its semaphore (not recreated)."""
    async with limiter.acquire("https://example.com/page1"):
        pass
    sem = limiter._semaphores["example.com"]
    async with limiter.acquire("https://example.com/page2"):
        pass
    assert limiter._semaphores["example.com"] is sem


async def test_concurrent_limit_enforced(limiter: DomainRateLimiter) -> None:
    """At most max_per_domain tasks run concurrently for one domain."""
    concurrent = 0
    max_concurrent = 0

    async def worker(url: str) -> None:
        nonlocal concurrent, max_concurrent
        async with limiter.acquire(url):
            concurrent += 1
            max_concurrent = max(max_concurrent, concurrent)
            await asyncio.sleep(0.05)
            concurrent -= 1

    # Launch 5 workers against the same domain (limit=2)
    await asyncio.gather(*[
        worker("https://example.com/page") for _ in range(5)
    ])

    assert max_concurrent <= 2


async def test_different_domains_independent(limiter: DomainRateLimiter) -> None:
    """Different domains are not blocked by each other."""
    concurrent_a = 0
    concurrent_b = 0
    max_concurrent_a = 0
    max_concurrent_b = 0

    async def worker_a() -> None:
        nonlocal concurrent_a, max_concurrent_a
        async with limiter.acquire("https://a.com/x"):
            concurrent_a += 1
            max_concurrent_a = max(max_concurrent_a, concurrent_a)
            await asyncio.sleep(0.05)
            concurrent_a -= 1

    async def worker_b() -> None:
        nonlocal concurrent_b, max_concurrent_b
        async with limiter.acquire("https://b.com/x"):
            concurrent_b += 1
            max_concurrent_b = max(max_concurrent_b, concurrent_b)
            await asyncio.sleep(0.05)
            concurrent_b -= 1

    # 2 workers each domain — should all run concurrently (limit=2 per domain)
    await asyncio.gather(
        worker_a(), worker_a(),
        worker_b(), worker_b(),
    )

    assert max_concurrent_a <= 2
    assert max_concurrent_b <= 2
