"""Fetcher for Canadian parliamentary data via Open Parliament API.

Sources:
  - api.openparliament.ca/bills/ for bill status tracking
  - api.openparliament.ca/debates/ for Hansard debate content

Tracked bills: C-27, S-7, C-34, C-70, Motion M-62
Keywords: China, Beijing, PRC, Huawei, canola
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from fetcher.config import SourceConfig

logger = logging.getLogger(__name__)

DEFAULT_KEYWORDS = ["China", "Beijing", "PRC", "Huawei", "canola"]
TRACKED_BILLS = ["C-27", "S-7", "C-34", "C-70", "M-62"]
PARLIAMENT_SESSION = "44-1"

OPEN_PARLIAMENT_BASE = "https://api.openparliament.ca"


async def _fetch_bills(
    client: httpx.AsyncClient,
    base_url: str,
    session: str,
    timeout: int,
) -> list[dict[str, Any]]:
    """Fetch bill status from Open Parliament API.

    Returns a list of bill records with id, title, status.
    """
    bills: list[dict[str, Any]] = []

    for bill_id in TRACKED_BILLS:
        try:
            url = f"{base_url}/bills/{session}/{bill_id}/?format=json"
            resp = await client.get(url, timeout=timeout)

            if resp.status_code == 404:
                logger.info("Bill %s not found in session %s", bill_id, session)
                continue

            resp.raise_for_status()
            data = resp.json()

            name = data.get("name", {})
            bills.append({
                "id": bill_id,
                "title": name.get("en", "") if isinstance(name, dict) else str(name),
                "title_fr": name.get("fr", "") if isinstance(name, dict) else "",
                "status": data.get("status_code", ""),
                "introduced": data.get("introduced", ""),
                "session": session,
                "sponsor": data.get("sponsor_politician_url", ""),
            })
        except httpx.HTTPStatusError as exc:
            logger.warning("Failed to fetch bill %s: HTTP %s", bill_id, exc.response.status_code)
        except httpx.RequestError as exc:
            logger.warning("Request error fetching bill %s: %s", bill_id, exc)

    return bills


async def _fetch_recent_debates(
    client: httpx.AsyncClient,
    base_url: str,
    timeout: int,
    limit: int = 5,
) -> list[dict[str, str]]:
    """Fetch recent debate session dates."""
    try:
        resp = await client.get(
            f"{base_url}/debates/?format=json",
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        sessions = data.get("objects", [])[:limit]
        return [{"date": s.get("date", ""), "url": s.get("url", "")} for s in sessions]
    except (httpx.HTTPStatusError, httpx.RequestError) as exc:
        logger.warning("Failed to fetch debate list: %s", exc)
        return []


async def _search_debate_content(
    client: httpx.AsyncClient,
    base_url: str,
    debate_url: str,
    keywords: list[str],
    timeout: int,
) -> dict[str, int]:
    """Search a single debate session for keyword mentions.

    Fetches the debate page and counts keyword occurrences in the text.
    """
    counts: dict[str, int] = {}
    try:
        resp = await client.get(
            f"{base_url}{debate_url}?format=json",
            timeout=timeout,
        )
        resp.raise_for_status()
        text = resp.text.lower()
        for kw in keywords:
            counts[kw] = text.count(kw.lower())
    except (httpx.HTTPStatusError, httpx.RequestError) as exc:
        logger.warning("Failed to fetch debate %s: %s", debate_url, exc)
        for kw in keywords:
            counts[kw] = 0
    return counts


async def fetch(config: SourceConfig, date: str) -> dict[str, Any]:
    """Fetch all parliamentary data for the given date.

    Args:
        config: Source configuration with URLs, timeout, keywords.
        date: Target date (YYYY-MM-DD).

    Returns:
        Dictionary with 'bills' list and 'hansard_stats'.
    """
    base_url = config.get("base_url", OPEN_PARLIAMENT_BASE)
    session = config.get("session", PARLIAMENT_SESSION)
    keywords = config.get("keywords", DEFAULT_KEYWORDS)
    timeout = config.timeout

    async with httpx.AsyncClient(follow_redirects=True) as client:
        # Fetch bills and recent debates in parallel
        bills_task = _fetch_bills(client, base_url, session, timeout)
        debates_task = _fetch_recent_debates(client, base_url, timeout)
        bills, recent_debates = await asyncio.gather(bills_task, debates_task)

        # Search recent debates for keyword mentions
        keyword_totals: dict[str, int] = {kw: 0 for kw in keywords}
        for debate in recent_debates:
            counts = await _search_debate_content(
                client, base_url, debate["url"], keywords, timeout,
            )
            for kw, count in counts.items():
                keyword_totals[kw] = keyword_totals.get(kw, 0) + count

    total_mentions = sum(keyword_totals.values())

    return {
        "date": date,
        "bills": bills,
        "hansard_stats": {
            "by_keyword": keyword_totals,
            "total_mentions": total_mentions,
            "debates_searched": len(recent_debates),
        },
        "tracked_bills": TRACKED_BILLS,
        "keywords": keywords,
    }
