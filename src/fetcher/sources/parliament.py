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

DEFAULT_KEYWORDS = [
    "China", "Chinese", "Beijing", "PRC",
    "Huawei", "canola", "Taiwan", "Hong Kong",
    "Indo-Pacific", "Uyghur", "Xinjiang", "Tibet",
    "foreign interference", "TikTok",
]
TRACKED_BILLS = ["C-27", "C-34", "C-70", "C-16", "S-7"]
PARLIAMENT_SESSION = "45-1"

OPEN_PARLIAMENT_BASE = "https://api.openparliament.ca"


async def _fetch_bills(
    client: httpx.AsyncClient,
    base_url: str,
    session: str,
    timeout: int,
) -> list[dict[str, Any]]:
    """Fetch bill status from Open Parliament API.

    Tries the current session first, then falls back to 44-1 for older bills.
    Returns a list of bill records with id, title, status.
    """
    bills: list[dict[str, Any]] = []
    sessions_to_try = [session, "44-1"] if session != "44-1" else [session]

    for bill_id in TRACKED_BILLS:
        found = False
        for try_session in sessions_to_try:
            if found:
                break
            try:
                url = f"{base_url}/bills/{try_session}/{bill_id}/?format=json"
                resp = await client.get(url, timeout=timeout)

                if resp.status_code == 404:
                    logger.info("Bill %s not found in session %s", bill_id, try_session)
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
                    "session": try_session,
                    "sponsor": data.get("sponsor_politician_url", ""),
                })
                found = True
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

    Fetches speeches from the debate via the speeches endpoint and counts
    keyword occurrences in the actual speech content.
    """
    counts: dict[str, int] = {kw: 0 for kw in keywords}
    try:
        # First get the debate detail to find the speeches URL
        resp = await client.get(
            f"{base_url}{debate_url}?format=json",
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()

        speeches_url = data.get("related", {}).get("speeches_url", "")
        if not speeches_url:
            logger.warning("No speeches_url for debate %s", debate_url)
            return counts

        # Fetch speeches (paginated, get up to 500 per debate)
        all_text_parts: list[str] = []
        sep = "&" if "?" in speeches_url else "?"
        next_url = f"{base_url}{speeches_url}{sep}format=json&limit=200"

        pages_fetched = 0
        while next_url and pages_fetched < 3:
            pages_fetched += 1
            try:
                speeches_resp = await client.get(next_url, timeout=timeout)
                speeches_resp.raise_for_status()
                speeches_data = speeches_resp.json()
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                logger.warning("Failed to fetch speeches page: %s", exc)
                break

            for speech in speeches_data.get("objects", []):
                # Speech content is in content.en / content.fr (HTML)
                content = speech.get("content", {})
                if isinstance(content, dict):
                    all_text_parts.append(content.get("en", ""))
                    all_text_parts.append(content.get("fr", ""))
                elif isinstance(content, str):
                    all_text_parts.append(content)

            # Follow pagination
            pagination = speeches_data.get("pagination", {})
            next_page = pagination.get("next_url", "")
            if next_page:
                next_url = f"{base_url}{next_page}"
            else:
                break

        combined_text = " ".join(all_text_parts).lower()
        for kw in keywords:
            counts[kw] = combined_text.count(kw.lower())

        logger.info(
            "Debate %s: %d speech segments, %d keyword matches",
            debate_url, len(all_text_parts) // 2, sum(counts.values()),
        )

    except (httpx.HTTPStatusError, httpx.RequestError) as exc:
        logger.warning("Failed to fetch debate %s: %s", debate_url, exc)
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
