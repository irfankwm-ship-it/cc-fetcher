"""Tests for the parliament source fetcher."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import respx

from fetcher.config import RetryConfig, SourceConfig
from fetcher.sources.parliament import TRACKED_BILLS, fetch


@pytest.fixture
def parliament_config() -> SourceConfig:
    return SourceConfig(
        name="parliament",
        settings={
            "legisinfo_url": "https://www.parl.ca/legisinfo/en/api/bills",
            "ourcommons_url": "https://api.ourcommons.ca/api/v1",
            "keywords": ["China", "Beijing", "PRC", "Huawei", "canola"],
        },
        timeout=10,
        retry=RetryConfig(),
    )


@pytest.fixture
def bill_response() -> dict[str, Any]:
    return {
        "Items": [
            {
                "ShortTitle": "Digital Charter Implementation Act, 2022",
                "StatusName": "Senate - Second Reading",
                "LatestCompletedMajorStageEn": "Passed House of Commons",
                "ParliamentSession": "44-1",
                "SponsorName": "Hon. Francois-Philippe Champagne",
            }
        ]
    }


@pytest.fixture
def hansard_response() -> dict[str, Any]:
    return {
        "TotalCount": 47,
        "Items": [{"id": i} for i in range(47)],
    }


@respx.mock
@pytest.mark.asyncio
async def test_fetch_returns_bills_and_hansard(
    parliament_config: SourceConfig,
    bill_response: dict[str, Any],
    hansard_response: dict[str, Any],
) -> None:
    """Test that fetch returns both bills and hansard_stats."""
    # Mock bill endpoints for each tracked bill
    for bill_id in TRACKED_BILLS:
        respx.get(
            "https://www.parl.ca/legisinfo/en/api/bills",
            params={"billNumber": bill_id, "language": "en"},
        ).mock(return_value=httpx.Response(200, json=bill_response))

    # Mock hansard endpoints for each keyword
    for keyword in ["China", "Beijing", "PRC", "Huawei", "canola"]:
        respx.get(
            "https://api.ourcommons.ca/api/v1/Debates",
            params={"keyword": keyword, "language": "en"},
        ).mock(return_value=httpx.Response(200, json=hansard_response))

    result = await fetch(parliament_config, "2025-01-17")

    assert "bills" in result
    assert "hansard_stats" in result
    assert len(result["bills"]) == len(TRACKED_BILLS)
    assert result["hansard_stats"]["total_mentions"] > 0
    assert result["date"] == "2025-01-17"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_handles_bill_http_error(
    parliament_config: SourceConfig,
    hansard_response: dict[str, Any],
) -> None:
    """Test that a failed bill request doesn't crash the whole fetch."""
    # First bill fails, rest succeed
    first = True
    for bill_id in TRACKED_BILLS:
        if first:
            respx.get(
                "https://www.parl.ca/legisinfo/en/api/bills",
                params={"billNumber": bill_id, "language": "en"},
            ).mock(return_value=httpx.Response(500))
            first = False
        else:
            respx.get(
                "https://www.parl.ca/legisinfo/en/api/bills",
                params={"billNumber": bill_id, "language": "en"},
            ).mock(
                return_value=httpx.Response(
                    200,
                    json={
                        "Items": [
                            {
                                "ShortTitle": "Test",
                                "StatusName": "Active",
                                "LatestCompletedMajorStageEn": "Committee",
                                "ParliamentSession": "44-1",
                                "SponsorName": "Test",
                            }
                        ]
                    },
                )
            )

    for keyword in parliament_config.get("keywords", []):
        respx.get(
            "https://api.ourcommons.ca/api/v1/Debates",
            params={"keyword": keyword, "language": "en"},
        ).mock(return_value=httpx.Response(200, json=hansard_response))

    result = await fetch(parliament_config, "2025-01-17")

    # Should still return results for the successful bills
    assert len(result["bills"]) == len(TRACKED_BILLS) - 1
    assert result["hansard_stats"]["total_mentions"] > 0


@respx.mock
@pytest.mark.asyncio
async def test_fetch_handles_hansard_error(
    parliament_config: SourceConfig,
) -> None:
    """Test that hansard errors are captured as zero counts."""
    for bill_id in TRACKED_BILLS:
        respx.get(
            "https://www.parl.ca/legisinfo/en/api/bills",
            params={"billNumber": bill_id, "language": "en"},
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "Items": [
                        {
                            "ShortTitle": "Test Bill",
                            "StatusName": "Active",
                            "LatestCompletedMajorStageEn": "In Progress",
                            "ParliamentSession": "44-1",
                            "SponsorName": "Sponsor",
                        }
                    ]
                },
            )
        )

    # All hansard requests fail
    for keyword in parliament_config.get("keywords", []):
        respx.get(
            "https://api.ourcommons.ca/api/v1/Debates",
            params={"keyword": keyword, "language": "en"},
        ).mock(return_value=httpx.Response(503))

    result = await fetch(parliament_config, "2025-01-17")

    assert result["hansard_stats"]["total_mentions"] == 0
    for keyword, count in result["hansard_stats"]["by_keyword"].items():
        assert count == 0


@respx.mock
@pytest.mark.asyncio
async def test_bill_data_structure(
    parliament_config: SourceConfig,
) -> None:
    """Test that each bill record has the expected fields."""
    bill_data = {
        "Items": [
            {
                "ShortTitle": "Test Act",
                "StatusName": "Active",
                "LatestCompletedMajorStageEn": "Royal Assent",
                "ParliamentSession": "44-1",
                "SponsorName": "Hon. Test Minister",
            }
        ]
    }

    for bill_id in TRACKED_BILLS:
        respx.get(
            "https://www.parl.ca/legisinfo/en/api/bills",
            params={"billNumber": bill_id, "language": "en"},
        ).mock(return_value=httpx.Response(200, json=bill_data))

    for keyword in parliament_config.get("keywords", []):
        respx.get(
            "https://api.ourcommons.ca/api/v1/Debates",
            params={"keyword": keyword, "language": "en"},
        ).mock(return_value=httpx.Response(200, json={"TotalCount": 0, "Items": []}))

    result = await fetch(parliament_config, "2025-01-17")

    for bill in result["bills"]:
        assert "id" in bill
        assert "title" in bill
        assert "status" in bill
        assert "last_action" in bill
        assert bill["id"] in TRACKED_BILLS
