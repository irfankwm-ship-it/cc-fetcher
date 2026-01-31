"""Tests for the Statistics Canada source fetcher."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import respx

from fetcher.config import RetryConfig, SourceConfig
from fetcher.sources.statcan import WDS_BASE, fetch


@pytest.fixture
def statcan_config() -> SourceConfig:
    return SourceConfig(
        name="statcan",
        settings={
            "base_url": WDS_BASE,
        },
        timeout=10,
        retry=RetryConfig(),
    )


@pytest.fixture
def wds_response() -> list[dict[str, Any]]:
    """Mock WDS API response for imports and exports."""
    return [
        {
            "status": "SUCCESS",
            "object": {
                "vectorDataPoint": [
                    {
                        "refPer": "2025-09-01",
                        "value": 7290.5,
                        "scalarFactorCode": 6,
                    },
                    {
                        "refPer": "2025-10-01",
                        "value": 8070.3,
                        "scalarFactorCode": 6,
                    },
                    {
                        "refPer": "2025-11-01",
                        "value": 7324.7,
                        "scalarFactorCode": 6,
                    },
                ],
            },
        },
        {
            "status": "SUCCESS",
            "object": {
                "vectorDataPoint": [
                    {
                        "refPer": "2025-09-01",
                        "value": 2550.2,
                        "scalarFactorCode": 6,
                    },
                    {
                        "refPer": "2025-10-01",
                        "value": 3737.1,
                        "scalarFactorCode": 6,
                    },
                    {
                        "refPer": "2025-11-01",
                        "value": 3980.3,
                        "scalarFactorCode": 6,
                    },
                ],
            },
        },
    ]


@respx.mock
@pytest.mark.asyncio
async def test_fetch_success(
    statcan_config: SourceConfig,
    wds_response: list[dict[str, Any]],
) -> None:
    """Test successful data fetch from StatCan WDS."""
    respx.post(
        f"{WDS_BASE}/getDataFromCubePidCoordAndLatestNPeriods"
    ).mock(return_value=httpx.Response(200, json=wds_response))

    result = await fetch(statcan_config, "2025-01-17")

    assert result["date"] == "2025-01-17"
    assert result["country"] == "China"
    assert result["imports_cad_millions"] == 7324.7
    assert result["exports_cad_millions"] == 3980.3
    assert result["balance_cad_millions"] == round(3980.3 - 7324.7, 1)
    assert result["reference_period"] == "2025-11-01"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_includes_series(
    statcan_config: SourceConfig,
    wds_response: list[dict[str, Any]],
) -> None:
    """Test that time series data is returned."""
    respx.post(
        f"{WDS_BASE}/getDataFromCubePidCoordAndLatestNPeriods"
    ).mock(return_value=httpx.Response(200, json=wds_response))

    result = await fetch(statcan_config, "2025-01-17")

    assert "Imports from China" in result["series"]
    assert "Exports to China" in result["series"]
    assert len(result["series"]["Imports from China"]) == 3
    assert len(result["series"]["Exports to China"]) == 3


@respx.mock
@pytest.mark.asyncio
async def test_fetch_computes_totals(
    statcan_config: SourceConfig,
    wds_response: list[dict[str, Any]],
) -> None:
    """Test that aggregate totals use latest period values."""
    respx.post(
        f"{WDS_BASE}/getDataFromCubePidCoordAndLatestNPeriods"
    ).mock(return_value=httpx.Response(200, json=wds_response))

    result = await fetch(statcan_config, "2025-01-17")

    assert result["totals"]["total_imports_cad"] == 7324.7
    assert result["totals"]["total_exports_cad"] == 3980.3
    assert result["totals"]["trade_balance_cad"] == round(3980.3 - 7324.7, 1)


@respx.mock
@pytest.mark.asyncio
async def test_fetch_http_error(statcan_config: SourceConfig) -> None:
    """Test graceful handling of HTTP errors."""
    respx.post(
        f"{WDS_BASE}/getDataFromCubePidCoordAndLatestNPeriods"
    ).mock(return_value=httpx.Response(500))

    result = await fetch(statcan_config, "2025-01-17")

    assert "error" in result
    assert result["commodities"] == []


@respx.mock
@pytest.mark.asyncio
async def test_fetch_timeout(statcan_config: SourceConfig) -> None:
    """Test graceful handling of request timeouts."""
    respx.post(
        f"{WDS_BASE}/getDataFromCubePidCoordAndLatestNPeriods"
    ).mock(side_effect=httpx.ConnectTimeout("Connection timed out"))

    result = await fetch(statcan_config, "2025-01-17")

    assert "error" in result
    assert result["commodities"] == []
