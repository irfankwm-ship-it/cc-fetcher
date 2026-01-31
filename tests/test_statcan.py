"""Tests for the Statistics Canada source fetcher."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import respx

from fetcher.config import RetryConfig, SourceConfig
from fetcher.sources.statcan import _compute_yoy_change, _parse_trade_data, fetch


@pytest.fixture
def statcan_config() -> SourceConfig:
    return SourceConfig(
        name="statcan",
        settings={
            "base_url": "https://www150.statcan.gc.ca/t1/tbl1/en/dtl",
            "table_id": "12-10-0011-01",
        },
        timeout=10,
        retry=RetryConfig(),
    )


@pytest.fixture
def trade_api_response() -> dict[str, Any]:
    return {
        "status": "ok",
        "object": [
            {
                "hs_code": "1205",
                "commodity_name": "Canola / rapeseed",
                "exports": 5234000000,
                "imports": 12000000,
                "prev_exports": 4800000000,
                "prev_imports": 11000000,
            },
            {
                "hs_code": "4407",
                "commodity_name": "Lumber (wood sawn)",
                "exports": 1890000000,
                "imports": 45000000,
                "prev_exports": 2100000000,
                "prev_imports": 40000000,
            },
            {
                "hs_code": "8471",
                "commodity_name": "Machinery / computers",
                "exports": 45000000,
                "imports": 12300000000,
                "prev_exports": 42000000,
                "prev_imports": 11800000000,
            },
        ],
    }


def test_compute_yoy_change_positive() -> None:
    """Test YoY change with positive growth."""
    result = _compute_yoy_change(110.0, 100.0)
    assert result == 10.0


def test_compute_yoy_change_negative() -> None:
    """Test YoY change with negative growth."""
    result = _compute_yoy_change(90.0, 100.0)
    assert result == -10.0


def test_compute_yoy_change_zero_previous() -> None:
    """Test YoY change when previous value is zero."""
    result = _compute_yoy_change(100.0, 0.0)
    assert result is None


def test_parse_trade_data(trade_api_response: dict[str, Any]) -> None:
    """Test parsing of StatCan API response."""
    commodities = _parse_trade_data(trade_api_response)

    assert len(commodities) == 3
    canola = commodities[0]
    assert canola["hs_code"] == "1205"
    assert canola["exports_cad"] == 5234000000
    assert canola["imports_cad"] == 12000000
    assert canola["trade_balance_cad"] == 5234000000 - 12000000
    assert canola["exports_yoy_pct"] is not None
    assert canola["imports_yoy_pct"] is not None


def test_parse_trade_data_computes_trade_balance(trade_api_response: dict[str, Any]) -> None:
    """Test that trade balance is correctly computed."""
    commodities = _parse_trade_data(trade_api_response)

    machinery = commodities[2]
    assert machinery["hs_code"] == "8471"
    # Imports > exports so balance should be negative
    assert machinery["trade_balance_cad"] < 0
    expected_balance = 45000000 - 12300000000
    assert machinery["trade_balance_cad"] == expected_balance


def test_parse_trade_data_empty() -> None:
    """Test parsing an empty response."""
    commodities = _parse_trade_data({"data": []})
    assert commodities == []


@respx.mock
@pytest.mark.asyncio
async def test_fetch_success(
    statcan_config: SourceConfig,
    trade_api_response: dict[str, Any],
) -> None:
    """Test successful data fetch from StatCan."""
    respx.get("https://www150.statcan.gc.ca/t1/tbl1/en/dtl").mock(
        return_value=httpx.Response(200, json=trade_api_response)
    )

    result = await fetch(statcan_config, "2025-01-17")

    assert result["date"] == "2025-01-17"
    assert result["country"] == "China"
    assert len(result["commodities"]) == 3
    assert "totals" in result
    assert "total_exports_cad" in result["totals"]
    assert "total_imports_cad" in result["totals"]
    assert "trade_balance_cad" in result["totals"]


@respx.mock
@pytest.mark.asyncio
async def test_fetch_computes_totals(
    statcan_config: SourceConfig,
    trade_api_response: dict[str, Any],
) -> None:
    """Test that aggregate totals are correctly computed."""
    respx.get("https://www150.statcan.gc.ca/t1/tbl1/en/dtl").mock(
        return_value=httpx.Response(200, json=trade_api_response)
    )

    result = await fetch(statcan_config, "2025-01-17")

    expected_exports = 5234000000 + 1890000000 + 45000000
    expected_imports = 12000000 + 45000000 + 12300000000
    assert result["totals"]["total_exports_cad"] == expected_exports
    assert result["totals"]["total_imports_cad"] == expected_imports
    assert result["totals"]["trade_balance_cad"] == expected_exports - expected_imports


@respx.mock
@pytest.mark.asyncio
async def test_fetch_http_error(statcan_config: SourceConfig) -> None:
    """Test graceful handling of HTTP errors."""
    respx.get("https://www150.statcan.gc.ca/t1/tbl1/en/dtl").mock(
        return_value=httpx.Response(500)
    )

    result = await fetch(statcan_config, "2025-01-17")

    assert "error" in result
    assert result["commodities"] == []


@respx.mock
@pytest.mark.asyncio
async def test_fetch_timeout(statcan_config: SourceConfig) -> None:
    """Test graceful handling of request timeouts."""
    respx.get("https://www150.statcan.gc.ca/t1/tbl1/en/dtl").mock(
        side_effect=httpx.ConnectTimeout("Connection timed out")
    )

    result = await fetch(statcan_config, "2025-01-17")

    assert "error" in result
    assert result["commodities"] == []
