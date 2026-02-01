"""Tests for the Statistics Canada source fetcher."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import respx

from fetcher.config import RetryConfig, SourceConfig
from fetcher.sources.statcan import (
    COMMODITY_COORDS,
    COMMODITY_TABLE_PID,
    WDS_BASE,
    _determine_trend,
    _extract_latest_and_previous,
    _fetch_commodities,
    fetch,
)


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


def _make_commodity_response(
    num_commodities: int = 6,
    *,
    all_success: bool = True,
) -> list[dict[str, Any]]:
    """Build a mock WDS response for commodity coordinate pairs.

    Each commodity produces two entries (import, export).
    """
    results: list[dict[str, Any]] = []
    for i in range(num_commodities):
        import_val_prev = 100.0 + i * 10
        import_val_latest = 110.0 + i * 10
        export_val_prev = 50.0 + i * 5
        export_val_latest = 55.0 + i * 5

        if all_success:
            results.append({
                "status": "SUCCESS",
                "object": {
                    "vectorDataPoint": [
                        {"refPer": "2025-10-01", "value": import_val_prev, "scalarFactorCode": 6},
                        {"refPer": "2025-11-01", "value": import_val_latest, "scalarFactorCode": 6},
                    ],
                },
            })
            results.append({
                "status": "SUCCESS",
                "object": {
                    "vectorDataPoint": [
                        {"refPer": "2025-10-01", "value": export_val_prev, "scalarFactorCode": 6},
                        {"refPer": "2025-11-01", "value": export_val_latest, "scalarFactorCode": 6},
                    ],
                },
            })
        else:
            # Mix of successes and failures
            results.append({"status": "FAILED"})
            results.append({"status": "FAILED"})
    return results


# ──────────────────────────────────────────────────────────────
# Helper function tests
# ──────────────────────────────────────────────────────────────

class TestDetermineTrend:
    """Tests for the _determine_trend helper."""

    def test_up(self) -> None:
        assert _determine_trend(110.0, 100.0) == "up"

    def test_down(self) -> None:
        assert _determine_trend(90.0, 100.0) == "down"

    def test_stable_within_threshold(self) -> None:
        assert _determine_trend(100.5, 100.0) == "stable"

    def test_stable_when_none(self) -> None:
        assert _determine_trend(None, 100.0) == "stable"
        assert _determine_trend(100.0, None) == "stable"
        assert _determine_trend(None, None) == "stable"

    def test_stable_when_previous_zero(self) -> None:
        assert _determine_trend(10.0, 0) == "stable"


class TestExtractLatestAndPrevious:
    """Tests for the _extract_latest_and_previous helper."""

    def test_empty_list(self) -> None:
        assert _extract_latest_and_previous([]) == (None, None)

    def test_single_point(self) -> None:
        assert _extract_latest_and_previous([{"value": 42.0}]) == (42.0, None)

    def test_two_points(self) -> None:
        pts = [{"value": 10.0}, {"value": 20.0}]
        assert _extract_latest_and_previous(pts) == (20.0, 10.0)

    def test_three_points_takes_last_two(self) -> None:
        pts = [{"value": 1.0}, {"value": 2.0}, {"value": 3.0}]
        assert _extract_latest_and_previous(pts) == (3.0, 2.0)


# ──────────────────────────────────────────────────────────────
# Existing aggregate-trade tests (updated for two POST calls)
# ──────────────────────────────────────────────────────────────

@respx.mock
@pytest.mark.asyncio
async def test_fetch_success(
    statcan_config: SourceConfig,
    wds_response: list[dict[str, Any]],
) -> None:
    """Test successful data fetch from StatCan WDS."""
    commodity_resp = _make_commodity_response()
    # The endpoint is called twice: once for aggregates, once for commodities.
    respx.post(
        f"{WDS_BASE}/getDataFromCubePidCoordAndLatestNPeriods"
    ).mock(side_effect=[
        httpx.Response(200, json=wds_response),
        httpx.Response(200, json=commodity_resp),
    ])

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
    commodity_resp = _make_commodity_response()
    respx.post(
        f"{WDS_BASE}/getDataFromCubePidCoordAndLatestNPeriods"
    ).mock(side_effect=[
        httpx.Response(200, json=wds_response),
        httpx.Response(200, json=commodity_resp),
    ])

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
    commodity_resp = _make_commodity_response()
    respx.post(
        f"{WDS_BASE}/getDataFromCubePidCoordAndLatestNPeriods"
    ).mock(side_effect=[
        httpx.Response(200, json=wds_response),
        httpx.Response(200, json=commodity_resp),
    ])

    result = await fetch(statcan_config, "2025-01-17")

    assert result["totals"]["total_imports_cad"] == 7324.7
    assert result["totals"]["total_exports_cad"] == 3980.3
    assert result["totals"]["trade_balance_cad"] == round(3980.3 - 7324.7, 1)


@respx.mock
@pytest.mark.asyncio
async def test_fetch_http_error(statcan_config: SourceConfig) -> None:
    """Test graceful handling of HTTP errors on the aggregate call."""
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


# ──────────────────────────────────────────────────────────────
# Commodity-level fetch tests
# ──────────────────────────────────────────────────────────────

@respx.mock
@pytest.mark.asyncio
async def test_fetch_commodities_success(
    statcan_config: SourceConfig,
    wds_response: list[dict[str, Any]],
) -> None:
    """Test that commodity data is populated on success."""
    commodity_resp = _make_commodity_response()
    respx.post(
        f"{WDS_BASE}/getDataFromCubePidCoordAndLatestNPeriods"
    ).mock(side_effect=[
        httpx.Response(200, json=wds_response),
        httpx.Response(200, json=commodity_resp),
    ])

    result = await fetch(statcan_config, "2025-01-17")

    assert isinstance(result["commodities"], list)
    assert len(result["commodities"]) == len(COMMODITY_COORDS)

    first = result["commodities"][0]
    assert first["name"] == "Energy Products"
    assert first["name_zh"] == "\u80fd\u6e90\u4ea7\u54c1"
    assert first["import_cad_millions"] is not None
    assert first["export_cad_millions"] is not None
    assert first["balance_cad_millions"] is not None
    assert first["trend"] in ("up", "down", "stable")


@respx.mock
@pytest.mark.asyncio
async def test_fetch_commodities_http_error_returns_empty(
    statcan_config: SourceConfig,
    wds_response: list[dict[str, Any]],
) -> None:
    """Commodity HTTP failure should result in empty commodities list."""
    respx.post(
        f"{WDS_BASE}/getDataFromCubePidCoordAndLatestNPeriods"
    ).mock(side_effect=[
        httpx.Response(200, json=wds_response),
        httpx.Response(503),  # commodity call fails
    ])

    result = await fetch(statcan_config, "2025-01-17")

    # Aggregate data should still be present
    assert result["imports_cad_millions"] == 7324.7
    # Commodities should gracefully return empty
    assert result["commodities"] == []


@respx.mock
@pytest.mark.asyncio
async def test_fetch_commodities_timeout_returns_empty(
    statcan_config: SourceConfig,
    wds_response: list[dict[str, Any]],
) -> None:
    """Commodity timeout should result in empty commodities list."""
    respx.post(
        f"{WDS_BASE}/getDataFromCubePidCoordAndLatestNPeriods"
    ).mock(side_effect=[
        httpx.Response(200, json=wds_response),
        httpx.ConnectTimeout("Connection timed out"),
    ])

    result = await fetch(statcan_config, "2025-01-17")

    assert result["imports_cad_millions"] == 7324.7
    assert result["commodities"] == []


@respx.mock
@pytest.mark.asyncio
async def test_fetch_commodities_partial_failure(
    statcan_config: SourceConfig,
    wds_response: list[dict[str, Any]],
) -> None:
    """Partial commodity failures should return only the successful ones."""
    # Build a response where first commodity succeeds, rest fail
    results: list[dict[str, Any]] = []
    # First commodity: import SUCCESS, export SUCCESS
    results.append({
        "status": "SUCCESS",
        "object": {
            "vectorDataPoint": [
                {"refPer": "2025-10-01", "value": 100.0, "scalarFactorCode": 6},
                {"refPer": "2025-11-01", "value": 120.0, "scalarFactorCode": 6},
            ],
        },
    })
    results.append({
        "status": "SUCCESS",
        "object": {
            "vectorDataPoint": [
                {"refPer": "2025-10-01", "value": 50.0, "scalarFactorCode": 6},
                {"refPer": "2025-11-01", "value": 60.0, "scalarFactorCode": 6},
            ],
        },
    })
    # Remaining commodities: all FAILED
    for _ in range(len(COMMODITY_COORDS) - 1):
        results.append({"status": "FAILED"})
        results.append({"status": "FAILED"})

    respx.post(
        f"{WDS_BASE}/getDataFromCubePidCoordAndLatestNPeriods"
    ).mock(side_effect=[
        httpx.Response(200, json=wds_response),
        httpx.Response(200, json=results),
    ])

    result = await fetch(statcan_config, "2025-01-17")

    # Only the first commodity should appear
    assert len(result["commodities"]) == 1
    comm = result["commodities"][0]
    assert comm["name"] == "Energy Products"
    assert comm["import_cad_millions"] == 120.0
    assert comm["export_cad_millions"] == 60.0
    assert comm["balance_cad_millions"] == round(60.0 - 120.0, 1)


@respx.mock
@pytest.mark.asyncio
async def test_fetch_commodities_trend_calculation(
    statcan_config: SourceConfig,
    wds_response: list[dict[str, Any]],
) -> None:
    """Test that commodity trend is calculated from total trade volume."""
    # One commodity: imports go up, exports go up -> trend "up"
    results: list[dict[str, Any]] = []
    results.append({
        "status": "SUCCESS",
        "object": {
            "vectorDataPoint": [
                {"refPer": "2025-10-01", "value": 100.0, "scalarFactorCode": 6},
                {"refPer": "2025-11-01", "value": 200.0, "scalarFactorCode": 6},
            ],
        },
    })
    results.append({
        "status": "SUCCESS",
        "object": {
            "vectorDataPoint": [
                {"refPer": "2025-10-01", "value": 100.0, "scalarFactorCode": 6},
                {"refPer": "2025-11-01", "value": 200.0, "scalarFactorCode": 6},
            ],
        },
    })
    # Fill remaining commodities as failed
    for _ in range(len(COMMODITY_COORDS) - 1):
        results.append({"status": "FAILED"})
        results.append({"status": "FAILED"})

    respx.post(
        f"{WDS_BASE}/getDataFromCubePidCoordAndLatestNPeriods"
    ).mock(side_effect=[
        httpx.Response(200, json=wds_response),
        httpx.Response(200, json=results),
    ])

    result = await fetch(statcan_config, "2025-01-17")

    assert len(result["commodities"]) == 1
    assert result["commodities"][0]["trend"] == "up"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_commodities_all_failed_returns_empty(
    statcan_config: SourceConfig,
    wds_response: list[dict[str, Any]],
) -> None:
    """If all commodity coordinates fail, commodities should be empty."""
    commodity_resp = _make_commodity_response(all_success=False)
    respx.post(
        f"{WDS_BASE}/getDataFromCubePidCoordAndLatestNPeriods"
    ).mock(side_effect=[
        httpx.Response(200, json=wds_response),
        httpx.Response(200, json=commodity_resp),
    ])

    result = await fetch(statcan_config, "2025-01-17")

    assert result["commodities"] == []
    # Aggregate data should still be present
    assert result["imports_cad_millions"] == 7324.7


@respx.mock
@pytest.mark.asyncio
async def test_commodity_coords_have_required_fields() -> None:
    """Verify COMMODITY_COORDS structure is well-formed."""
    for comm in COMMODITY_COORDS:
        assert "label" in comm, f"Missing 'label' in {comm}"
        assert "label_zh" in comm, f"Missing 'label_zh' in {comm}"
        assert "import_coordinate" in comm, f"Missing 'import_coordinate' in {comm}"
        assert "export_coordinate" in comm, f"Missing 'export_coordinate' in {comm}"
        # Import and export coordinates should differ only in trade dimension
        assert comm["import_coordinate"].startswith("1.1.")
        assert comm["export_coordinate"].startswith("1.2.")
