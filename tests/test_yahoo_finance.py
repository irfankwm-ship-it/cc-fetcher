"""Tests for the Yahoo Finance source fetcher."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from fetcher.config import RetryConfig, SourceConfig
from fetcher.sources.yahoo_finance import _fetch_index_data, fetch


@pytest.fixture
def yf_config() -> SourceConfig:
    return SourceConfig(
        name="yahoo_finance",
        settings={
            "indices": [
                {"ticker": "000001.SS", "name": "Shanghai Composite"},
                {"ticker": "^HSI", "name": "Hang Seng"},
            ],
        },
        timeout=10,
        retry=RetryConfig(),
    )


@pytest.fixture
def mock_history_df() -> pd.DataFrame:
    """Create a realistic yfinance history DataFrame."""
    dates = pd.date_range("2025-01-13", periods=5, freq="B")
    return pd.DataFrame(
        {
            "Open": [3200.0, 3215.0, 3220.0, 3210.0, 3240.0],
            "High": [3230.0, 3240.0, 3250.0, 3245.0, 3260.0],
            "Low": [3190.0, 3200.0, 3210.0, 3205.0, 3230.0],
            "Close": [3225.30, 3218.75, 3240.10, 3235.60, 3248.90],
            "Volume": [1000000, 1100000, 1050000, 980000, 1200000],
        },
        index=dates,
    )


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
def test_fetch_index_data_success(
    mock_ticker_cls: MagicMock, mock_history_df: pd.DataFrame,
) -> None:
    """Test successful index data fetch."""
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = mock_history_df
    mock_ticker_cls.return_value = mock_ticker

    result = _fetch_index_data("000001.SS", "Shanghai Composite", "2025-01-17")

    assert result["ticker"] == "000001.SS"
    assert result["name"] == "Shanghai Composite"
    assert result["value"] is not None
    assert result["change_pct"] is not None
    assert isinstance(result["sparkline"], list)
    assert len(result["sparkline"]) > 0


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
def test_fetch_index_data_empty_history(mock_ticker_cls: MagicMock) -> None:
    """Test handling of empty history (market holiday / no data)."""
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = pd.DataFrame()
    mock_ticker_cls.return_value = mock_ticker

    result = _fetch_index_data("000001.SS", "Shanghai Composite", "2025-01-17")

    assert result["value"] is None
    assert result["change_pct"] is None
    assert result["sparkline"] == []
    assert result["market_holiday"] is True


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
def test_fetch_index_data_exception(mock_ticker_cls: MagicMock) -> None:
    """Test handling of yfinance exceptions."""
    mock_ticker_cls.side_effect = Exception("Network error")

    result = _fetch_index_data("000001.SS", "Shanghai Composite", "2025-01-17")

    assert result["value"] is None
    assert "error" in result


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
@pytest.mark.asyncio
async def test_fetch_multiple_indices(
    mock_ticker_cls: MagicMock,
    yf_config: SourceConfig,
    mock_history_df: pd.DataFrame,
) -> None:
    """Test fetching multiple indices."""
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = mock_history_df
    mock_ticker_cls.return_value = mock_ticker

    result = await fetch(yf_config, "2025-01-17")

    assert result["date"] == "2025-01-17"
    assert len(result["indices"]) == 2
    assert result["summary"]["indices_fetched"] == 2
    assert result["summary"]["indices_failed"] == 0


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
@pytest.mark.asyncio
async def test_fetch_partial_failure(
    mock_ticker_cls: MagicMock,
    yf_config: SourceConfig,
    mock_history_df: pd.DataFrame,
) -> None:
    """Test that partial failures are handled (one index fails, other succeeds)."""
    call_count = 0

    def side_effect(*args: Any, **kwargs: Any) -> MagicMock:
        nonlocal call_count
        call_count += 1
        mock = MagicMock()
        if call_count == 1:
            mock.history.return_value = mock_history_df
        else:
            mock.history.return_value = pd.DataFrame()
        return mock

    mock_ticker_cls.side_effect = side_effect

    result = await fetch(yf_config, "2025-01-17")

    assert len(result["indices"]) == 2
    assert result["summary"]["indices_fetched"] == 1
    assert result["summary"]["indices_failed"] == 1


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
def test_sparkline_has_correct_length(
    mock_ticker_cls: MagicMock,
    mock_history_df: pd.DataFrame,
) -> None:
    """Test that sparkline data has the expected number of points."""
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = mock_history_df
    mock_ticker_cls.return_value = mock_ticker

    result = _fetch_index_data("000001.SS", "Shanghai Composite", "2025-01-17")

    assert len(result["sparkline"]) <= 5
    assert all(isinstance(v, float) for v in result["sparkline"])
