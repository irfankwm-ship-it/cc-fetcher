"""Tests for the Yahoo Finance source fetcher."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from fetcher.config import RetryConfig, SourceConfig
from fetcher.sources.yahoo_finance import (
    _fetch_index_data,
    _fetch_sector_data,
    _fetch_stock_data,
    fetch,
)


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
def yf_full_config() -> SourceConfig:
    """Config with indices, sectors, and watchlist."""
    return SourceConfig(
        name="yahoo_finance",
        settings={
            "indices": [
                {"ticker": "000001.SS", "name": "Shanghai Composite"},
            ],
            "sectors": [
                {"ticker": "^HSTECH", "name": "Hang Seng TECH"},
                {"ticker": "000016.SS", "name": "SSE 50"},
            ],
            "watchlist": [
                {"ticker": "9988.HK", "name": "Alibaba Group"},
                {"ticker": "0700.HK", "name": "Tencent Holdings"},
                {"ticker": "9999.HK", "name": "NetEase"},
                {"ticker": "3690.HK", "name": "Meituan"},
                {"ticker": "9618.HK", "name": "JD.com"},
                {"ticker": "9888.HK", "name": "Baidu"},
                {"ticker": "2318.HK", "name": "Ping An Insurance"},
                {"ticker": "1398.HK", "name": "ICBC"},
                {"ticker": "0939.HK", "name": "China Construction Bank"},
                {"ticker": "3988.HK", "name": "Bank of China"},
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


def _make_stock_df(close_values: list[float]) -> pd.DataFrame:
    """Helper to create a stock DataFrame with given close values."""
    n = len(close_values)
    dates = pd.date_range("2025-01-13", periods=n, freq="B")
    return pd.DataFrame(
        {
            "Open": close_values,
            "High": [c + 5.0 for c in close_values],
            "Low": [c - 5.0 for c in close_values],
            "Close": close_values,
            "Volume": [1000000] * n,
        },
        index=dates,
    )


# =============================================================================
# Index tests (existing)
# =============================================================================


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


# =============================================================================
# Sector tests
# =============================================================================


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
def test_fetch_sector_data_success(
    mock_ticker_cls: MagicMock, mock_history_df: pd.DataFrame,
) -> None:
    """Test successful sector data fetch returns expected fields."""
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = mock_history_df
    mock_ticker_cls.return_value = mock_ticker

    result = _fetch_sector_data("^HSTECH", "Hang Seng TECH", "2025-01-17")

    assert result["ticker"] == "^HSTECH"
    assert result["name"] == "Hang Seng TECH"
    assert result["index_name"] == "Hang Seng TECH"
    assert result["value"] is not None
    assert result["change_pct"] is not None
    assert result["direction"] in ("up", "down", "unchanged")
    # Sector data should not have sparkline
    assert "sparkline" not in result


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
def test_fetch_sector_data_direction_up(mock_ticker_cls: MagicMock) -> None:
    """Test that sector direction is 'up' when price increases."""
    # Close goes from 100 to 110 -> +10%
    df = _make_stock_df([100.0, 110.0])
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = df
    mock_ticker_cls.return_value = mock_ticker

    result = _fetch_sector_data("^HSTECH", "Hang Seng TECH", "2025-01-14")

    assert result["direction"] == "up"
    assert result["change_pct"] == 10.0


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
def test_fetch_sector_data_direction_down(mock_ticker_cls: MagicMock) -> None:
    """Test that sector direction is 'down' when price decreases."""
    # Close goes from 200 to 190 -> -5%
    df = _make_stock_df([200.0, 190.0])
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = df
    mock_ticker_cls.return_value = mock_ticker

    result = _fetch_sector_data("000016.SS", "SSE 50", "2025-01-14")

    assert result["direction"] == "down"
    assert result["change_pct"] == -5.0


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
def test_fetch_sector_data_direction_unchanged(mock_ticker_cls: MagicMock) -> None:
    """Test that sector direction is 'unchanged' when price is flat."""
    df = _make_stock_df([100.0, 100.0])
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = df
    mock_ticker_cls.return_value = mock_ticker

    result = _fetch_sector_data("000016.SS", "SSE 50", "2025-01-14")

    assert result["direction"] == "unchanged"
    assert result["change_pct"] == 0.0


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
def test_fetch_sector_data_empty_history(mock_ticker_cls: MagicMock) -> None:
    """Test sector fetch with no data returns None values."""
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = pd.DataFrame()
    mock_ticker_cls.return_value = mock_ticker

    result = _fetch_sector_data("^HSTECH", "Hang Seng TECH", "2025-01-17")

    assert result["value"] is None
    assert result["change_pct"] is None
    assert result["direction"] == "unchanged"


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
def test_fetch_sector_data_exception(mock_ticker_cls: MagicMock) -> None:
    """Test sector fetch handles exceptions gracefully."""
    mock_ticker_cls.side_effect = Exception("API timeout")

    result = _fetch_sector_data("^HSTECH", "Hang Seng TECH", "2025-01-17")

    assert result["value"] is None
    assert result["change_pct"] is None
    assert result["direction"] == "unchanged"
    assert "error" in result


# =============================================================================
# Stock / Watchlist tests
# =============================================================================


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
def test_fetch_stock_data_success(
    mock_ticker_cls: MagicMock, mock_history_df: pd.DataFrame,
) -> None:
    """Test successful stock data fetch returns expected fields."""
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = mock_history_df
    mock_ticker_cls.return_value = mock_ticker

    result = _fetch_stock_data("9988.HK", "Alibaba Group", "2025-01-17")

    assert result["ticker"] == "9988.HK"
    assert result["name"] == "Alibaba Group"
    assert result["close"] is not None
    assert result["change_pct"] is not None
    assert result["prev_close"] is not None
    # Stock data should not have sparkline or direction
    assert "sparkline" not in result
    assert "direction" not in result


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
def test_fetch_stock_data_empty_history(mock_ticker_cls: MagicMock) -> None:
    """Test stock fetch with no data returns None values."""
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = pd.DataFrame()
    mock_ticker_cls.return_value = mock_ticker

    result = _fetch_stock_data("9988.HK", "Alibaba Group", "2025-01-17")

    assert result["close"] is None
    assert result["change_pct"] is None
    assert result["prev_close"] is None


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
def test_fetch_stock_data_exception(mock_ticker_cls: MagicMock) -> None:
    """Test stock fetch handles exceptions gracefully."""
    mock_ticker_cls.side_effect = Exception("Connection refused")

    result = _fetch_stock_data("9988.HK", "Alibaba Group", "2025-01-17")

    assert result["close"] is None
    assert result["change_pct"] is None
    assert result["prev_close"] is None
    assert "error" in result


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
def test_fetch_stock_data_change_pct_calculation(mock_ticker_cls: MagicMock) -> None:
    """Test that stock change_pct is correctly calculated."""
    # Close: 100 -> 105 => +5%
    df = _make_stock_df([100.0, 105.0])
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = df
    mock_ticker_cls.return_value = mock_ticker

    result = _fetch_stock_data("0700.HK", "Tencent Holdings", "2025-01-14")

    assert result["change_pct"] == 5.0
    assert result["close"] == 105.0
    assert result["prev_close"] == 100.0


# =============================================================================
# Integration: fetch() with sectors and movers
# =============================================================================


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
@pytest.mark.asyncio
async def test_fetch_returns_sectors(
    mock_ticker_cls: MagicMock,
    yf_full_config: SourceConfig,
    mock_history_df: pd.DataFrame,
) -> None:
    """Test that fetch() returns sector data in the result."""
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = mock_history_df
    mock_ticker_cls.return_value = mock_ticker

    result = await fetch(yf_full_config, "2025-01-17")

    assert "sectors" in result
    assert len(result["sectors"]) == 2
    assert result["sectors"][0]["ticker"] == "^HSTECH"
    assert result["sectors"][1]["ticker"] == "000016.SS"
    assert result["summary"]["sectors_fetched"] == 2
    assert result["summary"]["sectors_failed"] == 0


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
@pytest.mark.asyncio
async def test_fetch_returns_movers(
    mock_ticker_cls: MagicMock,
    yf_full_config: SourceConfig,
) -> None:
    """Test that fetch() returns gainers and losers sorted by change_pct."""
    # Create varied stock data so different tickers get different change_pcts.
    # The watchlist in yf_full_config has 10 stocks.
    # We will make each stock have a different change_pct.
    change_pcts = [5.0, -3.0, 8.0, -1.5, 2.0, -7.0, 1.0, 0.5, -2.0, 3.5]
    call_idx = 0

    def ticker_side_effect(symbol: str) -> MagicMock:
        nonlocal call_idx
        mock = MagicMock()

        # First call is for the 1 index, next 2 are sectors, then 10 watchlist stocks
        if call_idx < 1:
            # Index
            mock.history.return_value = _make_stock_df([3200.0, 3210.0, 3220.0, 3230.0, 3240.0])
        elif call_idx < 3:
            # Sectors
            mock.history.return_value = _make_stock_df([1000.0, 1010.0])
        else:
            # Stocks: use change_pcts to compute prev/current close
            stock_idx = call_idx - 3
            if stock_idx < len(change_pcts):
                pct = change_pcts[stock_idx]
                prev = 100.0
                curr = prev * (1 + pct / 100)
                mock.history.return_value = _make_stock_df([prev, curr])
            else:
                mock.history.return_value = _make_stock_df([100.0, 100.0])

        call_idx += 1
        return mock

    mock_ticker_cls.side_effect = ticker_side_effect

    result = await fetch(yf_full_config, "2025-01-17")

    assert "movers" in result
    assert "gainers" in result["movers"]
    assert "losers" in result["movers"]

    gainers = result["movers"]["gainers"]
    losers = result["movers"]["losers"]

    assert len(gainers) == 5
    assert len(losers) == 5

    # Gainers should be sorted descending by change_pct
    for i in range(len(gainers) - 1):
        assert gainers[i]["change_pct"] >= gainers[i + 1]["change_pct"]

    # The top gainer should have the highest change_pct (8.0)
    assert gainers[0]["change_pct"] == 8.0

    # The bottom loser should have the lowest change_pct (-7.0)
    assert losers[-1]["change_pct"] == -7.0


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
@pytest.mark.asyncio
async def test_fetch_movers_with_failures(
    mock_ticker_cls: MagicMock,
    yf_full_config: SourceConfig,
) -> None:
    """Test movers when some stocks fail to fetch."""
    call_idx = 0

    def ticker_side_effect(symbol: str) -> MagicMock:
        nonlocal call_idx
        mock = MagicMock()

        if call_idx < 3:
            # Index + sectors: return valid data
            mock.history.return_value = _make_stock_df([100.0, 105.0])
        elif call_idx % 2 == 0:
            # Every other stock returns empty (simulating failure)
            mock.history.return_value = pd.DataFrame()
        else:
            mock.history.return_value = _make_stock_df([100.0, 102.0])

        call_idx += 1
        return mock

    mock_ticker_cls.side_effect = ticker_side_effect

    result = await fetch(yf_full_config, "2025-01-17")

    assert "movers" in result
    # Some stocks failed, so we should still get results for those that succeeded
    total_watchlist = len(yf_full_config.get("watchlist", []))
    fetched = result["summary"]["watchlist_fetched"]
    failed = result["summary"]["watchlist_failed"]
    assert fetched + failed == total_watchlist
    assert failed > 0  # At least some should have failed


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
@pytest.mark.asyncio
async def test_fetch_uses_defaults_when_no_config(
    mock_ticker_cls: MagicMock,
    mock_history_df: pd.DataFrame,
) -> None:
    """Test that fetch uses DEFAULT_SECTORS and DEFAULT_WATCHLIST when not configured."""
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = mock_history_df
    mock_ticker_cls.return_value = mock_ticker

    # Config with only indices, no sectors or watchlist specified
    minimal_config = SourceConfig(
        name="yahoo_finance",
        settings={
            "indices": [
                {"ticker": "000001.SS", "name": "Shanghai Composite"},
            ],
        },
        timeout=10,
        retry=RetryConfig(),
    )

    result = await fetch(minimal_config, "2025-01-17")

    # Should use defaults: 6 sectors and 20 watchlist stocks
    assert len(result["sectors"]) == 6
    assert len(result["movers"]["gainers"]) == 5
    assert len(result["movers"]["losers"]) == 5


@patch("fetcher.sources.yahoo_finance.yf.Ticker")
@pytest.mark.asyncio
async def test_fetch_summary_includes_all_counts(
    mock_ticker_cls: MagicMock,
    yf_full_config: SourceConfig,
    mock_history_df: pd.DataFrame,
) -> None:
    """Test that the summary dict includes counts for indices, sectors, and watchlist."""
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = mock_history_df
    mock_ticker_cls.return_value = mock_ticker

    result = await fetch(yf_full_config, "2025-01-17")

    summary = result["summary"]
    assert "indices_fetched" in summary
    assert "indices_failed" in summary
    assert "sectors_fetched" in summary
    assert "sectors_failed" in summary
    assert "watchlist_fetched" in summary
    assert "watchlist_failed" in summary
    assert "all_markets_closed" in summary
