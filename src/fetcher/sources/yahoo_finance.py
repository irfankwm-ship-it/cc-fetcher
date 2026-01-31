"""Fetcher for Chinese market indices via yfinance.

Tickers:
  000001.SS  - Shanghai Composite
  399001.SZ  - Shenzhen Component
  ^HSI       - Hang Seng Index
  000300.SS  - CSI 300

Also fetches 5-day close prices for sparkline data.
Handles market holidays gracefully (returns last available data).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import yfinance as yf

from fetcher.config import SourceConfig

logger = logging.getLogger(__name__)

DEFAULT_INDICES = [
    {"ticker": "000001.SS", "name": "Shanghai Composite"},
    {"ticker": "399001.SZ", "name": "Shenzhen Component"},
    {"ticker": "^HSI", "name": "Hang Seng"},
    {"ticker": "000300.SS", "name": "CSI 300"},
]

SPARKLINE_DAYS = 5


def _fetch_index_data(ticker_symbol: str, name: str, target_date: str) -> dict[str, Any]:
    """Fetch data for a single market index.

    Args:
        ticker_symbol: yfinance ticker symbol.
        name: Human-readable index name.
        target_date: Target date as YYYY-MM-DD.

    Returns:
        Dictionary with index data including value, change, and sparkline.
    """
    try:
        ticker = yf.Ticker(ticker_symbol)

        # Fetch recent history for sparkline and current value
        end_date = datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = end_date - timedelta(days=SPARKLINE_DAYS + 5)  # extra buffer for holidays

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        hist = ticker.history(start=start_str, end=end_str)

        if hist.empty:
            logger.warning(
                "No data available for %s (%s) near %s",
                name, ticker_symbol, target_date,
            )
            return {
                "ticker": ticker_symbol,
                "name": name,
                "value": None,
                "change_pct": None,
                "sparkline": [],
                "market_holiday": True,
            }

        # Get the most recent available close prices
        recent = hist.tail(SPARKLINE_DAYS + 1)
        closes = recent["Close"].tolist()
        sparkline = [round(float(c), 2) for c in closes[-SPARKLINE_DAYS:]]

        current_close = float(closes[-1])
        prev_close = float(closes[-2]) if len(closes) >= 2 else current_close
        if prev_close:
            change_pct = round(((current_close - prev_close) / prev_close) * 100, 2)
        else:
            change_pct = 0.0

        latest_date = recent.index[-1].strftime("%Y-%m-%d")

        return {
            "ticker": ticker_symbol,
            "name": name,
            "value": round(current_close, 2),
            "change_pct": change_pct,
            "prev_close": round(prev_close, 2),
            "sparkline": sparkline,
            "latest_date": latest_date,
            "market_holiday": latest_date != target_date,
        }

    except Exception as exc:
        logger.error("Error fetching %s (%s): %s", name, ticker_symbol, exc)
        return {
            "ticker": ticker_symbol,
            "name": name,
            "value": None,
            "change_pct": None,
            "sparkline": [],
            "error": str(exc),
        }


async def fetch(config: SourceConfig, date: str) -> dict[str, Any]:
    """Fetch Chinese market index data.

    Args:
        config: Source configuration with index definitions.
        date: Target date (YYYY-MM-DD).

    Returns:
        Dictionary with index data for all configured tickers.
    """
    indices_config = config.get("indices", DEFAULT_INDICES)

    # yfinance is synchronous, so we call it directly
    indices: list[dict[str, Any]] = []
    for idx_cfg in indices_config:
        ticker_symbol = idx_cfg.get("ticker", "")
        name = idx_cfg.get("name", ticker_symbol)

        if not ticker_symbol:
            continue

        result = _fetch_index_data(ticker_symbol, name, date)
        indices.append(result)

    # Compute summary
    available = [i for i in indices if i.get("value") is not None]
    all_holiday = all(i.get("market_holiday", False) for i in available) if available else True

    return {
        "date": date,
        "indices": indices,
        "summary": {
            "indices_fetched": len(available),
            "indices_failed": len(indices) - len(available),
            "all_markets_closed": all_holiday,
        },
    }
