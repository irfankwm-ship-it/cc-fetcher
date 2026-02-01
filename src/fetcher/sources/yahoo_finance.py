"""Fetcher for Chinese market indices, sector indices, and top movers via yfinance.

Tickers:
  000001.SS  - Shanghai Composite
  399001.SZ  - Shenzhen Component
  ^HSI       - Hang Seng Index
  000300.SS  - CSI 300

Also fetches:
  - Sector indices (e.g. Hang Seng TECH, ChiNext, SSE 50)
  - Top movers from a watchlist of major China-related stocks
  - 5-day close prices for sparkline data (indices only)

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

DEFAULT_SECTORS = [
    {"ticker": "^HSTECH", "name": "Hang Seng TECH"},
    {"ticker": "399006.SZ", "name": "ChiNext Composite"},
    {"ticker": "000016.SS", "name": "SSE 50"},
    {"ticker": "399330.SZ", "name": "Shenzhen 100"},
    {"ticker": "2840.HK", "name": "SPDR Gold Trust"},
    {"ticker": "000905.SS", "name": "CSI 500"},
]

DEFAULT_WATCHLIST = [
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
    {"ticker": "0857.HK", "name": "PetroChina"},
    {"ticker": "0883.HK", "name": "CNOOC"},
    {"ticker": "0386.HK", "name": "China Petroleum"},
    {"ticker": "2020.HK", "name": "ANTA Sports"},
    {"ticker": "1211.HK", "name": "BYD Company"},
    {"ticker": "9866.HK", "name": "NIO Inc"},
    {"ticker": "9868.HK", "name": "XPeng Inc"},
    {"ticker": "2269.HK", "name": "WuXi Biologics"},
    {"ticker": "0981.HK", "name": "SMIC"},
    {"ticker": "6862.HK", "name": "Haidilao"},
]

SPARKLINE_DAYS = 5
MOVERS_COUNT = 5


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


def _fetch_sector_data(ticker_symbol: str, name: str, target_date: str) -> dict[str, Any]:
    """Fetch data for a single sector index.

    Args:
        ticker_symbol: yfinance ticker symbol.
        name: Human-readable sector name.
        target_date: Target date as YYYY-MM-DD.

    Returns:
        Dictionary with sector data including value, change_pct, and direction.
    """
    try:
        ticker = yf.Ticker(ticker_symbol)

        end_date = datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = end_date - timedelta(days=10)  # buffer for holidays

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        hist = ticker.history(start=start_str, end=end_str)

        if hist.empty:
            logger.warning(
                "No sector data available for %s (%s) near %s",
                name, ticker_symbol, target_date,
            )
            return {
                "ticker": ticker_symbol,
                "name": name,
                "index_name": name,
                "value": None,
                "change_pct": None,
                "direction": "unchanged",
            }

        closes = hist["Close"].tolist()
        current_close = float(closes[-1])
        prev_close = float(closes[-2]) if len(closes) >= 2 else current_close

        if prev_close:
            change_pct = round(((current_close - prev_close) / prev_close) * 100, 2)
        else:
            change_pct = 0.0

        if change_pct > 0:
            direction = "up"
        elif change_pct < 0:
            direction = "down"
        else:
            direction = "unchanged"

        return {
            "ticker": ticker_symbol,
            "name": name,
            "index_name": name,
            "value": round(current_close, 2),
            "change_pct": change_pct,
            "direction": direction,
        }

    except Exception as exc:
        logger.error("Error fetching sector %s (%s): %s", name, ticker_symbol, exc)
        return {
            "ticker": ticker_symbol,
            "name": name,
            "index_name": name,
            "value": None,
            "change_pct": None,
            "direction": "unchanged",
            "error": str(exc),
        }


def _fetch_stock_data(ticker_symbol: str, name: str, target_date: str) -> dict[str, Any]:
    """Fetch data for a single stock from the watchlist.

    Args:
        ticker_symbol: yfinance ticker symbol.
        name: Human-readable stock name.
        target_date: Target date as YYYY-MM-DD.

    Returns:
        Dictionary with stock data including close, change_pct, and prev_close.
    """
    try:
        ticker = yf.Ticker(ticker_symbol)

        end_date = datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = end_date - timedelta(days=10)  # buffer for holidays

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        hist = ticker.history(start=start_str, end=end_str)

        if hist.empty:
            logger.warning(
                "No stock data available for %s (%s) near %s",
                name, ticker_symbol, target_date,
            )
            return {
                "ticker": ticker_symbol,
                "name": name,
                "close": None,
                "change_pct": None,
                "prev_close": None,
            }

        closes = hist["Close"].tolist()
        current_close = float(closes[-1])
        prev_close = float(closes[-2]) if len(closes) >= 2 else current_close

        if prev_close:
            change_pct = round(((current_close - prev_close) / prev_close) * 100, 2)
        else:
            change_pct = 0.0

        return {
            "ticker": ticker_symbol,
            "name": name,
            "close": round(current_close, 2),
            "change_pct": change_pct,
            "prev_close": round(prev_close, 2),
        }

    except Exception as exc:
        logger.error("Error fetching stock %s (%s): %s", name, ticker_symbol, exc)
        return {
            "ticker": ticker_symbol,
            "name": name,
            "close": None,
            "change_pct": None,
            "prev_close": None,
            "error": str(exc),
        }


async def fetch(config: SourceConfig, date: str) -> dict[str, Any]:
    """Fetch Chinese market index data, sector indices, and top movers.

    Args:
        config: Source configuration with index, sector, and watchlist definitions.
        date: Target date (YYYY-MM-DD).

    Returns:
        Dictionary with index data, sector data, and movers for all configured tickers.
    """
    indices_config = config.get("indices", DEFAULT_INDICES)
    sectors_config = config.get("sectors", DEFAULT_SECTORS)
    watchlist_config = config.get("watchlist", DEFAULT_WATCHLIST)

    # yfinance is synchronous, so we call it directly

    # --- Indices ---
    indices: list[dict[str, Any]] = []
    for idx_cfg in indices_config:
        ticker_symbol = idx_cfg.get("ticker", "")
        name = idx_cfg.get("name", ticker_symbol)

        if not ticker_symbol:
            continue

        result = _fetch_index_data(ticker_symbol, name, date)
        indices.append(result)

    # --- Sectors ---
    sectors: list[dict[str, Any]] = []
    for sec_cfg in sectors_config:
        ticker_symbol = sec_cfg.get("ticker", "")
        name = sec_cfg.get("name", ticker_symbol)

        if not ticker_symbol:
            continue

        result = _fetch_sector_data(ticker_symbol, name, date)
        sectors.append(result)

    # --- Watchlist / Top Movers ---
    stock_results: list[dict[str, Any]] = []
    for stock_cfg in watchlist_config:
        ticker_symbol = stock_cfg.get("ticker", "")
        name = stock_cfg.get("name", ticker_symbol)

        if not ticker_symbol:
            continue

        result = _fetch_stock_data(ticker_symbol, name, date)
        stock_results.append(result)

    # Filter out stocks with no data, then sort by change_pct descending
    valid_stocks = [s for s in stock_results if s.get("change_pct") is not None]
    sorted_stocks = sorted(valid_stocks, key=lambda s: s["change_pct"], reverse=True)

    top_gainers = sorted_stocks[:MOVERS_COUNT]
    top_losers = sorted_stocks[-MOVERS_COUNT:] if len(sorted_stocks) >= MOVERS_COUNT else sorted_stocks

    # Compute summary
    available = [i for i in indices if i.get("value") is not None]
    all_holiday = all(i.get("market_holiday", False) for i in available) if available else True
    sectors_available = [s for s in sectors if s.get("value") is not None]

    return {
        "date": date,
        "indices": indices,
        "sectors": sectors,
        "movers": {
            "gainers": top_gainers,
            "losers": top_losers,
        },
        "summary": {
            "indices_fetched": len(available),
            "indices_failed": len(indices) - len(available),
            "sectors_fetched": len(sectors_available),
            "sectors_failed": len(sectors) - len(sectors_available),
            "watchlist_fetched": len(valid_stocks),
            "watchlist_failed": len(stock_results) - len(valid_stocks),
            "all_markets_closed": all_holiday,
        },
    }
