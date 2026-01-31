"""Fetcher for Statistics Canada bilateral trade data.

Uses the StatCan Web Data Service (WDS) REST API to fetch
Canada-China merchandise trade figures (imports, exports)
from table 12-10-0011-01.

API docs: https://www.statcan.gc.ca/en/developers/wds/user-guide
Endpoint: getDataFromCubePidCoordAndLatestNPeriods

Coordinate system for table 12100011:
  Dim 1: Geography (1=Canada)
  Dim 2: Trade (1=Import, 2=Export)
  Dim 3: Basis (1=Customs, 2=Balance of payments)
  Dim 4: Seasonal adjustment (1=Unadjusted, 2=Seasonally adjusted)
  Dim 5: Principal trading partners (11=China)
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from fetcher.config import SourceConfig

logger = logging.getLogger(__name__)

WDS_BASE = "https://www150.statcan.gc.ca/t1/wds/rest"
TABLE_PID = 12100011

# Scalar factor codes: 0=units, 3=thousands, 6=millions, 9=billions
SCALAR_LABELS = {0: "", 3: "thousands", 6: "millions", 9: "billions"}

# Trade data coordinates (Geography=Canada, Basis=Customs, Unadjusted, Partner=China)
TRADE_COORDS = [
    {"label": "Imports from China", "coordinate": "1.1.1.1.11.0.0.0.0.0"},
    {"label": "Exports to China", "coordinate": "1.2.1.1.11.0.0.0.0.0"},
]


async def fetch(config: SourceConfig, date: str) -> dict[str, Any]:
    """Fetch bilateral trade data from Statistics Canada WDS.

    Args:
        config: Source configuration with base_url, timeout.
        date: Target date (YYYY-MM-DD).

    Returns:
        Dictionary with imports, exports, balance, and period info.
    """
    base_url = config.get("base_url", WDS_BASE)
    periods = config.get("periods", 3)
    timeout = config.timeout

    payload = [
        {
            "productId": TABLE_PID,
            "coordinate": coord["coordinate"],
            "latestN": periods,
        }
        for coord in TRADE_COORDS
    ]

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                f"{base_url}/getDataFromCubePidCoordAndLatestNPeriods",
                json=payload,
                timeout=timeout,
            )
            resp.raise_for_status()
            results = resp.json()
        except httpx.HTTPStatusError as exc:
            logger.error("StatCan WDS error: HTTP %s", exc.response.status_code)
            return {
                "date": date,
                "error": f"HTTP {exc.response.status_code}",
                "commodities": [],
                "totals": {},
            }
        except httpx.RequestError as exc:
            logger.error("StatCan WDS request failed: %s", exc)
            return {
                "date": date,
                "error": str(exc),
                "commodities": [],
                "totals": {},
            }

    # Parse results
    series: dict[str, list[dict[str, Any]]] = {}
    for i, coord in enumerate(TRADE_COORDS):
        label = coord["label"]
        item = results[i] if i < len(results) else {}

        if item.get("status") != "SUCCESS":
            logger.warning("StatCan %s query failed: %s", label, item.get("status"))
            series[label] = []
            continue

        points = item.get("object", {}).get("vectorDataPoint", [])
        scalar_code = points[0].get("scalarFactorCode", 6) if points else 6
        scalar_label = SCALAR_LABELS.get(scalar_code, "")

        series[label] = [
            {
                "period": p.get("refPer", ""),
                "value": p.get("value"),
                "scalar": scalar_label,
            }
            for p in points
        ]

    # Build summary from most recent period
    imports_series = series.get("Imports from China", [])
    exports_series = series.get("Exports to China", [])

    latest_imports = imports_series[-1]["value"] if imports_series else None
    latest_exports = exports_series[-1]["value"] if exports_series else None
    latest_period = (imports_series[-1]["period"] if imports_series
                     else exports_series[-1]["period"] if exports_series
                     else "")

    balance = None
    if latest_imports is not None and latest_exports is not None:
        balance = round(latest_exports - latest_imports, 1)

    return {
        "date": date,
        "country": "China",
        "table_id": str(TABLE_PID),
        "reference_period": latest_period,
        "scalar_factor": "millions of Canadian dollars",
        "imports_cad_millions": latest_imports,
        "exports_cad_millions": latest_exports,
        "balance_cad_millions": balance,
        "series": series,
        "commodities": [],
        "totals": {
            "total_exports_cad": latest_exports,
            "total_imports_cad": latest_imports,
            "trade_balance_cad": balance,
        },
    }
