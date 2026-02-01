"""Fetcher for Statistics Canada bilateral trade data.

Uses the StatCan Web Data Service (WDS) REST API to fetch
Canada-China merchandise trade figures (imports, exports)
from table 12-10-0011-01 (aggregate totals) and table
12-10-0121-01 (commodity-level breakdowns by HS section).

API docs: https://www.statcan.gc.ca/en/developers/wds/user-guide
Endpoint: getDataFromCubePidCoordAndLatestNPeriods

Coordinate system for table 12100011:
  Dim 1: Geography (1=Canada)
  Dim 2: Trade (1=Import, 2=Export)
  Dim 3: Basis (1=Customs, 2=Balance of payments)
  Dim 4: Seasonal adjustment (1=Unadjusted, 2=Seasonally adjusted)
  Dim 5: Principal trading partners (11=China)

Coordinate system for table 12100121 (commodity trade):
  Dim 1: Geography (1=Canada)
  Dim 2: Trade (1=Import, 2=Export)
  Dim 3: Principal trading partners -- best-guess mapping; partner
         code for China may be 6 in this table's dimension.
  Dim 4+: HS commodity section codes -- see COMMODITY_COORDS below.

  NOTE: The commodity coordinate mappings below are best-guess
  estimates based on the StatCan WDS coordinate convention.  They
  have not been fully verified against the live API metadata and may
  need adjustment once tested against the real endpoint.  The code
  handles per-commodity failures gracefully by returning an empty
  result for any coordinate that does not resolve.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from fetcher.config import SourceConfig

logger = logging.getLogger(__name__)

WDS_BASE = "https://www150.statcan.gc.ca/t1/wds/rest"
TABLE_PID = 12100011
COMMODITY_TABLE_PID = 12100121

# Scalar factor codes: 0=units, 3=thousands, 6=millions, 9=billions
SCALAR_LABELS = {0: "", 3: "thousands", 6: "millions", 9: "billions"}

# Trade data coordinates (Geography=Canada, Basis=Customs, Unadjusted, Partner=China)
TRADE_COORDS = [
    {"label": "Imports from China", "coordinate": "1.1.1.1.11.0.0.0.0.0"},
    {"label": "Exports to China", "coordinate": "1.2.1.1.11.0.0.0.0.0"},
]

# ---------------------------------------------------------------------------
# Commodity-level trade coordinates for table 12-10-0121-01
# ---------------------------------------------------------------------------
# Each entry maps a readable commodity group to a pair of StatCan WDS
# coordinates (import and export).  The coordinate format is:
#   Geography.Trade.Partner.CommoditySection
# where Trade 1=Import, 2=Export, Partner 6=China (best guess), and
# the commodity section dimension corresponds to HS chapter groupings.
#
# WARNING: These coordinates are best-effort approximations.  The real
# mapping depends on the cube member IDs that StatCan assigns, which
# can differ from simple sequential numbering.  If a coordinate fails,
# the fetcher will log a warning and skip that commodity.
# ---------------------------------------------------------------------------
COMMODITY_COORDS: list[dict[str, Any]] = [
    {
        "label": "Energy Products",
        "label_zh": "\u80fd\u6e90\u4ea7\u54c1",
        # HS Section V -- Mineral products (chapters 25-27)
        "import_coordinate": "1.1.6.5",
        "export_coordinate": "1.2.6.5",
    },
    {
        "label": "Canola & Oilseeds",
        "label_zh": "\u83dc\u7c7d\u6cb9\u548c\u6cb9\u7c7d",
        # HS Section II -- Vegetable products (chapters 6-14)
        "import_coordinate": "1.1.6.2",
        "export_coordinate": "1.2.6.2",
    },
    {
        "label": "Forest Products",
        "label_zh": "\u6797\u4ea7\u54c1",
        # HS Section IX -- Wood and articles of wood (chapters 44-46)
        "import_coordinate": "1.1.6.9",
        "export_coordinate": "1.2.6.9",
    },
    {
        "label": "Machinery & Equipment",
        "label_zh": "\u673a\u68b0\u8bbe\u5907",
        # HS Section XVI -- Machinery and mechanical appliances (ch 84-85)
        "import_coordinate": "1.1.6.16",
        "export_coordinate": "1.2.6.16",
    },
    {
        "label": "Electronics",
        "label_zh": "\u7535\u5b50\u4ea7\u54c1",
        # HS Section XVI also covers electrical machinery (ch 85).
        # Use Section XVIII (optical, photographic, precision instruments,
        # chapters 90-92) as a proxy for electronics.
        "import_coordinate": "1.1.6.18",
        "export_coordinate": "1.2.6.18",
    },
    {
        "label": "Metals & Minerals",
        "label_zh": "\u91d1\u5c5e\u548c\u77ff\u4ea7",
        # HS Section XV -- Base metals and articles (chapters 72-83)
        "import_coordinate": "1.1.6.15",
        "export_coordinate": "1.2.6.15",
    },
]


def _determine_trend(
    latest: float | None,
    previous: float | None,
) -> str:
    """Return a simple trend label comparing two values.

    Returns ``"up"`` if the latest value is more than 1 % above the previous,
    ``"down"`` if more than 1 % below, or ``"stable"`` otherwise (including
    when either value is ``None``).
    """
    if latest is None or previous is None or previous == 0:
        return "stable"
    pct_change = (latest - previous) / abs(previous)
    if pct_change > 0.01:
        return "up"
    if pct_change < -0.01:
        return "down"
    return "stable"


def _extract_latest_and_previous(
    points: list[dict[str, Any]],
) -> tuple[float | None, float | None]:
    """Extract the latest and second-latest values from a data-point list.

    The StatCan WDS returns data points in chronological order (oldest first).
    """
    if not points:
        return None, None
    latest = points[-1].get("value")
    previous = points[-2].get("value") if len(points) >= 2 else None
    return latest, previous


async def _fetch_commodities(
    client: httpx.AsyncClient,
    base_url: str,
    timeout: int,
    periods: int,
) -> list[dict[str, Any]]:
    """Fetch commodity-level Canada-China trade data from table 12-10-0121-01.

    Makes individual WDS API calls per commodity coordinate pair.  If the
    underlying coordinate mapping is incorrect, the API will return a
    non-SUCCESS status and the commodity will be skipped with a warning.

    Args:
        client: An open ``httpx.AsyncClient``.
        base_url: StatCan WDS base URL.
        timeout: Request timeout in seconds.
        periods: Number of most-recent periods to request.

    Returns:
        A list of commodity dicts, each containing ``name``, ``name_zh``,
        ``export_cad_millions``, ``import_cad_millions``,
        ``balance_cad_millions``, and ``trend``.  Returns an empty list
        if the overall request fails.
    """
    # Build a single batched payload for all commodity coordinates.
    # Each commodity needs two entries: one for imports, one for exports.
    payload: list[dict[str, Any]] = []
    for comm in COMMODITY_COORDS:
        payload.append({
            "productId": COMMODITY_TABLE_PID,
            "coordinate": comm["import_coordinate"],
            "latestN": periods,
        })
        payload.append({
            "productId": COMMODITY_TABLE_PID,
            "coordinate": comm["export_coordinate"],
            "latestN": periods,
        })

    try:
        resp = await client.post(
            f"{base_url}/getDataFromCubePidCoordAndLatestNPeriods",
            json=payload,
            timeout=timeout,
        )
        resp.raise_for_status()
        results = resp.json()
    except httpx.HTTPStatusError as exc:
        logger.warning(
            "StatCan commodity query HTTP error: %s -- returning empty commodities",
            exc.response.status_code,
        )
        return []
    except httpx.RequestError as exc:
        logger.warning(
            "StatCan commodity query failed: %s -- returning empty commodities",
            exc,
        )
        return []

    commodities: list[dict[str, Any]] = []

    for idx, comm in enumerate(COMMODITY_COORDS):
        import_idx = idx * 2
        export_idx = idx * 2 + 1

        import_item = results[import_idx] if import_idx < len(results) else {}
        export_item = results[export_idx] if export_idx < len(results) else {}

        # --- imports -------------------------------------------------------
        import_latest: float | None = None
        import_previous: float | None = None
        if import_item.get("status") == "SUCCESS":
            pts = import_item.get("object", {}).get("vectorDataPoint", [])
            import_latest, import_previous = _extract_latest_and_previous(pts)
        else:
            logger.warning(
                "StatCan commodity %s import query failed (status=%s) "
                "-- coordinate %s may be incorrect",
                comm["label"],
                import_item.get("status", "MISSING"),
                comm["import_coordinate"],
            )

        # --- exports -------------------------------------------------------
        export_latest: float | None = None
        export_previous: float | None = None
        if export_item.get("status") == "SUCCESS":
            pts = export_item.get("object", {}).get("vectorDataPoint", [])
            export_latest, export_previous = _extract_latest_and_previous(pts)
        else:
            logger.warning(
                "StatCan commodity %s export query failed (status=%s) "
                "-- coordinate %s may be incorrect",
                comm["label"],
                export_item.get("status", "MISSING"),
                comm["export_coordinate"],
            )

        # Skip entirely if both sides failed
        if import_latest is None and export_latest is None:
            continue

        # Balance = exports - imports (positive means Canada exports more)
        balance: float | None = None
        if export_latest is not None and import_latest is not None:
            balance = round(export_latest - import_latest, 1)

        # Trend is based on total trade volume (imports + exports)
        total_latest = (export_latest or 0) + (import_latest or 0)
        total_previous = (export_previous or 0) + (import_previous or 0)
        trend = _determine_trend(total_latest, total_previous)

        commodities.append({
            "name": comm["label"],
            "name_zh": comm["label_zh"],
            "export_cad_millions": export_latest,
            "import_cad_millions": import_latest,
            "balance_cad_millions": balance,
            "trend": trend,
        })

    return commodities


async def fetch(config: SourceConfig, date: str) -> dict[str, Any]:
    """Fetch bilateral trade data from Statistics Canada WDS.

    Args:
        config: Source configuration with base_url, timeout.
        date: Target date (YYYY-MM-DD).

    Returns:
        Dictionary with imports, exports, balance, period info, and
        commodity-level breakdowns.
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

        # Fetch commodity breakdowns (tolerates failures gracefully)
        commodities = await _fetch_commodities(client, base_url, timeout, periods)

    # Parse aggregate results
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
        "commodities": commodities,
        "totals": {
            "total_exports_cad": latest_exports,
            "total_imports_cad": latest_imports,
            "trade_balance_cad": balance,
        },
    }
