"""Fetcher for Statistics Canada bilateral trade data.

Uses the StatCan Web Data Service (WDS) REST API to fetch
Canada-China merchandise trade figures (imports, exports)
from table 12-10-0011-01 (aggregate totals) and table
12-10-0175-01 (commodity-level breakdowns by NAPCS section).

API docs: https://www.statcan.gc.ca/en/developers/wds/user-guide
Endpoint: getDataFromCubePidCoordAndLatestNPeriods

Coordinate system for table 12100011:
  Dim 1: Geography (1=Canada)
  Dim 2: Trade (1=Import, 2=Export)
  Dim 3: Basis (1=Customs, 2=Balance of payments)
  Dim 4: Seasonal adjustment (1=Unadjusted, 2=Seasonally adjusted)
  Dim 5: Principal trading partners (11=China)

Coordinate system for table 12100175 (commodity trade):
  Dim 1: Geography (1=Canada)
  Dim 2: Trade (1=Import, 2=Domestic export, 3=Re-export)
  Dim 3: NAPCS Commodity (2-13, see COMMODITY_COORDS below)
  Dim 4: Principal trading partners (3=China)
  Values are in CAD x 1,000 (scalarFactorCode=3).
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from fetcher.config import SourceConfig

logger = logging.getLogger(__name__)

WDS_BASE = "https://www150.statcan.gc.ca/t1/wds/rest"
TABLE_PID = 12100011
COMMODITY_TABLE_PID = 12100175

# Scalar factor codes: 0=units, 3=thousands, 6=millions, 9=billions
SCALAR_LABELS = {0: "", 3: "thousands", 6: "millions", 9: "billions"}

# Trade data coordinates (Geography=Canada, Basis=Customs, Unadjusted, Partner=China)
TRADE_COORDS = [
    {"label": "Imports from China", "coordinate": "1.1.1.1.11.0.0.0.0.0"},
    {"label": "Exports to China", "coordinate": "1.2.1.1.11.0.0.0.0.0"},
]

# ---------------------------------------------------------------------------
# Commodity-level trade coordinates for table 12-10-0175-01
# ---------------------------------------------------------------------------
# Coordinate format: Geography.Trade.Commodity.Partner.0.0.0.0.0.0
#   Geography: 1=Canada
#   Trade: 1=Import, 2=Domestic export
#   Commodity: NAPCS category member ID (2-13)
#   Partner: 3=China
# Values are in CAD x 1,000 (divide by 1000 to get millions).
# Member ID 14 (Other BoP adjustments) has no country-level data.
# ---------------------------------------------------------------------------
COMMODITY_COORDS: list[dict[str, Any]] = [
    {
        "label": "Electronic & Electrical Equipment",
        "label_zh": "电子电气设备",
        # NAPCS C18 (member 9)
        "import_coordinate": "1.1.9.3.0.0.0.0.0.0",
        "export_coordinate": "1.2.9.3.0.0.0.0.0.0",
    },
    {
        "label": "Consumer Goods",
        "label_zh": "消费品",
        # NAPCS C22 (member 12)
        "import_coordinate": "1.1.12.3.0.0.0.0.0.0",
        "export_coordinate": "1.2.12.3.0.0.0.0.0.0",
    },
    {
        "label": "Industrial Machinery & Equipment",
        "label_zh": "工业机械设备",
        # NAPCS C17 (member 8)
        "import_coordinate": "1.1.8.3.0.0.0.0.0.0",
        "export_coordinate": "1.2.8.3.0.0.0.0.0.0",
    },
    {
        "label": "Metal & Mineral Products",
        "label_zh": "金属和矿产品",
        # NAPCS C14 (member 5)
        "import_coordinate": "1.1.5.3.0.0.0.0.0.0",
        "export_coordinate": "1.2.5.3.0.0.0.0.0.0",
    },
    {
        "label": "Forestry & Building Materials",
        "label_zh": "林产品和建筑材料",
        # NAPCS C16 (member 7)
        "import_coordinate": "1.1.7.3.0.0.0.0.0.0",
        "export_coordinate": "1.2.7.3.0.0.0.0.0.0",
    },
    {
        "label": "Energy Products",
        "label_zh": "能源产品",
        # NAPCS C12 (member 3)
        "import_coordinate": "1.1.3.3.0.0.0.0.0.0",
        "export_coordinate": "1.2.3.3.0.0.0.0.0.0",
    },
    {
        "label": "Farm, Fishing & Food Products",
        "label_zh": "农渔食品",
        # NAPCS C11 (member 2) -- includes canola/oilseeds
        "import_coordinate": "1.1.2.3.0.0.0.0.0.0",
        "export_coordinate": "1.2.2.3.0.0.0.0.0.0",
    },
    {
        "label": "Chemicals, Plastics & Rubber",
        "label_zh": "化工塑料橡胶",
        # NAPCS C15 (member 6)
        "import_coordinate": "1.1.6.3.0.0.0.0.0.0",
        "export_coordinate": "1.2.6.3.0.0.0.0.0.0",
    },
    {
        "label": "Motor Vehicles & Parts",
        "label_zh": "汽车及零部件",
        # NAPCS C19 (member 10)
        "import_coordinate": "1.1.10.3.0.0.0.0.0.0",
        "export_coordinate": "1.2.10.3.0.0.0.0.0.0",
    },
]


def _to_millions(val: float | None) -> float | None:
    """Convert a value in thousands to millions."""
    return round(val / 1000, 1) if val is not None else None


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
    """Fetch commodity-level Canada-China trade data from table 12-10-0175-01.

    Makes a single batched WDS API call for all commodity coordinates.
    Values from the API are in CAD x 1,000 and are converted to millions.

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

    # The WDS batch API does NOT preserve request order — results are
    # sorted by vectorId.  Map results back by coordinate string.
    result_by_coord: dict[str, dict[str, Any]] = {}
    for item in results:
        if item.get("status") == "SUCCESS":
            coord = item.get("object", {}).get("coordinate", "")
            result_by_coord[coord] = item

    commodities: list[dict[str, Any]] = []

    for comm in COMMODITY_COORDS:
        import_item = result_by_coord.get(comm["import_coordinate"], {})
        export_item = result_by_coord.get(comm["export_coordinate"], {})

        # --- imports -------------------------------------------------------
        import_latest: float | None = None
        import_previous: float | None = None
        if import_item:
            pts = import_item.get("object", {}).get("vectorDataPoint", [])
            import_latest, import_previous = _extract_latest_and_previous(pts)
        else:
            logger.warning(
                "StatCan commodity %s import not found for coordinate %s",
                comm["label"],
                comm["import_coordinate"],
            )

        # --- exports -------------------------------------------------------
        export_latest: float | None = None
        export_previous: float | None = None
        if export_item:
            pts = export_item.get("object", {}).get("vectorDataPoint", [])
            export_latest, export_previous = _extract_latest_and_previous(pts)
        else:
            logger.warning(
                "StatCan commodity %s export not found for coordinate %s",
                comm["label"],
                comm["export_coordinate"],
            )

        # Skip entirely if both sides failed
        if import_latest is None and export_latest is None:
            continue

        imp_m = _to_millions(import_latest)
        exp_m = _to_millions(export_latest)
        imp_prev_m = _to_millions(import_previous)
        exp_prev_m = _to_millions(export_previous)

        # Balance = exports - imports (positive means Canada exports more)
        balance: float | None = None
        if exp_m is not None and imp_m is not None:
            balance = round(exp_m - imp_m, 1)

        # Trend is based on total trade volume (imports + exports)
        total_latest = (imp_m or 0) + (exp_m or 0)
        total_previous = (imp_prev_m or 0) + (exp_prev_m or 0)
        trend = _determine_trend(total_latest, total_previous)

        commodities.append({
            "name": comm["label"],
            "name_zh": comm["label_zh"],
            "export_cad_millions": exp_m,
            "import_cad_millions": imp_m,
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
