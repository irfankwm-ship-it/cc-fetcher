"""CLI entry point for cc-fetcher.

Usage:
    fetcher run                         # Run all fetchers for today
    fetcher run --source parliament     # Run single source
    fetcher run --env staging           # Use staging config
    fetcher run --date 2025-01-15       # Fetch for a specific date
    fetcher run --output-dir ./data     # Custom output directory
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import date as date_type
from pathlib import Path
from typing import Any

import click

from fetcher.config import AppConfig, load_config
from fetcher.output import write_raw
from fetcher.sources import SOURCE_REGISTRY, run_source

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("fetcher")


async def _run_single_source(
    name: str,
    config: AppConfig,
    date: str,
    output_dir: str,
) -> dict[str, Any]:
    """Run a single source fetcher and write output.

    Returns a result dict with status and optional error info.
    """
    source_config = config.get_source(name)
    if source_config is None:
        logger.warning("No config found for source '%s', using defaults", name)
        from fetcher.config import SourceConfig

        source_config = SourceConfig(name=name)

    logger.info("Fetching %s ...", name)
    try:
        data = await run_source(name, source_config, date)
        out_path = write_raw(date, name, data, output_dir)
        logger.info("  -> wrote %s", out_path)
        return {"source": name, "status": "ok", "output": str(out_path)}
    except Exception as exc:
        logger.error("  -> FAILED: %s", exc)
        return {"source": name, "status": "error", "error": str(exc)}


async def _run_all(
    config: AppConfig,
    date: str,
    output_dir: str,
    source_filter: str | None = None,
) -> list[dict[str, Any]]:
    """Run source fetchers, collecting results.

    One source failing does not stop others. Errors are captured and returned.
    """
    sources_to_run = [source_filter] if source_filter else list(SOURCE_REGISTRY.keys())
    results: list[dict[str, Any]] = []

    for name in sources_to_run:
        if name not in SOURCE_REGISTRY:
            logger.error("Unknown source: '%s'. Available: %s", name, list(SOURCE_REGISTRY.keys()))
            results.append({"source": name, "status": "error", "error": "Unknown source"})
            continue

        result = await _run_single_source(name, config, date, output_dir)
        results.append(result)

    return results


@click.group()
def main() -> None:
    """cc-fetcher: Raw data fetcher for the China Compass pipeline."""


@main.command()
@click.option(
    "--source",
    type=click.Choice(list(SOURCE_REGISTRY.keys()), case_sensitive=False),
    default=None,
    help="Run a single source fetcher. Omit to run all.",
)
@click.option(
    "--env",
    type=click.Choice(["dev", "staging", "prod"], case_sensitive=False),
    default=None,
    help="Environment (default: dev or CC_ENV).",
)
@click.option(
    "--date",
    "target_date",
    type=str,
    default=None,
    help="Target date as YYYY-MM-DD (default: today).",
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default=None,
    help="Output directory (default: ../cc-data/raw/{date}/).",
)
def run(
    source: str | None,
    env: str | None,
    target_date: str | None,
    output_dir: str | None,
) -> None:
    """Run data fetchers."""
    # Resolve date
    if target_date is None:
        target_date = date_type.today().isoformat()

    # Resolve output directory
    if output_dir is None:
        output_dir = str(Path(__file__).resolve().parent.parent.parent.parent / "cc-data" / "raw")

    # Load config
    try:
        config = load_config(env)
    except FileNotFoundError as exc:
        logger.error("Config error: %s", exc)
        sys.exit(1)

    logger.info("cc-fetcher starting [env=%s, date=%s]", config.env, target_date)
    logger.info("Output directory: %s", output_dir)

    if source:
        logger.info("Running single source: %s", source)
    else:
        logger.info("Running all sources: %s", list(SOURCE_REGISTRY.keys()))

    # Execute
    results = asyncio.run(_run_all(config, target_date, output_dir, source))

    # Summary
    ok = sum(1 for r in results if r["status"] == "ok")
    failed = sum(1 for r in results if r["status"] == "error")

    logger.info("Finished: %d succeeded, %d failed", ok, failed)

    if failed > 0:
        for r in results:
            if r["status"] == "error":
                logger.error("  FAILED: %s - %s", r["source"], r.get("error", "unknown"))
        sys.exit(1)


if __name__ == "__main__":
    main()
