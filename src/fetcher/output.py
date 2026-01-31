"""Output writer for raw fetched data.

Writes raw JSON files to the cc-data directory structure:
  {output_dir}/{date}/{source}.json

Each file includes metadata: fetch_timestamp, source_name, version.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fetcher import __version__


def write_raw(
    date: str,
    source: str,
    data: dict[str, Any] | list[Any],
    output_dir: str,
) -> Path:
    """Write raw fetched data to a JSON file.

    Args:
        date: Date string in YYYY-MM-DD format.
        source: Source identifier (e.g. 'parliament', 'statcan').
        data: The raw data payload to persist.
        output_dir: Base output directory path.

    Returns:
        Path to the written file.
    """
    out_path = Path(output_dir) / date
    out_path.mkdir(parents=True, exist_ok=True)

    envelope: dict[str, Any] = {
        "metadata": {
            "fetch_timestamp": datetime.now(UTC).isoformat(),
            "source_name": source,
            "version": __version__,
            "date": date,
        },
        "data": data,
    }

    file_path = out_path / f"{source}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False, indent=2, default=str)

    return file_path
