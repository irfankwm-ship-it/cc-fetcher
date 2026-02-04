#!/usr/bin/env python3
"""Content Disarm and Reconstruction (CDR) cleaner for fetched data.

This script implements the CDR approach:
1. Parse incoming JSON data
2. Extract ONLY expected fields with strict type validation
3. Sanitize all string content (remove HTML/scripts)
4. Reconstruct a brand-new, clean JSON file

This runs in an AIR-GAPPED container with no internet access.
Even if a malicious payload exploits a vulnerability here,
it cannot exfiltrate data or call home.

Usage:
    python cdr_cleaner.py /staging /clean
    python cdr_cleaner.py /staging/2026-02-03 /clean/2026-02-03
"""

from __future__ import annotations

import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import bleach

# ===========================================================================
# Configuration
# ===========================================================================

# Maximum sizes to prevent memory exhaustion
MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024  # 50MB
MAX_STRING_LENGTH = 50_000  # 50K chars per string
MAX_ARRAY_LENGTH = 10_000  # 10K items per array
MAX_NESTING_DEPTH = 20

# Allowed HTML tags (strip everything else)
ALLOWED_TAGS: list[str] = []  # Empty = strip ALL HTML

# Fields we expect in our data schema
EXPECTED_METADATA_FIELDS = {"source_name", "fetch_timestamp", "date", "version"}


# ===========================================================================
# Sanitization Functions
# ===========================================================================

def sanitize_string(value: str, max_length: int = MAX_STRING_LENGTH) -> str:
    """Sanitize a string value using CDR approach.

    - Strip ALL HTML tags (not just dangerous ones)
    - Remove control characters
    - Truncate to max length
    - Normalize whitespace
    """
    if not isinstance(value, str):
        return ""

    # Strip ALL HTML tags - this is CDR, not filtering
    clean = bleach.clean(value, tags=ALLOWED_TAGS, strip=True)

    # Remove control characters (except newline, tab)
    clean = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]", "", clean)

    # Normalize excessive whitespace
    clean = re.sub(r"\s{10,}", "  ", clean)

    # Truncate
    if len(clean) > max_length:
        clean = clean[:max_length] + "..."

    return clean.strip()


def sanitize_url(url: str) -> str | None:
    """Sanitize and validate a URL.

    Only allows http:// and https:// protocols.
    Returns None for invalid/dangerous URLs.
    """
    if not isinstance(url, str):
        return None

    url = url.strip()

    # Only allow http/https
    if not url.startswith(("http://", "https://")):
        return None

    # Check for encoded dangerous content
    url_lower = url.lower()
    dangerous = ["javascript:", "data:", "vbscript:", "file:", "<script", "onerror="]
    if any(d in url_lower for d in dangerous):
        return None

    # Truncate excessively long URLs
    if len(url) > 2048:
        return None

    return url


def reconstruct_value(value: Any, depth: int = 0) -> Any:
    """Recursively reconstruct a value, sanitizing as we go.

    This is the core CDR function - we don't modify the original,
    we BUILD a new clean version from scratch.
    """
    if depth > MAX_NESTING_DEPTH:
        return None  # Prevent stack overflow from deeply nested data

    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        # Validate numeric ranges
        if abs(value) > 1e15:  # Prevent huge numbers
            return 0
        return value

    if isinstance(value, str):
        return sanitize_string(value)

    if isinstance(value, list):
        # Limit array size
        if len(value) > MAX_ARRAY_LENGTH:
            value = value[:MAX_ARRAY_LENGTH]
        return [reconstruct_value(item, depth + 1) for item in value]

    if isinstance(value, dict):
        return {
            sanitize_string(str(k), max_length=200): reconstruct_value(v, depth + 1)
            for k, v in value.items()
        }

    # Unknown type - convert to string
    return sanitize_string(str(value))


# ===========================================================================
# Schema-Aware Reconstruction
# ===========================================================================

def reconstruct_article(article: dict[str, Any]) -> dict[str, Any] | None:
    """Reconstruct an article object with strict field validation."""
    if not isinstance(article, dict):
        return None

    clean = {}

    # Required string fields
    for field in ["title", "source"]:
        if field in article:
            val = sanitize_string(str(article[field]), max_length=500)
            if val:
                clean[field] = val

    # URL fields
    for field in ["source_url", "url", "link"]:
        if field in article:
            url = sanitize_url(article[field])
            if url:
                clean[field] = url

    # Body/content fields (allow longer text)
    for field in ["body", "body_text", "content", "summary", "description"]:
        if field in article:
            clean[field] = sanitize_string(str(article[field]), max_length=MAX_STRING_LENGTH)

    # Date field
    if "date" in article:
        date_val = str(article["date"])[:20]  # Limit length
        # Validate it looks like a date
        if re.match(r"^\d{4}-\d{2}-\d{2}", date_val):
            clean["date"] = date_val

    # Optional fields (sanitized)
    for field in ["language", "category", "author"]:
        if field in article:
            clean[field] = sanitize_string(str(article[field]), max_length=100)

    # Tags (list of strings)
    if "relevance_tags" in article and isinstance(article["relevance_tags"], list):
        clean["relevance_tags"] = [
            sanitize_string(str(t), max_length=50)
            for t in article["relevance_tags"][:20]  # Max 20 tags
        ]

    return clean if clean.get("title") or clean.get("source_url") else None


def reconstruct_fetcher_output(data: dict[str, Any]) -> dict[str, Any]:
    """Reconstruct a complete fetcher output file."""
    clean: dict[str, Any] = {}

    # Reconstruct metadata
    if "metadata" in data and isinstance(data["metadata"], dict):
        meta = data["metadata"]
        clean["metadata"] = {
            "source_name": sanitize_string(str(meta.get("source_name", "")), 50),
            "fetch_timestamp": sanitize_string(str(meta.get("fetch_timestamp", "")), 50),
            "date": sanitize_string(str(meta.get("date", "")), 20),
            "version": sanitize_string(str(meta.get("version", "0.1.0")), 20),
            "cleaned_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        }

    # Reconstruct data section
    if "data" in data and isinstance(data["data"], dict):
        raw_data = data["data"]
        clean_data: dict[str, Any] = {}

        # Date field
        if "date" in raw_data:
            clean_data["date"] = sanitize_string(str(raw_data["date"]), 20)

        # Source URL
        if "source_url" in raw_data:
            url = sanitize_url(raw_data["source_url"])
            if url:
                clean_data["source_url"] = url

        # Counts (integers only)
        for field in ["total_scraped", "total_fetched", "total_relevant"]:
            if field in raw_data:
                try:
                    clean_data[field] = max(0, min(int(raw_data[field]), 100000))
                except (ValueError, TypeError):
                    clean_data[field] = 0

        # Articles array
        if "articles" in raw_data and isinstance(raw_data["articles"], list):
            articles = raw_data["articles"][:MAX_ARRAY_LENGTH]
            clean_articles = []
            for art in articles:
                clean_art = reconstruct_article(art)
                if clean_art:
                    clean_articles.append(clean_art)
            clean_data["articles"] = clean_articles

        # Error field (sanitized)
        if "error" in raw_data:
            clean_data["error"] = sanitize_string(str(raw_data["error"]), 500)

        # Feed errors (for news sources)
        if "feed_errors" in raw_data and isinstance(raw_data["feed_errors"], list):
            clean_data["feed_errors"] = [
                {
                    "feed": sanitize_url(e.get("feed", "")) or "unknown",
                    "error": sanitize_string(str(e.get("error", "")), 200),
                }
                for e in raw_data["feed_errors"][:50]
                if isinstance(e, dict)
            ]

        # Generic reconstruction for other fields
        for key, value in raw_data.items():
            if key not in clean_data:
                clean_data[key] = reconstruct_value(value)

        clean["data"] = clean_data

    return clean


# ===========================================================================
# File Processing
# ===========================================================================

def process_file(input_path: Path, output_path: Path) -> bool:
    """Process a single JSON file through CDR."""
    try:
        # Check file size
        if input_path.stat().st_size > MAX_FILE_SIZE_BYTES:
            print(f"  SKIP: {input_path.name} - file too large")
            return False

        # Read and parse
        with open(input_path, encoding="utf-8") as f:
            raw = json.load(f)

        # Reconstruct
        clean = reconstruct_fetcher_output(raw)

        # Write clean version
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(clean, f, ensure_ascii=False, indent=2)

        print(f"  OK: {input_path.name}")
        return True

    except json.JSONDecodeError as e:
        print(f"  FAIL: {input_path.name} - invalid JSON: {e}")
        return False
    except Exception as e:
        print(f"  FAIL: {input_path.name} - {type(e).__name__}: {e}")
        return False


def process_directory(input_dir: Path, output_dir: Path) -> tuple[int, int]:
    """Process all JSON files in a directory."""
    passed = 0
    failed = 0

    # Find all JSON files
    json_files = list(input_dir.rglob("*.json"))

    if not json_files:
        print(f"No JSON files found in {input_dir}")
        return 0, 0

    for input_path in json_files:
        # Compute output path preserving directory structure
        relative = input_path.relative_to(input_dir)
        output_path = output_dir / relative

        if process_file(input_path, output_path):
            passed += 1
        else:
            failed += 1

    return passed, failed


# ===========================================================================
# Main
# ===========================================================================

def main() -> int:
    if len(sys.argv) < 3:
        print("Usage: cdr_cleaner.py <input_dir> <output_dir>")
        return 1

    input_dir = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])

    if not input_dir.exists():
        print(f"Input directory does not exist: {input_dir}")
        return 1

    print(f"CDR Cleaner")
    print(f"  Input:  {input_dir}")
    print(f"  Output: {output_dir}")
    print("-" * 60)

    passed, failed = process_directory(input_dir, output_dir)

    print("-" * 60)
    print(f"Results: {passed} cleaned, {failed} failed")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
