#!/usr/bin/env python3
"""Validate fetcher output for security and data integrity.

This script checks fetched data before it's moved to the main data store.
It validates:
- JSON structure and encoding
- No embedded scripts or malicious content
- File size limits
- Expected schema structure
- No path traversal attempts in URLs

Usage:
    python validate_output.py /data/raw/2026-02-03/
    python validate_output.py /data/raw/2026-02-03/mfa.json
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

# Security limits
MAX_FILE_SIZE_MB = 50
MAX_STRING_LENGTH = 100_000
MAX_URL_LENGTH = 2048

# Patterns that should never appear in fetched data
# These are crafted to minimize false positives while catching real threats
SUSPICIOUS_PATTERNS = [
    r"<script[^>]*>",  # Script tags
    r"javascript:",  # JavaScript URLs
    r"data:text/html",  # Data URLs with HTML
    r"on(click|error|load|mouseover|focus|blur)\s*=",  # Common event handlers
    r"\.\./\.\./",  # Multiple path traversal (single ../ may be legit in text)
    r"file://",  # File protocol
    r"\\x[0-9a-fA-F]{2}\\x[0-9a-fA-F]{2}",  # Multiple hex escapes (obfuscation)
    r"eval\s*\(",  # eval() calls
    r"document\.(write|cookie|location|body|head|getElementById|querySelector)",  # DOM methods
    r"window\.(location|open|eval|execScript)",  # Window methods
    r"<iframe[^>]*>",  # Iframe injection
    r"<object[^>]*>",  # Object injection
    r"<embed[^>]*>",  # Embed injection
]

SUSPICIOUS_REGEX = re.compile("|".join(SUSPICIOUS_PATTERNS), re.IGNORECASE)


class ValidationError(Exception):
    """Raised when validation fails."""

    pass


def check_file_size(path: Path) -> None:
    """Check file size is within limits."""
    size_mb = path.stat().st_size / (1024 * 1024)
    if size_mb > MAX_FILE_SIZE_MB:
        raise ValidationError(f"File too large: {size_mb:.2f}MB > {MAX_FILE_SIZE_MB}MB")


def check_json_structure(data: dict[str, Any]) -> None:
    """Validate expected JSON envelope structure."""
    if not isinstance(data, dict):
        raise ValidationError("Root must be a JSON object")

    # Expect our standard envelope format
    if "metadata" not in data and "data" not in data:
        # Allow raw data format too
        pass

    if "metadata" in data:
        meta = data["metadata"]
        if not isinstance(meta, dict):
            raise ValidationError("metadata must be an object")
        # Check for expected metadata fields
        expected = {"source_name", "fetch_timestamp", "date"}
        if not expected.intersection(meta.keys()):
            raise ValidationError(f"metadata missing expected fields: {expected}")


def check_string_content(value: str, path: str = "") -> None:
    """Check string for suspicious content."""
    if len(value) > MAX_STRING_LENGTH:
        raise ValidationError(f"String too long at {path}: {len(value)} > {MAX_STRING_LENGTH}")

    # Check for suspicious patterns
    match = SUSPICIOUS_REGEX.search(value)
    if match:
        raise ValidationError(
            f"Suspicious pattern found at {path}: '{match.group()[:50]}...'"
        )


def is_url_field(path: str) -> bool:
    """Check if a field path is expected to contain a URL."""
    url_fields = {"url", "source_url", "link", "href", "image_url", "thumbnail"}
    parts = path.lower().replace("[", ".").replace("]", "").split(".")
    return any(part in url_fields for part in parts)


def check_url(url: str, path: str = "") -> None:
    """Validate URL format and content."""
    if len(url) > MAX_URL_LENGTH:
        raise ValidationError(f"URL too long at {path}: {len(url)} > {MAX_URL_LENGTH}")

    # Check for path traversal
    if ".." in url or url.startswith("/etc/") or url.startswith("/proc/"):
        raise ValidationError(f"Path traversal attempt at {path}: {url[:100]}")

    # Only allow http/https protocols for actual URL fields
    if url.startswith(("http://", "https://")):
        pass
    elif "://" in url:
        protocol = url.split("://")[0]
        # Only raise for dangerous protocols, allow others in non-URL fields
        dangerous_protocols = {"javascript", "data", "file", "vbscript"}
        if protocol.lower() in dangerous_protocols:
            raise ValidationError(f"Dangerous protocol at {path}: {protocol}")


def validate_value(value: Any, path: str = "") -> None:
    """Recursively validate a JSON value."""
    if isinstance(value, str):
        check_string_content(value, path)
        # Only validate as URL if it's a URL field or starts with http(s)
        if is_url_field(path) and value.startswith(("http://", "https://")):
            check_url(value, path)
        # Always check for dangerous protocols in any string
        elif "://" in value:
            for proto in ["javascript:", "data:text/html", "file://", "vbscript:"]:
                if proto in value.lower():
                    raise ValidationError(f"Dangerous protocol at {path}: {proto}")
    elif isinstance(value, dict):
        for key, val in value.items():
            validate_value(val, f"{path}.{key}" if path else key)
    elif isinstance(value, list):
        for i, item in enumerate(value):
            validate_value(item, f"{path}[{i}]")
    # Numbers, bools, None are safe


def validate_file(path: Path) -> list[str]:
    """Validate a single JSON file. Returns list of warnings."""
    warnings: list[str] = []

    # Check file size
    check_file_size(path)

    # Parse JSON
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValidationError(f"Invalid JSON: {e}")
    except UnicodeDecodeError as e:
        raise ValidationError(f"Invalid encoding: {e}")

    # Validate structure
    check_json_structure(data)

    # Recursively validate all content
    validate_value(data)

    return warnings


def validate_directory(dir_path: Path) -> tuple[int, int, list[str]]:
    """Validate all JSON files in a directory.

    Returns: (passed_count, failed_count, errors)
    """
    passed = 0
    failed = 0
    errors: list[str] = []

    json_files = list(dir_path.glob("*.json"))
    if not json_files:
        errors.append(f"No JSON files found in {dir_path}")
        return 0, 0, errors

    for json_file in json_files:
        try:
            warnings = validate_file(json_file)
            passed += 1
            for w in warnings:
                print(f"  WARNING [{json_file.name}]: {w}")
        except ValidationError as e:
            failed += 1
            errors.append(f"{json_file.name}: {e}")
        except Exception as e:
            failed += 1
            errors.append(f"{json_file.name}: Unexpected error: {e}")

    return passed, failed, errors


def main() -> int:
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: validate_output.py <path>")
        print("  path: Directory or JSON file to validate")
        return 1

    target = Path(sys.argv[1])

    if not target.exists():
        print(f"ERROR: Path does not exist: {target}")
        return 1

    print(f"Validating: {target}")
    print("-" * 60)

    if target.is_file():
        try:
            warnings = validate_file(target)
            print(f"PASSED: {target.name}")
            for w in warnings:
                print(f"  WARNING: {w}")
            return 0
        except ValidationError as e:
            print(f"FAILED: {target.name}")
            print(f"  ERROR: {e}")
            return 1
    else:
        passed, failed, errors = validate_directory(target)
        print("-" * 60)
        print(f"Results: {passed} passed, {failed} failed")

        if errors:
            print("\nErrors:")
            for err in errors:
                print(f"  - {err}")
            return 1

        print("\nAll validations passed!")
        return 0


if __name__ == "__main__":
    sys.exit(main())
