"""Environment-aware configuration loader.

Loads YAML config from config/sources.{env}.yaml and provides
source URLs, API keys (from env vars), timeouts, and retry settings.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

VALID_ENVS = ("dev", "staging", "prod")
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


@dataclass(frozen=True)
class RetryConfig:
    """Retry settings for HTTP requests."""

    max_retries: int = 3
    backoff_factor: float = 0.5
    retry_statuses: tuple[int, ...] = (429, 500, 502, 503, 504)


@dataclass(frozen=True)
class SourceConfig:
    """Configuration for a single data source."""

    name: str
    settings: dict[str, Any] = field(default_factory=dict)
    timeout: int = 30
    retry: RetryConfig = field(default_factory=RetryConfig)

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a setting by key with an optional default."""
        return self.settings.get(key, default)


@dataclass(frozen=True)
class AppConfig:
    """Top-level application configuration."""

    env: str
    sources: dict[str, SourceConfig]

    def get_source(self, name: str) -> SourceConfig | None:
        """Retrieve configuration for a named source."""
        return self.sources.get(name)


def detect_env(cli_env: str | None = None) -> str:
    """Detect the runtime environment.

    Priority:
      1. Explicit CLI flag
      2. CC_ENV environment variable
      3. Default to 'dev'
    """
    env = cli_env or os.environ.get("CC_ENV", "dev")
    if env not in VALID_ENVS:
        raise ValueError(f"Invalid environment '{env}'. Must be one of {VALID_ENVS}")
    return env


def _resolve_env_vars(value: Any) -> Any:
    """Recursively resolve ${VAR} references in config values."""
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        var_name = value[2:-1]
        return os.environ.get(var_name, "")
    if isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env_vars(item) for item in value]
    return value


def _build_source_config(name: str, raw: dict[str, Any]) -> SourceConfig:
    """Build a SourceConfig from raw YAML data."""
    timeout = raw.pop("timeout", 30)
    retry_raw = raw.pop("retry", {})
    retry = RetryConfig(
        max_retries=retry_raw.get("max_retries", 3),
        backoff_factor=retry_raw.get("backoff_factor", 0.5),
    )
    settings = _resolve_env_vars(raw)
    return SourceConfig(name=name, settings=settings, timeout=timeout, retry=retry)


def load_config(env: str | None = None, config_dir: Path | None = None) -> AppConfig:
    """Load and parse the YAML config for the given environment.

    Args:
        env: The environment name (dev/staging/prod). Auto-detected if None.
        config_dir: Override the config directory path.

    Returns:
        Fully resolved AppConfig instance.
    """
    resolved_env = detect_env(env)
    config_path = (config_dir or PROJECT_ROOT / "config") / f"sources.{resolved_env}.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        raw_config: dict[str, Any] = yaml.safe_load(f) or {}

    sources: dict[str, SourceConfig] = {}
    for source_name, source_raw in raw_config.items():
        if isinstance(source_raw, dict):
            sources[source_name] = _build_source_config(source_name, dict(source_raw))

    return AppConfig(env=resolved_env, sources=sources)
