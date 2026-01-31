"""Tests for the CLI entry point."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner

from fetcher.cli import main
from fetcher.output import write_raw


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def config_dir(tmp_path: Path) -> Path:
    """Create a temporary config directory with dev config."""
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    (cfg_dir / "sources.dev.yaml").write_text(
        """
parliament:
  legisinfo_url: "https://www.parl.ca/legisinfo/en/api/bills"
  ourcommons_url: "https://api.ourcommons.ca/api/v1"
  timeout: 5
  keywords: ["China"]
statcan:
  base_url: "https://www150.statcan.gc.ca/t1/tbl1/en/dtl"
  table_id: "12-10-0011-01"
  timeout: 5
"""
    )
    return cfg_dir


def test_cli_help(runner: CliRunner) -> None:
    """Test that --help works."""
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "China Compass" in result.output


def test_run_help(runner: CliRunner) -> None:
    """Test that run --help shows options."""
    result = runner.invoke(main, ["run", "--help"])
    assert result.exit_code == 0
    assert "--source" in result.output
    assert "--env" in result.output
    assert "--date" in result.output
    assert "--output-dir" in result.output


@patch("fetcher.cli.run_source")
@patch("fetcher.cli.load_config")
def test_run_single_source(
    mock_load_config: AsyncMock,
    mock_run_source: AsyncMock,
    runner: CliRunner,
    tmp_path: Path,
    dev_app_config: object,
) -> None:
    """Test running a single source."""
    mock_load_config.return_value = dev_app_config
    mock_run_source.return_value = {"bills": [], "hansard_stats": {"total_mentions": 0}}

    output_dir = str(tmp_path / "output")
    result = runner.invoke(
        main,
        ["run", "--source", "parliament", "--date", "2025-01-17", "--output-dir", output_dir],
    )

    # Should have called run_source once
    assert mock_run_source.call_count == 1
    assert result.exit_code == 0


@patch("fetcher.cli.run_source")
@patch("fetcher.cli.load_config")
def test_run_all_sources(
    mock_load_config: AsyncMock,
    mock_run_source: AsyncMock,
    runner: CliRunner,
    tmp_path: Path,
    dev_app_config: object,
) -> None:
    """Test running all sources."""
    mock_load_config.return_value = dev_app_config
    mock_run_source.return_value = {"data": "test"}

    output_dir = str(tmp_path / "output")
    result = runner.invoke(
        main,
        ["run", "--date", "2025-01-17", "--output-dir", output_dir],
    )

    # Should have called run_source for each registered source
    assert mock_run_source.call_count == 6
    assert result.exit_code == 0


@patch("fetcher.cli.run_source")
@patch("fetcher.cli.load_config")
def test_run_handles_source_failure(
    mock_load_config: AsyncMock,
    mock_run_source: AsyncMock,
    runner: CliRunner,
    tmp_path: Path,
    dev_app_config: object,
) -> None:
    """Test that one source failing doesn't stop others and exits with code 1."""
    mock_load_config.return_value = dev_app_config

    call_count = 0

    async def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("Network error")
        return {"data": "ok"}

    mock_run_source.side_effect = side_effect

    output_dir = str(tmp_path / "output")
    result = runner.invoke(
        main,
        ["run", "--date", "2025-01-17", "--output-dir", output_dir],
    )

    # All sources should have been attempted
    assert mock_run_source.call_count == 6
    # Should exit with 1 because one source failed
    assert result.exit_code == 1


def test_run_invalid_env(runner: CliRunner) -> None:
    """Test that an invalid --env value is rejected."""
    result = runner.invoke(main, ["run", "--env", "invalid"])
    assert result.exit_code != 0


def test_write_raw_creates_file(tmp_output_dir: Path) -> None:
    """Test that write_raw creates the expected JSON file."""
    data = {"test_key": "test_value", "items": [1, 2, 3]}
    out_path = write_raw("2025-01-17", "test_source", data, str(tmp_output_dir))

    assert out_path.exists()
    assert out_path.name == "test_source.json"

    with open(out_path) as f:
        written = json.load(f)

    assert written["metadata"]["source_name"] == "test_source"
    assert written["metadata"]["date"] == "2025-01-17"
    assert written["metadata"]["version"] is not None
    assert written["data"] == data


def test_write_raw_creates_directories(tmp_path: Path) -> None:
    """Test that write_raw creates the date directory if needed."""
    output_dir = str(tmp_path / "nonexistent" / "path")
    data = {"key": "value"}
    out_path = write_raw("2025-01-17", "source", data, output_dir)

    assert out_path.exists()
    assert "2025-01-17" in str(out_path)


def test_write_raw_overwrites_existing(tmp_output_dir: Path) -> None:
    """Test that write_raw overwrites an existing file."""
    write_raw("2025-01-17", "source", {"version": 1}, str(tmp_output_dir))
    out_path = write_raw("2025-01-17", "source", {"version": 2}, str(tmp_output_dir))

    with open(out_path) as f:
        written = json.load(f)

    assert written["data"]["version"] == 2
