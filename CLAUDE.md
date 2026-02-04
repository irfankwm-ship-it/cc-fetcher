# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**cc-fetcher** is the raw data ingestion layer for the China Compass pipeline. It collects data from 9 external sources (including Chinese-language sources from mainland China, Taiwan, and Hong Kong) and persists them as structured JSON in `cc-data/raw/{date}/{source}.json`. This is the first stage of the pipeline — only raw data collection and light parsing, no analysis.

The fetcher runs in a **secure Docker container** with data validation and CDR (Content Disarm & Reconstruction) cleaning before output reaches storage.

## Architecture

Plugin-based source system with fail-in-isolation design (one source failure doesn't block others):

1. **CLI** (Click) → parses args and dispatches
2. **Config Loader** → env-aware YAML config → frozen dataclasses
3. **Source Registry** → dynamic imports via `importlib.import_module()`
4. **Output Writer** → JSON envelope with metadata

Each source module exposes a single async `fetch(config, date) -> dict` entry point. Sources are registered in `fetcher/sources/__init__.py` mapping names to dotted module paths.

### Sources

| Source | Module | Reliability | Notes |
|--------|--------|-------------|-------|
| parliament | `sources/parliament.py` | Stable | LEGISinfo + OurCommons APIs, concurrent bill fetches via `asyncio.gather()` |
| statcan | `sources/statcan.py` | Stable | Statistics Canada WDS REST API, batched POST |
| yahoo_finance | `sources/yahoo_finance.py` | Medium | yfinance (synchronous, wrapped with `asyncio.to_thread()`) |
| news | `sources/news_scraper.py` | Low | RSS feeds with keyword filtering, 0.75 dedup similarity threshold, semaphore of 10 concurrent |
| xinhua | `sources/xinhua.py` | Low | HTML scraper — fragile to layout changes |
| global_affairs | `sources/global_affairs.py` | Low | canada.ca API, bilingual (EN+FR), 7-day recency window |
| mfa | `sources/mfa.py` | Medium | MFA press conference scraper (fmprc.gov.cn), fetches ALL articles from last 24h |
| mofcom | `sources/mofcom.py` | Medium | MOFCOM trade policy scraper (mofcom.gov.cn), fetches ALL articles from last 24h |
| chinese_news | `sources/chinese_news.py` | Medium | Chinese-language RSS feeds from mainland (新华社, 人民日报), Taiwan (自由時報), Hong Kong (香港電台). Tags with `"language": "zh"` and `"region": "mainland/taiwan/hongkong"` |

### Output Format

All sources write a JSON envelope:
```json
{
  "metadata": { "fetch_timestamp": "...", "source_name": "...", "version": "0.1.0", "date": "..." },
  "data": { ... }
}
```
Files go to `{output_dir}/{date}/{source}.json` with UTF-8, `ensure_ascii=False`, 2-space indent.

## Security Architecture

The fetcher uses a multi-stage security pipeline to prevent malicious content from reaching storage:

### Docker Containers

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│    FETCHER      │    │    CLEANER      │    │     MOVER       │
│  (has internet) │───►│  (air-gapped)   │───►│  (no network)   │
│                 │    │                 │    │                 │
│ - fetch data    │    │ - validate      │    │ - copy to       │
│ - parse         │    │ - CDR clean     │    │   final storage │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

- **Fetcher**: Read-only container, non-root user, drops all capabilities
- **Cleaner**: Air-gapped (no internet), runs CDR reconstruction
- **Mover**: No network at all, just copies validated files

### Content Disarm & Reconstruction (CDR)

The cleaner (`scripts/cdr_cleaner.py`) doesn't filter data — it **rebuilds** it:
1. Parse JSON and extract only expected fields
2. Sanitize all strings (strip HTML, control chars)
3. Validate URLs (only http/https, no javascript:)
4. Reconstruct a brand-new clean JSON file

### Validation

`scripts/validate_output.py` checks for malicious patterns:
- Script injection (`<script>`, event handlers)
- Dangerous URL protocols
- Excessive nesting or file sizes

### Local Development

```bash
docker compose up --build          # full pipeline
docker compose run fetcher         # fetch only
docker compose run cleaner         # clean only
```

## Build & Development Commands

```bash
poetry install                                    # install deps
poetry run pytest                                 # all tests
poetry run pytest tests/test_statcan.py -v        # single test file
poetry run ruff check src/ tests/                 # lint
poetry run fetcher run --env dev                  # all sources
poetry run fetcher run --source parliament        # single source
poetry run fetcher run --date 2025-01-15 --env prod  # specific date
```

## CLI Reference

```
fetcher run [--source NAME] [--env dev|staging|prod] [--date YYYY-MM-DD] [--output-dir PATH]
```

Exit codes: 0 = all sources succeeded, 1 = one or more failed or config missing.

## Configuration

Config files: `config/sources.{env}.yaml` (dev/staging/prod). Environment resolved via: `--env` flag → `CC_ENV` env var → default `dev`.

YAML values matching `${VAR_NAME}` are resolved to environment variables at load time.

Config dataclasses are all frozen/immutable: `AppConfig` → `SourceConfig` → `RetryConfig`.

Environment differences: dev uses shorter timeouts (30s) and fewer retries (3); prod uses longer timeouts (60-120s) and more retries (3-5).

## Testing Patterns

- **pytest** with **pytest-asyncio** (`asyncio_mode = "auto"`)
- **respx** for HTTP mocking at transport layer
- **Click.testing.CliRunner** for CLI integration tests
- Fixtures in `tests/fixtures/` (JSON responses, HTML snapshots, RSS feeds)
- Key fixtures in `conftest.py`: `_load_json_fixture()`, `_load_text_fixture()`, `dev_source_config`, `tmp_output_dir`

## Key Conventions

- All source `fetch()` functions are async; yfinance is wrapped with `asyncio.to_thread()`
- HTTP retry logic in `fetcher/http.py`: retries on 429/500/502/503/504 with exponential backoff
- Ruff config: line-length 100, target Python 3.12, rules E/F/I/N/W/UP
- Non-serializable values converted via `default=str` in JSON output
- Chinese-language sources tag articles with `"language": "zh"` and `"region"` for downstream processing
- Chinese keyword filtering uses exact substring match (no word boundaries) with both Simplified and Traditional variants
- MFA/MOFCOM fetch ALL articles from last 24h (date-based filtering, no keyword filtering)
- Government sources (MFA, MOFCOM) use URL date patterns or JavaScript timestamps for 24h filtering
