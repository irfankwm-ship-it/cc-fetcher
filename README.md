# cc-fetcher

Fetches raw data from various APIs and web sources for the China Compass pipeline.

## Sources

- **Parliament** - LEGISinfo and OurCommons API for Canadian parliamentary data
- **StatCan** - Statistics Canada bilateral trade data
- **Yahoo Finance** - Chinese market indices via yfinance
- **News Scraper** - RSS feeds with keyword filtering
- **Xinhua** - Xinhua state media scraper
- **Global Affairs** - Global Affairs Canada news releases

## Usage

```bash
# Run all fetchers for today
fetcher run

# Run a single source
fetcher run --source parliament

# Specify environment and date
fetcher run --env staging --date 2025-01-15

# Custom output directory
fetcher run --output-dir ./data/raw
```

## Development

```bash
poetry install
poetry run pytest
poetry run ruff check src/ tests/
```

## Configuration

Config files live in `config/sources.{env}.yaml`. The environment is selected via:
1. `--env` CLI flag
2. `CC_ENV` environment variable
3. Defaults to `dev`
