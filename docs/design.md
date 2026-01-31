# cc-fetcher Design Document

## 1. Overview

cc-fetcher is the raw data ingestion layer of the China Compass pipeline. Its sole
responsibility is to pull data from a set of external APIs, RSS feeds, and web pages
and persist it as structured JSON in the `cc-data/raw/` directory. Downstream
components (normalization, analysis, rendering) consume these raw files and are
completely decoupled from the fetching logic.

The tool is invoked daily -- either manually or via a scheduler -- to produce one
JSON file per source per date. It does not transform, aggregate, or analyze the data
it collects; it writes what it receives, wrapped in a thin metadata envelope.

### Role in the pipeline

```
                                cc-fetcher (this project)
                                     |
               fetches from 6 external sources
                                     |
                                     v
                     cc-data/raw/{YYYY-MM-DD}/{source}.json
                                     |
                          (downstream pipeline stages)
                                     v
                          normalization -> analysis -> site
```

### Key design principles

- **One async function per source.** Every source module exposes a single
  `async def fetch(config, date) -> dict` entry point. Nothing else is required.
- **Fail in isolation.** A failing source never blocks other sources from running.
- **Raw-only output.** The fetcher writes data as-received (with light parsing where
  the raw format is HTML or XML). No scoring, ranking, or summarization happens here.
- **Environment-aware configuration.** Timeouts, retry counts, keyword lists, and
  feed URLs all vary by environment (dev / staging / prod) via per-env YAML files.

---

## 2. Architecture

### Component diagram

```
CLI (click)
  |
  v
Config Loader (YAML, env-aware)
  |
  v
Source Registry (dynamic import via importlib)
  |
  +---> parliament.py      LEGISinfo + OurCommons APIs
  +---> statcan.py          Statistics Canada Web Data Service
  +---> yahoo_finance.py    yfinance library (Chinese market indices)
  +---> news_scraper.py     RSS feeds with keyword filtering
  +---> xinhua.py           Xinhua English HTML scraping
  +---> global_affairs.py   Global Affairs Canada HTML scraping
  |
  v
Output Writer (JSON envelope with metadata)
  |
  v
cc-data/raw/{date}/{source}.json
```

### Source plugin model

Source modules are registered in `fetcher/sources/__init__.py` via a flat dictionary
called `SOURCE_REGISTRY`:

```python
SOURCE_REGISTRY: dict[str, str] = {
    "parliament": "fetcher.sources.parliament",
    "statcan": "fetcher.sources.statcan",
    "yahoo_finance": "fetcher.sources.yahoo_finance",
    "news": "fetcher.sources.news_scraper",
    "xinhua": "fetcher.sources.xinhua",
    "global_affairs": "fetcher.sources.global_affairs",
}
```

Each value is a dotted module path. At runtime, `run_source()` uses `importlib.import_module()`
to lazily load the module and call its `fetch()` coroutine. This means source modules
are only imported when they are actually invoked, keeping startup fast and avoiding
import-time side effects from unused sources.

### CLI entry point

The CLI is built with Click and is registered as a console script named `fetcher` in
`pyproject.toml`. The top-level group exposes a single `run` subcommand:

```
fetcher run                          # all sources, today, dev env
fetcher run --source parliament      # single source
fetcher run --env prod               # production config
fetcher run --date 2025-01-15        # historical date
fetcher run --output-dir ./data      # custom output path
```

The `run` command resolves its defaults (date defaults to today, output directory
defaults to `../cc-data/raw/`, environment defaults to dev or `CC_ENV`), loads the
YAML config, and iterates through each source sequentially. Results are collected
and a summary of successes and failures is printed at exit.

### Config system

Configuration is loaded from `config/sources.{env}.yaml` by `fetcher.config.load_config()`.
The loader:

1. Detects the environment from CLI flag, `CC_ENV`, or the default `"dev"`.
2. Reads the corresponding YAML file.
3. Recursively resolves `${VAR}` references to environment variables.
4. Builds frozen dataclass instances (`AppConfig`, `SourceConfig`, `RetryConfig`).

All config objects are immutable (`frozen=True` dataclasses), preventing accidental
mutation during a run.

---

## 3. Data Flow

The end-to-end data flow for a single invocation:

1. **CLI parsing.** Click parses `--source`, `--env`, `--date`, `--output-dir`.
   Missing values are filled from defaults (today's date, dev environment,
   `../cc-data/raw/` output directory).

2. **Config loading.** `load_config(env)` reads the YAML file for the resolved
   environment, resolves any `${ENV_VAR}` references, and returns an `AppConfig`
   containing a `SourceConfig` for each source.

3. **Source iteration.** The CLI loops through either the single requested source
   or all keys in `SOURCE_REGISTRY`. For each source:

   a. The source's `SourceConfig` is looked up from `AppConfig`. If absent, a
      default `SourceConfig` is used with the source name and default timeout/retry.

   b. `run_source(name, config, date)` dynamically imports the module and calls
      `await module.fetch(config, date)`.

   c. The source module makes HTTP requests (via `httpx.AsyncClient` or `yfinance`)
      to external services, parses responses, and returns a Python dictionary.

   d. `write_raw(date, source, data, output_dir)` wraps the dictionary in a
      metadata envelope and writes it to
      `{output_dir}/{date}/{source}.json`.

4. **Error capture.** If a source raises any exception, the error is logged and
   recorded in the results list. Execution continues with the next source.

5. **Exit.** The CLI prints a summary (`N succeeded, M failed`) and exits with
   code 0 if all sources succeeded, code 1 otherwise.

```
External APIs / Websites
        |
        | HTTP (httpx) or library call (yfinance)
        v
Source Module (parse, filter, structure)
        |
        | Python dict
        v
Output Writer (envelope + JSON serialization)
        |
        | File I/O
        v
cc-data/raw/2025-01-17/parliament.json
cc-data/raw/2025-01-17/statcan.json
cc-data/raw/2025-01-17/yahoo_finance.json
cc-data/raw/2025-01-17/news.json
cc-data/raw/2025-01-17/xinhua.json
cc-data/raw/2025-01-17/global_affairs.json
```

---

## 4. Source Modules

### 4.1 parliament -- Canadian Parliamentary Data

**Module:** `fetcher/sources/parliament.py`

**External services:**
- LEGISinfo API (`https://www.parl.ca/legisinfo/en/api/bills`) -- bill status tracking
- OurCommons API (`https://api.ourcommons.ca/api/v1/Debates`) -- Hansard keyword mentions

**Approach:** Makes one HTTP GET per tracked bill to LEGISinfo and one GET per keyword
to the OurCommons Debates endpoint. Both sub-fetches run concurrently via
`asyncio.gather()`.

**Tracked bills:** C-27, S-7, C-34, C-70, M-62 (hardcoded in `TRACKED_BILLS`).

**Keywords:** Configurable via YAML; defaults to `["China", "Beijing", "PRC", "Huawei", "canola"]`.

**Output structure:**
```json
{
  "date": "2025-01-17",
  "bills": [
    {
      "id": "C-27",
      "title": "Digital Charter Implementation Act, 2022",
      "status": "Senate - Second Reading",
      "last_action": "Passed House of Commons",
      "parliament_session": "44-1",
      "sponsor": "Hon. Francois-Philippe Champagne"
    }
  ],
  "hansard_stats": {
    "by_keyword": {"China": 47, "Beijing": 12, ...},
    "total_mentions": 120
  },
  "tracked_bills": ["C-27", "S-7", "C-34", "C-70", "M-62"],
  "keywords": ["China", "Beijing", "PRC", "Huawei", "canola"]
}
```

**Error handling:** Individual bill or keyword failures are logged as warnings and
skipped. The overall fetch still succeeds with partial data. Failed Hansard keywords
record a count of 0.

**Rate limiting:** Sequential per-bill and per-keyword requests within the source.
No explicit throttle; relies on the per-source timeout.

---

### 4.2 statcan -- Statistics Canada Bilateral Trade Data

**Module:** `fetcher/sources/statcan.py`

**External service:** Statistics Canada Web Data Service
(`https://www150.statcan.gc.ca/t1/tbl1/en/dtl`)

**Approach:** Single HTTP GET with query parameters for the table ID
(`12-10-0011-01`), country code (`CHN`), and a comma-separated list of HS commodity
codes.

**Tracked HS codes:**

| Code | Commodity |
|------|-----------|
| 1205 | Canola / rapeseed |
| 4407 | Lumber (wood sawn) |
| 2709 | Crude petroleum |
| 2710 | Petroleum products |
| 2711 | Natural gas |
| 8471 | Machinery / computers |
| 8542 | Electronic integrated circuits |
| 2601 | Iron ores and concentrates |

**Output structure:**
```json
{
  "date": "2025-01-17",
  "country": "China",
  "country_code": "CHN",
  "table_id": "12-10-0011-01",
  "commodities": [
    {
      "hs_code": "1205",
      "commodity_name": "Canola / rapeseed",
      "exports_cad": 5234000000,
      "imports_cad": 12000000,
      "trade_balance_cad": 5222000000,
      "exports_yoy_pct": 9.04,
      "imports_yoy_pct": 9.09
    }
  ],
  "totals": {
    "total_exports_cad": 7169000000,
    "total_imports_cad": 12357000000,
    "trade_balance_cad": -5188000000
  }
}
```

**Error handling:** HTTP errors and request failures return a dict with an `error`
field and empty `commodities` / `totals` -- the fetch does not raise.

**Rate limiting:** Single request per invocation. Prod timeout set to 120s due to
occasional slow responses from the StatCan API.

---

### 4.3 yahoo_finance -- Chinese Market Indices

**Module:** `fetcher/sources/yahoo_finance.py`

**External service:** Yahoo Finance (via the `yfinance` Python library).

**Approach:** Uses `yfinance.Ticker.history()` to fetch recent close prices for each
configured index. The call is synchronous (yfinance does not support async), so
tickers are fetched in a serial loop within the async wrapper.

**Default indices:**

| Ticker | Name |
|--------|------|
| 000001.SS | Shanghai Composite |
| 399001.SZ | Shenzhen Component |
| ^HSI | Hang Seng |
| 000300.SS | CSI 300 |

**Features:**
- Fetches the last 5 trading days of close prices for sparkline rendering.
- Computes daily percent change from the previous close.
- Detects market holidays by comparing the latest available date against the target date.

**Output structure:**
```json
{
  "date": "2025-01-17",
  "indices": [
    {
      "ticker": "000001.SS",
      "name": "Shanghai Composite",
      "value": 3240.61,
      "change_pct": -0.38,
      "prev_close": 3252.98,
      "sparkline": [3261.50, 3258.10, 3252.98, 3248.00, 3240.61],
      "latest_date": "2025-01-17",
      "market_holiday": false
    }
  ],
  "summary": {
    "indices_fetched": 4,
    "indices_failed": 0,
    "all_markets_closed": false
  }
}
```

**Error handling:** Each ticker is fetched independently. A failure for one index
produces a record with `null` value/change and an `error` field, while other indices
proceed normally.

**Rate limiting:** No explicit rate limiting. Yahoo Finance imposes its own
throttling; the library handles this internally. A buffer of 5 extra days is
requested to account for market holidays.

---

### 4.4 news -- RSS Feed Scraper

**Module:** `fetcher/sources/news_scraper.py`

**External services:** Configurable RSS feeds. Production config includes:
- Reuters World News
- CBC World
- Globe and Mail World
- South China Morning Post
- AP International News

**Approach:** For each feed, an HTTP GET fetches the raw XML. The `feedparser`
library parses it into article entries. Articles are then:

1. **Keyword-filtered:** Only articles whose title or body snippet contain at
   least one configured keyword (case-insensitive) are retained.
2. **Deduplicated:** `SequenceMatcher` with a 0.75 similarity threshold removes
   near-duplicate titles across feeds.
3. **Classified:** Each article is tagged with one or more categories based on
   bilingual (English/French) keyword sets.

**Category taxonomy:**

| Category | Example keywords |
|----------|-----------------|
| diplomatic | ambassador, embassy, ambassadeur |
| trade | tariff, export, canola, commerce |
| military | defense, navy, NORAD, NATO |
| technology | Huawei, 5G, semiconductor, cybersecurite |
| political | parliament, election, Xi Jinping |
| economic | GDP, investment, yuan, PIB |
| social | Uyghur, Hong Kong, human rights |
| legal | sanctions, espionage, extradition |
| general | (fallback when no category matches) |

**Output structure:**
```json
{
  "date": "2025-01-17",
  "articles": [
    {
      "title": "Canada reviews China trade policy",
      "source": "Reuters",
      "date": "Fri, 17 Jan 2025 10:00:00 GMT",
      "body_snippet": "The Canadian government announced...",
      "url": "https://reuters.com/...",
      "categories": ["trade", "political"]
    }
  ],
  "total_articles": 12,
  "feeds_checked": 5,
  "feed_errors": [],
  "keywords_used": ["China", "Beijing", "Canada-China", "PRC"]
}
```

**Error handling:** Feed-level errors (HTTP failures, timeouts) are captured in the
`feed_errors` array. Other feeds continue processing.

**Rate limiting:** Feeds are fetched sequentially within a single `httpx.AsyncClient`
session. Per-source timeout applies to each individual feed request.

---

### 4.5 xinhua -- Xinhua State Media

**Module:** `fetcher/sources/xinhua.py`

**External service:** Xinhua English web portal (`http://www.xinhuanet.com/english/`).

**Approach:** Fetches the main page HTML with `httpx` (following redirects) and
parses it with BeautifulSoup. The parser tries multiple CSS selectors
(`div.news_item`, `div.tit`, `li.clearfix`, `article`, etc.) to locate article
elements. If none match, it falls back to finding `<a>` tags whose `href` matches
a date-like URL pattern (`/YYYY-MM/DD/`).

Extracted articles are then filtered through two keyword lists:
- **Canada keywords:** Canada, Canadian, Ottawa, Trudeau, canola, Huawei, Meng
  Wanzhou, Arctic, NORAD
- **Policy keywords:** Belt and Road, BRI, Xi Jinping, State Council, BRICS, SCO,
  RCEP, trade war, sanctions

Matching articles receive `relevance_tags` indicating the match type (e.g.,
`"canada:Trudeau"`, `"policy:Belt and Road"`).

**Output structure:**
```json
{
  "date": "2025-01-17",
  "articles": [
    {
      "title": "Xi meets Canadian delegation",
      "body": "Chinese President Xi Jinping...",
      "date": "2025-01-17",
      "source_url": "http://www.xinhuanet.com/english/...",
      "source": "Xinhua",
      "relevance_tags": ["canada:Canadian", "policy:Xi Jinping"]
    }
  ],
  "total_scraped": 85,
  "total_relevant": 3,
  "source_url": "http://www.xinhuanet.com/english/"
}
```

**Error handling:** HTTP or connection errors return a dict with an `error` field
and an empty `articles` list.

**Rate limiting:** Single page fetch per invocation. Prod config sets the retry
count to 5 with a backoff factor of 2.0 due to occasional connectivity issues with
Chinese servers.

**Fragility note:** This is an HTML scraper. It will break if Xinhua changes its
page structure. The multi-selector fallback strategy provides some resilience, but
this source should be considered Tier 3 (see reliability tiers below).

---

### 4.6 global_affairs -- Global Affairs Canada

**Module:** `fetcher/sources/global_affairs.py`

**External service:** Global Affairs Canada news page
(`https://www.international.gc.ca/news-nouvelles/`).

**Approach:** Similar to the Xinhua scraper. Fetches the page HTML, parses with
BeautifulSoup using Canada.ca-specific selectors (`article`, `li.item`,
`section.gc-nws li`, `div.views-row`, etc.), and falls back to finding links
containing `news`, `nouvelles`, or `statements` in the main content area.

Articles are filtered against a bilingual keyword list (English and French):
China, Chinese, Beijing, PRC, Chine, chinois, Pekin, RPC, Hong Kong, Taiwan,
Xinjiang, Tibet, Indo-Pacific, Indo-Pacifique.

Each article is classified by content type (statement, press release, travel
advisory, news release, declaration, communique, avis aux voyageurs).

**Output structure:**
```json
{
  "date": "2025-01-17",
  "articles": [
    {
      "title": "Statement on Indo-Pacific trade",
      "body_snippet": "The Minister of Foreign Affairs...",
      "date": "2025-01-17",
      "source_url": "https://www.international.gc.ca/...",
      "source": "Global Affairs Canada",
      "content_type": "statement",
      "matched_keywords": ["Indo-Pacific"]
    }
  ],
  "total_scraped": 40,
  "total_relevant": 2,
  "source_url": "https://www.international.gc.ca/news-nouvelles/"
}
```

**Error handling:** Same pattern as Xinhua -- HTTP errors produce a dict with `error`
and empty `articles`.

**Rate limiting:** Single page fetch. Prod retries set to 5 with backoff factor 2.0.

### Source reliability tiers

| Tier | Source | Stability | Notes |
|------|--------|-----------|-------|
| 1 -- Stable APIs | parliament | High | Government APIs with documented endpoints |
| 1 -- Stable APIs | statcan | High | Official statistical data service |
| 2 -- Library-wrapped | yahoo_finance | Medium | Depends on yfinance and Yahoo's undocumented API |
| 3 -- Web scraping | news (RSS) | Medium | RSS feeds are semi-stable but URLs change |
| 3 -- Web scraping | xinhua | Low | HTML scraping; fragile to layout changes |
| 3 -- Web scraping | global_affairs | Low | HTML scraping; Canada.ca templates evolve |

---

## 5. Configuration

### Environment system

Three environments with progressively more aggressive settings:

| Aspect | dev | staging | prod |
|--------|-----|---------|------|
| Timeouts | 30s | 45s | 60-120s |
| Retry count | defaults (3) | 2-3 | 3-5 |
| Backoff factor | default (0.5) | 0.5-1.0 | 1.0-2.0 |
| RSS feeds | 2 (Reuters, CBC) | 4 (+ Globe, SCMP) | 5 (+ AP) |
| Keywords | minimal set | extended set | full set |

Environment resolution priority:
1. `--env` CLI flag
2. `CC_ENV` environment variable
3. Default: `dev`

### YAML config structure

Config files live at `config/sources.{env}.yaml`. Each top-level key is a source
name; its value is a flat dictionary of settings plus optional `timeout` and `retry`
blocks.

```yaml
parliament:
  legisinfo_url: "https://www.parl.ca/legisinfo/en/api/bills"
  ourcommons_url: "https://api.ourcommons.ca/api/v1"
  timeout: 60
  keywords: ["China", "Beijing", "PRC", "Huawei", "canola"]
  retry:
    max_retries: 5
    backoff_factor: 1.5

statcan:
  base_url: "https://www150.statcan.gc.ca/t1/tbl1/en/dtl"
  table_id: "12-10-0011-01"
  timeout: 120
  retry:
    max_retries: 5
    backoff_factor: 2.0
```

The `timeout` and `retry` keys are extracted during config parsing and stored as
typed fields on `SourceConfig`. All other keys are placed into the `settings` dict
and accessed by source modules via `config.get(key, default)`.

### Environment variable resolution

Any YAML value matching the pattern `${VAR_NAME}` is resolved to the value of the
corresponding environment variable at load time. This is used for API keys or
secrets that must not be committed to version control:

```yaml
some_source:
  api_key: "${MY_API_KEY}"
```

Resolution is recursive -- it applies to values nested inside dicts and lists.
If the environment variable is not set, the value resolves to an empty string.

### Config dataclasses

```python
@dataclass(frozen=True)
class RetryConfig:
    max_retries: int = 3
    backoff_factor: float = 0.5
    retry_statuses: tuple[int, ...] = (429, 500, 502, 503, 504)

@dataclass(frozen=True)
class SourceConfig:
    name: str
    settings: dict[str, Any]      # arbitrary source-specific settings
    timeout: int = 30             # seconds
    retry: RetryConfig = RetryConfig()

@dataclass(frozen=True)
class AppConfig:
    env: str
    sources: dict[str, SourceConfig]
```

All three are frozen to prevent accidental mutation during a run.

---

## 6. Output Format

### Raw JSON envelope

Every output file follows the same envelope structure:

```json
{
  "metadata": {
    "fetch_timestamp": "2025-01-17T14:30:00.123456+00:00",
    "source_name": "parliament",
    "version": "0.1.0",
    "date": "2025-01-17"
  },
  "data": { ... }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `metadata.fetch_timestamp` | ISO 8601 string (UTC) | When the fetch completed |
| `metadata.source_name` | string | Registry key for the source |
| `metadata.version` | string | `cc-fetcher` package version at fetch time |
| `metadata.date` | string (YYYY-MM-DD) | The target date requested |
| `data` | dict or list | The raw payload from the source module |

### File path convention

```
{output_dir}/{YYYY-MM-DD}/{source_name}.json
```

Default `output_dir` is `../cc-data/raw/` relative to the project root, yielding
paths like:

```
cc-data/raw/2025-01-17/parliament.json
cc-data/raw/2025-01-17/statcan.json
cc-data/raw/2025-01-17/yahoo_finance.json
cc-data/raw/2025-01-17/news.json
cc-data/raw/2025-01-17/xinhua.json
cc-data/raw/2025-01-17/global_affairs.json
```

Directories are created automatically (`mkdir -p` equivalent). If a file already
exists for the same date and source, it is overwritten.

### Serialization details

- Encoding: UTF-8, `ensure_ascii=False` (preserves Chinese characters, French accents).
- Indentation: 2 spaces for human readability.
- Non-serializable values: The `default=str` fallback converts datetime objects and
  other non-JSON types to strings.

---

## 7. Extension Points -- Adding a New Source

Adding a new data source requires three steps:

### Step 1: Create the source module

Create a new file at `src/fetcher/sources/{source_name}.py` with a single async
entry point:

```python
from __future__ import annotations
from typing import Any
from fetcher.config import SourceConfig

async def fetch(config: SourceConfig, date: str) -> dict[str, Any]:
    """Fetch data from the new source.

    Args:
        config: Source configuration (access settings via config.get()).
        date: Target date as YYYY-MM-DD.

    Returns:
        Dictionary containing the fetched data.
    """
    url = config.get("url", "https://default-url.example.com")
    timeout = config.timeout

    # ... fetch logic ...

    return {
        "date": date,
        # ... source-specific data ...
    }
```

### Step 2: Register the source

Add an entry to `SOURCE_REGISTRY` in `src/fetcher/sources/__init__.py`:

```python
SOURCE_REGISTRY: dict[str, str] = {
    # ... existing sources ...
    "new_source": "fetcher.sources.new_source_module",
}
```

### Step 3: Add configuration

Add a block for the new source to each environment config file
(`config/sources.{dev,staging,prod}.yaml`):

```yaml
new_source:
  url: "https://api.example.com/data"
  timeout: 30
  retry:
    max_retries: 3
    backoff_factor: 0.5
```

### Optional: Add tests

Create `tests/test_new_source.py`. Use `respx` to mock HTTP calls and add JSON or
text fixtures to `tests/fixtures/` as needed. Follow the existing pattern of
testing the happy path, HTTP errors, timeouts, and output structure.

---

## 8. Error Handling

### Strategy

The error handling philosophy is **isolation with graceful degradation**: capture
everything, log it, keep going.

### Per-source isolation

Sources are executed sequentially in `_run_all()`. Each source call is wrapped in a
try/except that catches all exceptions:

```python
try:
    data = await run_source(name, source_config, date)
    out_path = write_raw(date, name, data, output_dir)
    return {"source": name, "status": "ok", "output": str(out_path)}
except Exception as exc:
    return {"source": name, "status": "error", "error": str(exc)}
```

A failure in source N does not prevent source N+1 from running.

### Within-source partial failures

Individual source modules handle sub-request failures internally:

- **parliament:** A failing bill or keyword request is logged and skipped; the
  remaining bills/keywords still return data.
- **statcan:** Returns an `error` field with empty `commodities` on HTTP failure.
- **yahoo_finance:** Each index is fetched independently; failed indices produce
  records with `null` values while successful ones proceed normally.
- **news:** Failed feeds are logged in `feed_errors`; other feeds still contribute
  articles.
- **xinhua / global_affairs:** HTTP errors produce a dict with `error` and empty
  `articles`.

### Timeouts

Each `httpx` request receives the source's configured `timeout` value (in seconds).
Timeouts are caught as `httpx.RequestError` and handled identically to other
request failures.

### Retry configuration

The config system supports per-source retry settings:

```python
@dataclass(frozen=True)
class RetryConfig:
    max_retries: int = 3
    backoff_factor: float = 0.5
    retry_statuses: tuple[int, ...] = (429, 500, 502, 503, 504)
```

The intended backoff formula is `delay = backoff_factor * (2 ** attempt)`.
`retry_statuses` defines which HTTP status codes should trigger a retry (rate
limiting, server errors, gateway errors).

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | All sources succeeded |
| 1 | One or more sources failed, or config file not found |

---

## 9. Testing

### Approach

Tests use **fixture-based mocking** to avoid hitting real external services. The
test suite combines:

- **respx** for mocking `httpx` HTTP calls at the transport layer.
- **JSON and text fixtures** (`tests/fixtures/`) for realistic API responses and
  HTML/XML content.
- **pytest-asyncio** for testing async `fetch()` coroutines.
- **Click's `CliRunner`** for integration-testing the CLI without subprocess spawning.
- **`unittest.mock.patch`** for isolating CLI tests from real source execution.

### Fixture files

```
tests/fixtures/
  parliament_response.json     # LEGISinfo API response
  statcan_response.json        # Statistics Canada API response
  yahoo_finance_response.json  # Yahoo Finance historical data
  rss_feed.xml                 # Sample RSS feed with China-related articles
  xinhua_page.html             # Xinhua English page HTML snapshot
  global_affairs_response.json # GAC page response
```

Fixtures are loaded via helper functions in `tests/conftest.py`:

```python
def _load_json_fixture(name: str) -> dict[str, Any]:
    path = FIXTURES_DIR / name
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def _load_text_fixture(name: str) -> str:
    path = FIXTURES_DIR / name
    with open(path, encoding="utf-8") as f:
        return f.read()
```

### Shared fixtures in conftest.py

- `dev_source_config` / `dev_app_config` -- pre-built config objects for tests.
- `tmp_output_dir` -- temporary directory that mimics `cc-data/raw/`.
- Per-source response fixtures exposed as pytest fixtures.

### Test coverage patterns

Each source test file covers:

1. **Happy path:** Mocked successful responses produce correctly structured output.
2. **HTTP errors:** 500, 503 responses are handled gracefully without raising.
3. **Timeouts:** `httpx.ConnectTimeout` is caught and reported.
4. **Output structure:** Required fields are present in every returned record.
5. **Business logic:** YoY calculation, keyword filtering, deduplication,
   category classification, trade balance computation.

CLI tests cover:
- `--help` output.
- Single-source and all-source invocation.
- Source failure isolation (one failure does not block others, exit code is 1).
- Invalid `--env` rejection.
- `write_raw` file creation, directory creation, and overwrite behavior.

### Running tests

```bash
pytest                    # run all tests
pytest tests/test_statcan.py  # run tests for a single source
pytest -v                 # verbose output
```

The project uses `asyncio_mode = "auto"` in `pyproject.toml`, so async test
functions are automatically detected without explicit markers (though markers are
still used in the existing test files for clarity).

---

## 10. Dependencies

### Runtime dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| **httpx** | ^0.27 | Async HTTP client for all API and web requests. Chosen over `aiohttp` for its cleaner API, `requests`-like interface, and built-in timeout support. |
| **feedparser** | ^6.0 | RSS/Atom feed parsing for the news scraper. The de facto standard Python library for feed parsing. |
| **beautifulsoup4** | ^4.12 | HTML parsing for Xinhua and Global Affairs Canada scrapers. Provides resilient parsing of malformed HTML with multiple parser backends. |
| **yfinance** | ^0.2 | Yahoo Finance data access for Chinese market indices. Wraps Yahoo's undocumented API and handles authentication, rate limiting, and data formatting. |
| **pyyaml** | ^6.0 | YAML config file parsing. Used with `safe_load` to avoid code execution risks. |
| **click** | ^8.1 | CLI framework. Provides argument parsing, help generation, and `Choice` validation for `--source` and `--env` flags. |

### Development dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| **pytest** | ^8.0 | Test runner and fixture framework. |
| **pytest-asyncio** | ^0.24 | Async test support for testing `fetch()` coroutines. |
| **ruff** | ^0.5 | Linter and formatter. Configured for Python 3.12 with line length 100 and lint rules E, F, I, N, W, UP. |
| **respx** | ^0.21 | HTTP mocking library for `httpx`. Intercepts requests at the transport layer, enabling precise URL and parameter matching in tests. |

### Python version

Requires Python 3.12+ (`python = "^3.12"` in `pyproject.toml`). The codebase uses
3.12 features including `type` statement compatibility and modern type hint syntax
(`dict[str, Any]`, `list[str]`, `X | None`).

### Build system

Uses Poetry (`poetry-core` backend). The package is structured with `src/` layout:

```
src/
  fetcher/
    __init__.py          # __version__ = "0.1.0"
    cli.py               # Click CLI
    config.py            # Config loader
    output.py            # JSON writer
    sources/
      __init__.py        # SOURCE_REGISTRY + run_source()
      parliament.py
      statcan.py
      yahoo_finance.py
      news_scraper.py
      xinhua.py
      global_affairs.py
```

The `fetcher` console script is defined in `pyproject.toml`:

```toml
[tool.poetry.scripts]
fetcher = "fetcher.cli:main"
```
