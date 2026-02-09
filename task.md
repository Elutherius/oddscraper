# Agent Prompt: Polymarket Universe Daily Pricing Snapshot to CSV (Python)

You are a senior Python engineer. Build a small, reliable Python project that I can run once per day to fetch Polymarket "universe mode" market metadata + latest pricing and output CSVs I can download and share. Use ONLY official Polymarket APIs, do NOT scrape HTML, do NOT use on-chain RPC.

## Objective
When I run one command, the program will:
1) Fetch ALL markets from Polymarket Gamma API (universe).
2) Extract all CLOB token IDs for outcomes.
3) Fetch latest available prices for every token using the Polymarket CLOB pricing API in bulk.
4) Write clean CSV outputs to a local `data/` folder.
5) Print a short run summary and where the CSVs were written.

This is not live. It is a daily snapshot. Keep the design simple, debuggable, and robust against partial failures.

## Official Endpoints
- Gamma base: `https://gamma-api.polymarket.com`
- CLOB base: `https://clob.polymarket.com`

Use these docs as source of truth:
- Gamma endpoints overview: https://docs.polymarket.com/quickstart/reference/endpoints
- Gamma list markets (pagination): https://docs.polymarket.com/api-reference/markets/list-markets
- Gamma markets structure fields: https://docs.polymarket.com/developers/gamma-markets-api/gamma-structure
- CLOB bulk prices: https://docs.polymarket.com/api-reference/pricing/get-multiple-market-prices-by-request
- Polymarket rate limits: https://docs.polymarket.com/quickstart/introduction/rate-limits

## CLI
Implement a CLI entrypoint with:
- `python -m pm_universe fetch`

Optional flags (nice-to-have, not required):
- `--date YYYY-MM-DD` (default: today's date in UTC)
- `--outdir data`
- `--max-markets N` (debug mode)
- `--concurrency N` (default conservative)
- `--batch-size N` (default 500 request items max for /prices)

## Outputs (must exist after each run)
Create these files:
1) `data/markets/markets_YYYY-MM-DD.csv`
2) `data/prices/prices_YYYY-MM-DD.csv`
3) `data/prices/latest.csv` (copy/overwrite with today’s prices file)
4) `data/run/run_manifest_YYYY-MM-DD.json`
Also store raw API responses for debugging:
- `data/raw/gamma/markets_YYYY-MM-DD.json`
- `data/raw/clob/prices_batches/markets_YYYY-MM-DD/batch_0001.json` (one per batch)

### CSV: markets_YYYY-MM-DD.csv (one row per market)
Include at minimum these columns (leave blank if not present):
- `pulled_at_utc`
- `source` (constant: `polymarket_gamma`)
- `market_id`
- `slug`
- `question`
- `condition_id`
- `active` (boolean if available)
- `closed` (boolean if available)
- `end_date_utc` (if present)
- `outcomes_json` (stringified JSON list)
- `clob_token_ids_json` (stringified JSON list)
- `volume_num` (if present)
- `liquidity_num` (if present)

### CSV: prices_YYYY-MM-DD.csv (one row per token outcome)
Include at minimum these columns:
- `snapshot_ts_utc`
- `source` (constant: `polymarket_clob`)
- `market_id`
- `slug`
- `question`
- `token_id`
- `outcome` (aligned to the token_id via Gamma)
- `bid` (derived from /prices side SELL)
- `ask` (derived from /prices side BUY)
- `mid` (computed when both bid and ask exist; else blank)
- `active` (from Gamma if present)
- `status` (`ok`, `missing_price`, `api_error`)
- `volume_num` (from Gamma if present)
- `liquidity_num` (from Gamma if present)

Notes:
- Interpret /prices request items as `{ token_id, side }`. Use side "BUY" and "SELL" for each token.
- Bid/ask mapping for CSV:
  - `ask` = price from side "BUY"
  - `bid` = price from side "SELL"
This gives a consistent snapshot even if spreads exist.

## Design Requirements
### Project structure
Create:
- `pm_universe/`
  - `__init__.py`
  - `cli.py` (arg parsing, calls runner)
  - `runner.py` (orchestrates full flow)
  - `gamma.py` (Gamma client + pagination)
  - `clob.py` (CLOB pricing client + batching)
  - `models.py` (typed dicts/dataclasses for internal records)
  - `io_store.py` (write raw JSON, write CSV, write manifest)
  - `utils.py` (time, chunking, safe json stringify)
- `pyproject.toml` (preferred) or `requirements.txt`
- `README.md`

### HTTP + reliability
Use `httpx` (sync is fine for MVP).
- Set timeouts (connect and read).
- Implement retries with exponential backoff on:
  - 429
  - 5xx
- Add a simple rate limiter per host to be polite, aligned with published rate limits.
- Log one line per request: endpoint, page/batch, status_code, latency_ms, bytes.

### Pagination for Gamma
- Use `limit` and `offset` as described in docs.
- Continue until returned list is empty.
- Include a safety `max_pages` to avoid infinite loops.

### Batching for CLOB /prices
- Build a request list of `{token_id, side}` for all tokens:
  - [x] Create a prototype script to fetch odds <!-- id: 1 -->
- [x] Verify the data fetching works <!-- id: 2 -->
- [x] Parse scraped JSON to structured CSV <!-- id: 3 -->
- [x] Expand scraping to multiple sports (NHL, NFL, etc.) <!-- id: 4 -->
- [x] Organize output into folders <!-- id: 5 -->
- [x] Create unified runner script <!-- id: 6 -->
- [x] Explore PointsBet Canada website for API/scraping <!-- id: 7 -->
- [x] Create script to fetch PointsBet odds <!-- id: 8 -->
- [x] Create script to fetch PointsBet odds <!-- id: 8 -->
- [x] Parse PointsBet data to CSV <!-- id: 9 -->
- [x] Add vig breakdown by category to app <!-- id: 10 -->docs).
- Chunk into batches of <= 500 request items per API call (per docs).
- Fetch with modest concurrency (default 5) so the run finishes in reasonable time without hitting rate limits.

### Alignment of token IDs to outcomes
- From Gamma market records, outcomes and clobTokenIds are lists that correspond by index. Use that to map:
  - token_id -> outcome label
- Persist this mapping implicitly by writing `token_id` and `outcome` in the prices CSV.

### Partial failures
Universe mode means some data will be missing.
- If a price is missing for a token, write the row with `status=missing_price`.
- Do not fail the entire run unless Gamma market fetch fails completely.
- The run summary should include counts: markets, tokens, priced_ok, missing_price, api_errors.

### Run manifest
Write `data/run/run_manifest_YYYY-MM-DD.json` with:
- start/end timestamps, duration
- number of markets fetched
- number of tokens discovered
- number of /prices batches
- counts: ok/missing/api_error
- output file paths

## User Experience
After `python -m pm_universe fetch`:
- The program prints:
  - where the prices CSV is
  - where latest.csv is
  - counts and any top-level warnings
- The CSVs are ready for me to upload/share or place in a shared folder.

## Deliverable
Return the full code for the project, plus README with:
- install steps (venv)
- run command
- what files are produced
- troubleshooting tips (rate limits, missing prices)

Keep it minimal, clean, and easy to extend later (but do not add analytics/matching).
