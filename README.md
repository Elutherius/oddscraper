# Polymarket Universe Daily Pricing Snapshot

Fetch all Polymarket markets and their latest prices, outputting clean CSVs for daily snapshots.

## Installation

```powershell
# Create virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install the package
pip install -e .
```

## Usage

### Full fetch
```powershell
python -m pm_universe fetch
```

### Smoke test (5 markets)
```powershell
python -m pm_universe fetch --max-markets 5
```

### Dry run (Gamma only, no pricing)
```powershell
python -m pm_universe fetch --dry-run --max-markets 10
```

### All options
```powershell
python -m pm_universe fetch --help
```

| Flag | Default | Description |
|------|---------|-------------|
| `--date YYYY-MM-DD` | today UTC | Date for snapshot |
| `--outdir PATH` | `data` | Output directory |
| `--max-markets N` | all | Limit markets (for testing) |
| `--concurrency N` | 5 | CLOB batch concurrency |
| `--batch-size N` | 500 | CLOB batch size |
| `--dry-run` | false | Fetch Gamma only |
| `--gamma-rate N` | 2.0 | Gamma req/s |
| `--clob-rate N` | 1.0 | CLOB req/s |
| `-v, --verbose` | false | Debug logging |

## Output Files

After a run, you'll find:

```
data/
├── markets/
│   └── markets_YYYY-MM-DD.csv      # All markets
├── prices/
│   ├── prices_YYYY-MM-DD.csv       # Token prices
│   └── latest.csv                  # Copy of today's prices
├── run/
│   └── run_manifest_YYYY-MM-DD.json # Run stats
└── raw/
    ├── gamma/
    │   └── markets_YYYY-MM-DD.json # Raw Gamma response
    └── clob/prices_batches/
        └── markets_YYYY-MM-DD/     # Raw CLOB batches
            ├── batch_0001.json
            └── ...
```

## CSV Schemas

### markets_YYYY-MM-DD.csv
| Column | Description |
|--------|-------------|
| `pulled_at_utc` | Timestamp of fetch |
| `source` | `polymarket_gamma` |
| `market_id` | Unique market ID |
| `slug` | URL slug |
| `question` | Market question |
| `condition_id` | Condition ID |
| `active` | Is market active |
| `closed` | Is market closed |
| `end_date_utc` | Market end date |
| `outcomes_json` | JSON list of outcomes |
| `clob_token_ids_json` | JSON list of token IDs |
| `volume_num` | Trading volume |
| `liquidity_num` | Liquidity |

### prices_YYYY-MM-DD.csv
| Column | Description |
|--------|-------------|
| `snapshot_ts_utc` | Timestamp |
| `source` | `polymarket_clob` |
| `market_id` | Market ID |
| `slug` | URL slug |
| `question` | Market question |
| `token_id` | Token ID |
| `outcome` | Outcome label |
| `bid` | Best bid (SELL price) |
| `ask` | Best ask (BUY price) |
| `mid` | Midpoint price |
| `active` | Market active status |
| `status` | `ok`, `missing_price`, `api_error` |
| `volume_num` | Volume |
| `liquidity_num` | Liquidity |

## Troubleshooting

### Rate limits
The tool respects Polymarket rate limits (Gamma: 2/s, CLOB: 1/s). If you hit 429 errors, the tool will automatically retry with backoff.

### Missing prices
Some tokens may not have prices if:
- No orders exist on that side
- Market is inactive
- CLOB API returned an error

Check `status` column in prices CSV for details.

### Large runs
Full universe can take 5-10 minutes. Use `--max-markets 100` for testing.
