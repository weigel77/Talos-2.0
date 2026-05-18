# SPX / VIX Market Lookup

Small internal Flask application for local SPX and VIX market-data lookup with a provider-based market-data architecture.

## Features

- Local Flask web app that opens in the browser at `http://localhost:5000`
- Provider-based market-data layer with configuration-driven selection
- Current default provider: Yahoo Finance
- Included Schwab provider scaffold for future OAuth/app-based integration
- Included Schwab OAuth integration for local quote lookups once approved credentials are available
- Supported symbols in the current UI:
  - SPX: `^GSPC`
  - VIX: `^VIX`
- Query types:
  - Latest SPX data
  - SPX closing values for a date range
  - SPX daily change for a single date
  - SPX daily change for a date range
  - Latest VIX data
  - VIX closing value for a single date
  - VIX closing values for a date range
- Date-range results include:
  - Date
  - Open
  - High
  - Low
  - Close
  - Daily Point Change
  - Daily Percent Change
- CSV and Excel export
- America/Chicago normalization for displayed timestamps
- Modular service layer for future extension
- In-memory cache and retry handling around provider calls
- Logging to console and `market_lookup.log`

## Project structure

- [app.py](app.py) - Flask entry point and request handling
- [config.py](config.py) - environment-variable configuration
- [services/market_data.py](services/market_data.py) - provider-agnostic orchestration, cache coordination, and result metadata
- [services/calculations.py](services/calculations.py) - point and percent change logic
- [services/cache_service.py](services/cache_service.py) - in-memory cache helpers
- [services/export_service.py](services/export_service.py) - CSV and XLSX generation
- [services/provider_factory.py](services/provider_factory.py) - provider selection from configuration
- [services/providers/base_provider.py](services/providers/base_provider.py) - provider contract and shared errors
- [services/providers/yahoo_provider.py](services/providers/yahoo_provider.py) - Yahoo Finance implementation
- [services/providers/schwab_provider.py](services/providers/schwab_provider.py) - Schwab OAuth provider for latest quotes and historical daily prices
- [services/schwab_auth_service.py](services/schwab_auth_service.py) - Schwab OAuth authorization, token exchange, and refresh logic
- [services/token_store.py](services/token_store.py) - simple JSON token persistence for Schwab OAuth
- [templates/index.html](templates/index.html) - UI template
- [static/styles.css](static/styles.css) - simple UI styling
- [requirements.txt](requirements.txt) - Python dependencies
- [.env.example](.env.example) - sample provider and OAuth-related settings

## Requirements

- Python 3.11 or newer installed locally
- Internet access for Yahoo Finance requests

## Setup and run

### Configuration

The app reads environment variables for provider selection and future Schwab settings.

Supported values:

- `MARKET_DATA_PROVIDER=yahoo`
- `MARKET_DATA_PROVIDER=schwab`
- `APP_TIMEZONE=America/Chicago`
- `SCHWAB_CLIENT_ID`
- `SCHWAB_CLIENT_SECRET`
- `SCHWAB_REDIRECT_URI`
- `SCHWAB_AUTH_URL`
- `SCHWAB_TOKEN_URL`
- `SCHWAB_BASE_URL`
- `SCHWAB_TOKEN_PATH`
- `SCHWAB_SHARED_MARKET_TOKEN_PATH`

Talos 3.2 should point both `SCHWAB_TOKEN_PATH` and `SCHWAB_SHARED_MARKET_TOKEN_PATH` at the same Talos-3.0 market-data token file, for example `instance/schwab_market_data_token.json`.

Copy `.env.example` to `.env` and edit as needed, or set the variables directly in your shell.

For Schwab local OAuth, set:

- `MARKET_DATA_PROVIDER=schwab`
- `SCHWAB_REDIRECT_URI=https://127.0.0.1:5000/callback`

For hosted production OAuth on eigeltrade.com, set:

- `DELPHI_RUNTIME_TARGET=hosted`
- `HOSTED_PUBLIC_BASE_URL=https://eigeltrade.com`

When hosted runtime has `HOSTED_PUBLIC_BASE_URL` set, Delphi derives the Schwab redirect URI as `https://eigeltrade.com/callback` automatically.

When Schwab is the active provider, the Flask app starts with a local HTTPS development certificate so the callback route can receive the authorization code.

### Windows PowerShell

From the project folder, run these exact commands:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python app.py
```

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
python app.py
```

When the app starts, it will open automatically in your default browser.

If it does not open automatically, browse to:

- `http://localhost:5000`

## How to use

1. Select a query type.
2. Enter a single date or a start/end date when the selected query requires it.
3. Click **Run lookup**.
4. Review the summary cards and result table.
5. Use **Export CSV** or **Export Excel** to download the result.

## Validation rules

The app includes basic validation for:

- Invalid date formats
- Missing required dates
- End dates earlier than start dates
- Requested dates with no trading data
- Empty provider responses

## Notes on calculations

- `Point Change = current close - prior trading day close`
- `Percent Change = (point change / prior trading day close) * 100`
- For date ranges, the app pulls an additional lookback window so the first requested day can still calculate against the prior trading day when available.

## Logging and troubleshooting

- Application logs are written to `market_lookup.log`
- If the active provider returns no data, the app shows a friendly message in the UI
- If Yahoo Finance intraday data is unavailable, the latest lookup falls back to the latest daily close
- If `MARKET_DATA_PROVIDER=schwab` is selected before Schwab OAuth and app access are ready, the app returns a friendly configuration message instead of an obscure stack trace
- If Schwab tokens expire, the app refreshes them automatically where possible and otherwise redirects back through `/login`

## Manual verification checklist

After starting the app locally, verify the following:

1. Latest SPX lookup returns a result.
2. Latest VIX lookup returns a result.
3. SPX and VIX date-range tables render with the required columns.
4. SPX single-date daily change shows requested date, prior trading date, and change values.
5. CSV export downloads and opens cleanly in Excel.
6. Excel export downloads as `.xlsx` and opens cleanly in Excel.
7. Invalid date input shows a friendly validation message.

## Running Delphi 3.0 Production and Delphi 4.0 Dev in Parallel

- Delphi 3.0 Production stays in the current project folder and defaults to `127.0.0.1:5000`.
- Delphi 4.0 Dev runs from the cloned folder at [parallel/delphi-4.0-dev](parallel/delphi-4.0-dev) and uses `127.0.0.1:5001`.
- The two instances use separate browser session cookies, separate SQLite and replay storage, and separate log/token files.

### Setup the dev clone

From the production project root, run:

```powershell
.\scripts\setup_delphi4_dev_clone.ps1
```

### Start Delphi 3.0 Production

```powershell
.\scripts\start_delphi_production.ps1
```

### Start Delphi 4.0 Dev

```powershell
.\scripts\start_delphi_dev.ps1
```

### Default storage split

- Production database: [instance/horme_trades.db](instance/horme_trades.db)
- Production replay library: `%APPDATA%\Horme\kairos_replays`
- Production import previews: [instance/trade_import_previews](instance/trade_import_previews)
- Production log: [market_lookup.log](market_lookup.log)
- Production token file: [schwab_token.json](schwab_token.json)

- Dev database: [parallel/delphi-4.0-dev/instance/horme_trades.dev.db](parallel/delphi-4.0-dev/instance/horme_trades.dev.db)
- Dev replay library: [parallel/delphi-4.0-dev/instance/kairos_replays](parallel/delphi-4.0-dev/instance/kairos_replays)
- Dev import previews: [parallel/delphi-4.0-dev/instance/trade_import_previews](parallel/delphi-4.0-dev/instance/trade_import_previews)
- Dev log: [parallel/delphi-4.0-dev/instance/logs/market_lookup.dev.log](parallel/delphi-4.0-dev/instance/logs/market_lookup.dev.log)
- Dev token file: [parallel/delphi-4.0-dev/instance/schwab_token.dev.json](parallel/delphi-4.0-dev/instance/schwab_token.dev.json)

## Apollo post-update validation rule

No update should be treated as complete until Apollo regression checks run.

- Run `py -3 scripts/validate_apollo_post_update.py` after each meaningful change.
- The mock regression gate must pass every time.
- The live smoke gate runs when `APOLLO_LIVE_SMOKE=1` is set and Schwab OAuth credentials plus a token are available.
- Live smoke is allowed to skip on machines without a local Schwab session, but it should be run on the credentialed workstation before closing Apollo-related work.

## Future extension points

The code is structured for later additions such as:

- Full Schwab OAuth integration and token storage
- Additional market-data providers
- Options-chain support
- Trading-rule engines for Kairos
- Trading-rule engines for Apollo

## Provider notes

- Yahoo Finance is the temporary live provider today.
- The app now routes market-data requests through a provider abstraction layer.
- The Schwab provider now supports OAuth login plus latest and historical daily SPX/VIX lookups once approved credentials are available.
- Schwab option-chain support remains future work and currently returns a friendly not-yet-implemented message.

## Schwab OAuth flow

1. Start the app with `MARKET_DATA_PROVIDER=schwab`.
2. Open the app locally and click **Connect to Schwab**.
3. Sign in and approve access in the Schwab authorization window.
4. Schwab redirects back to `/callback`.
5. The app exchanges the code for access and refresh tokens and stores them in the configured token file.
6. Latest SPX and VIX queries use the Schwab quotes endpoint with automatic token refresh.
7. Historical range and single-date SPX/VIX queries use the Schwab price-history endpoint and continue to export clean CSV/XLSX files.

For Render hosted production, configure `HOSTED_PUBLIC_BASE_URL=https://eigeltrade.com` in the service environment. Hosted runtime will then build the Schwab authorize URL with `redirect_uri=https://eigeltrade.com/callback` while local `127.0.0.1` development callbacks remain unchanged.
