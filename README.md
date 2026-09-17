# VES-data-collection-processing

**Vestra Capital — Data Collection & Processing Pipeline**

A production-grade Python pipeline that retrieves trading account data from the Morrison Securities Data Access API, enriches it with derived client attributes, persists it into MongoDB, and provides a branded transactional email utility via the Brevo API.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Data Flow](#data-flow)
4. [Module Reference](#module-reference)
5. [Setup](#setup)
6. [Configuration](#configuration)
7. [Usage](#usage)
8. [Error Handling](#error-handling)
9. [Troubleshooting](#troubleshooting)
10. [Testing](#testing)
11. [Deployment Considerations](#deployment-considerations)

---

## Overview

This repository implements the **Vestra Capital Data Collection & Processing** pipeline. It is responsible for:

- **Discovering** adviser and branch scope from the Morrison Securities Data Access API (`/dataaccess/v1`).
- **Fetching** trading accounts for each adviser scope via the Morrison Securities Trading Accounts API (`/tradingaccounts/v2`) with `includeInactive=false`, fetching active accounts only.
- **Enriching** each raw trading account document with derived client fields:
  - `first_name` — parsed from `accountName`
  - `last_name` — parsed from `accountName`
  - `email` — sourced from `emailAddress` or `contractNoteEmailAddress`
  - `telephone` — sourced from `mobilePhone`, `workPhone`, or `homePhone`
  - `client_category` — always set to `"Wealth Management"`
- **Upserting** enriched documents into the MongoDB `clients` collection, keyed by `accountNumber`.
- **Fetching** account equity holdings via the Morrison Securities Equity Holdings API (`/equityholdings/v1`) without zero holdings and persisting them into MongoDB `portfolios`.
- **Sending** branded transactional emails via the Brevo SMTP transactional API.
- **Reporting** pending prospects by querying the MongoDB `prospects` collection and sending a branded HTML table email.
- **Calculating** portfolio risk scores and drawdown metrics from NAV timeseries and holdings, persisting results into MongoDB `portfolios`.

The pipeline is designed to be:
- **Idempotent** — repeated runs safely update existing records via upsert.
- **Side-effect-free on import** — modules can be safely imported by tests or other code.
- **Resilient** — API errors, empty responses, and network failures are surfaced as explicit exceptions with diagnostic context.

---

## Architecture

### System Context

```mermaid
graph LR
    A[Vestra Capital<br/>Data Pipeline] --> B[Morrison Securities<br/>Data Access API<br/>/dataaccess/v1]
    A --> C[Morrison Securities<br/>Trading Accounts API<br/>/tradingaccounts/v2]
    A --> D[Morrison Securities<br/>Equity Holdings API<br/>/equityholdings/v1]
    A --> E[Morrison Securities<br/>Cash Balance API<br/>/cashbalances/v1]
    A --> F[Morrison Securities<br/>Equity Holding Transactions API<br/>/equityholdingtransactions/v1]
    A --> G[Market Data Provider<br/>YFinanceProvider]
    A --> H[(MongoDB Atlas<br/>VESTRA_PROD.clients)]
    A --> I[(MongoDB Atlas<br/>VESTRA_PROD.portfolios)]
    A --> J[(MongoDB Atlas<br/>VESTRA_PROD.prospects)]
    A --> K[(MongoDB Atlas<br/>VESTRA_PROD.instruments)]
    A --> L[(MongoDB Atlas<br/>VESTRA_PROD.trades)]
    A --> M[(MongoDB Atlas<br/>VESTRA_PROD.fees)]
    A --> N[Brevo SMTP API<br/>v3/smtp/email]
```

### Component Architecture

```mermaid
graph TB
    subgraph "Python Pipeline"
        CLI["CLI Entry Point<br/>python data/001_get_trading_account.py"]
        CONFIG["configuration.py<br/>fetch_data()"]
        FETCHER["001_get_trading_account.py<br/>fetch_trading_accounts()"]
        ENRICH["Enrichment Layer<br/>_enrich_client_document()"]
        PERSIST["Persistence Layer<br/>upsert_clients()"]
        EMAIL["email/send_email.py<br/>send_email()"]
        PROSPECTS["scripts/send_pending_prospects_email.py<br/>send_pending_prospects_email()"]
        HOLDINGS["002_get_portfolio_holdings.py<br/>fetch_account_equity_holdings()"]
        HOLDINGS_PERSIST["Persistence Layer<br/>upsert_portfolios()"]
        CASH["003_get_cash_balances.py<br/>fetch_account_cash_balance()"]
        CASH_PERSIST["Persistence Layer<br/>upsert_cash_balances()"]
        TRADES["004_get_trades_history.py<br/>fetch_equity_holding_transactions()"]
        TRADES_PERSIST["Persistence Layer<br/>upsert_trades()"]
        INSTRUMENTS["batch/get_instruments.py<br/>extract_instruments()"]
        INSTRUMENTS_ENRICH["Market Data Provider<br/>_enrich_instruments()"]
        INSTRUMENTS_PERSIST["Persistence Layer<br/>upsert_instruments()"]
        INSTRUMENTS_TS["batch/get_instruments_timeseries.py<br/>fetch_instrument_timeseries()"]
        INSTRUMENTS_TS_PERSIST["Persistence Layer<br/>_upsert_instrument_timeseries()"]
        PORTFOLIOS_NAV["batch/get_portfolios_nav.py<br/>_calculate_nav_timeseries()"]
        PORTFOLIOS_NAV_PERSIST["Persistence Layer<br/>_update_portfolio_nav()"]
        FEES["data/005_get_data_for_fees_reporting.py<br/>_build_aum_document()"]
        FEES_PERSIST["Persistence Layer<br/>insert_aum()"]
        RISK["006_get_risk_score.py<br/>calculate_risk_score()"]
        RISK_PERSIST["Persistence Layer<br/>update_portfolio_metrics()"]
        BATCH_COLLECT["batch/data_collection.py<br/>main()"]
        BATCH_FEES["batch/fees_reporting.py<br/>main()"]
        PRICE_STREAMER["batch/price_streamer.py<br/>main()"]
    end

    subgraph "External Services"
        MORRISON_CONFIG["Morrison Securities<br/>Data Access API"]
        MORRISON_TRADE["Morrison Securities<br/>Trading Accounts API"]
        MORRISON_HOLDINGS["Morrison Securities<br/>Equity Holdings API"]
        MORRISON_CASH["Morrison Securities<br/>Cash Balance API"]
        MORRISON_TRADES["Morrison Securities<br/>Equity Holding Transactions API"]
        MARKET["Market Data Provider<br/>YFinanceProvider"]
        MONGO[(MongoDB Atlas<br/>VESTRA_PROD.clients)]
        PORTFOLIO_MONGO[(MongoDB Atlas<br/>VESTRA_PROD.portfolios)]
        PROSPECTS_MONGO[(MongoDB Atlas<br/>VESTRA_PROD.prospects)]
        INSTRUMENTS_MONGO[(MongoDB Atlas<br/>VESTRA_PROD.instruments)]
        TRADES_MONGO[(MongoDB Atlas<br/>VESTRA_PROD.trades)]
        FEES_MONGO[(MongoDB Atlas<br/>VESTRA_PROD.fees)]
        BREVO["Brevo<br/>Transactional Email"]
    end

    CLI --> CONFIG
    CONFIG --> MORRISON_CONFIG
    MORRISON_CONFIG --> CONFIG
    CONFIG --> FETCHER
    FETCHER --> MORRISON_TRADE
    MORRISON_TRADE --> FETCHER
    FETCHER --> ENRICH
    ENRICH --> PERSIST
    PERSIST --> MONGO
    CLI --> EMAIL
    EMAIL --> BREVO
    PROSPECTS --> PROSPECTS_MONGO
    PROSPECTS --> EMAIL
    EMAIL --> BREVO
    HOLDINGS --> MONGO
    HOLDINGS --> HOLDINGS_PERSIST
    HOLDINGS_PERSIST --> PORTFOLIO_MONGO
    CASH --> MORRISON_CASH
    CASH --> CASH_PERSIST
    CASH_PERSIST --> PORTFOLIO_MONGO
    TRADES --> MORRISON_TRADES
    TRADES --> TRADES_PERSIST
    TRADES_PERSIST --> TRADES_MONGO
    INSTRUMENTS --> PORTFOLIO_MONGO
    INSTRUMENTS --> INSTRUMENTS_ENRICH
    INSTRUMENTS_ENRICH --> MARKET
    INSTRUMENTS_ENRICH --> INSTRUMENTS_PERSIST
    INSTRUMENTS_PERSIST --> INSTRUMENTS_MONGO
    INSTRUMENTS_TS --> INSTRUMENTS_MONGO
    INSTRUMENTS_TS --> INSTRUMENTS_TS_PERSIST
    INSTRUMENTS_TS_PERSIST --> INSTRUMENTS_MONGO
    PORTFOLIOS_NAV --> PORTFOLIO_MONGO
    PORTFOLIOS_NAV --> INSTRUMENTS_MONGO
    PORTFOLIOS_NAV --> MARKET
    PORTFOLIOS_NAV --> PORTFOLIOS_NAV_PERSIST
    PORTFOLIOS_NAV_PERSIST --> PORTFOLIO_MONGO
    FEES --> MONGO
    FEES --> PORTFOLIO_MONGO
    FEES --> INSTRUMENTS_MONGO
    FEES --> FEES_PERSIST
    FEES_PERSIST --> FEES_MONGO
    RISK --> PORTFOLIO_MONGO
    RISK --> INSTRUMENTS_MONGO
    RISK --> RISK_PERSIST
    RISK_PERSIST --> PORTFOLIO_MONGO
    BATCH_COLLECT --> CLI
    BATCH_COLLECT --> HOLDINGS
    BATCH_COLLECT --> CASH
    BATCH_COLLECT --> TRADES
    BATCH_FEES --> CLI
    BATCH_FEES --> HOLDINGS
    BATCH_FEES --> CASH
    BATCH_FEES --> INSTRUMENTS
    BATCH_FEES --> INSTRUMENTS_TS
    BATCH_FEES --> PORTFOLIOS_NAV
    BATCH_FEES --> FEES
    PRICE_STREAMER --> INSTRUMENTS_MONGO
    PRICE_STREAMER --> MARKET
```

### Module Dependency Graph

```mermaid
graph TD
    MAIN["001_get_trading_account.py<br/>(__main__)"] --> CONFIG["configuration.py"]
    MAIN --> ENRICH["Enrichment Helpers<br/>(internal)"]
    MAIN --> MONGO["MongoDB Client<br/>(internal)"]
    HOLDINGS["002_get_portfolio_holdings.py<br/>(__main__)"] --> CONFIG
    HOLDINGS --> MONGO
    CASH["003_get_cash_balances.py<br/>(__main__)"] --> CONFIG
    CASH --> MONGO
    TRADES["004_get_trades_history.py<br/>(__main__)"] --> CONFIG
    TRADES --> MONGO
    FEES["005_get_data_for_fees_reporting.py<br/>(__main__)"] --> MONGO
    FEES --> MATH["math"]
    RISK["006_get_risk_score.py<br/>(__main__)"] --> CONFIG
    RISK --> PYTHON_MONGO["pymongo"]
    RISK --> MATH
    EMAIL["email/send_email.py"] --> DOTENV["python-dotenv"]
    CONFIG --> DOTENV
    MAIN --> DOTENV
    HOLDINGS --> DOTENV
    CASH --> DOTENV
    TRADES --> DOTENV
    FEES --> DOTENV
    RISK --> DOTENV
    MAIN --> PYTHON_MONGO
    HOLDINGS --> PYTHON_MONGO
    CASH --> PYTHON_MONGO
    TRADES --> PYTHON_MONGO
    FEES --> PYTHON_MONGO
    EMAIL --> REQUESTS["requests"]
    PROSPECTS["scripts/send_pending_prospects_email.py"] --> DOTENV
    PROSPECTS --> PYTHON_MONGO
    PROSPECTS --> EMAIL
    BATCH_COLLECT["batch/data_collection.py<br/>(__main__)"] --> SUBPROCESS["subprocess"]
    BATCH_FEES["batch/fees_reporting.py<br/>(__main__)"] --> SUBPROCESS
    GET_INSTRUMENTS["batch/get_instruments.py<br/>(__main__)"] --> DOTENV
    GET_INSTRUMENTS --> PYTHON_MONGO
    GET_INSTRUMENTS --> MARKET["market provider"]
    GET_INSTRUMENTS_TS["batch/get_instruments_timeseries.py<br/>(__main__)"] --> DOTENV
    GET_INSTRUMENTS_TS --> PYTHON_MONGO
    GET_INSTRUMENTS_TS --> MARKET
    GET_PORTFOLIOS_NAV["batch/get_portfolios_nav.py<br/>(__main__)"] --> DOTENV
    GET_PORTFOLIOS_NAV --> PYTHON_MONGO
    GET_PORTFOLIOS_NAV --> MARKET
    PRICE_STREAMER["batch/price_streamer.py<br/>(__main__)"] --> DOTENV
    PRICE_STREAMER --> PYTHON_MONGO
    PRICE_STREAMER --> MARKET
    PRICE_STREAMER --> TIME["time"]
```

---

## Data Flow

### Trading Account Pipeline

```mermaid
sequenceDiagram
    participant CLI as 001_get_trading_account.py
    participant Config as configuration.py
    participant Morrison as Morrison Securities API
    participant Enrich as Enrichment Layer
    participant Mongo as MongoDB clients

    CLI->>Config: fetch_data()
    Config->>Morrison: GET /dataaccess/v1
    Morrison-->>Config: adviser scope JSON
    Config-->>CLI: scope items

    loop For each adviser scope
        CLI->>Morrison: GET /tradingaccounts/v2?...&includeInactive=false
        Morrison-->>CLI: trading accounts JSON
        CLI->>Enrich: _enrich_client_document(account)
        Enrich-->>CLI: enriched document
    end

    CLI->>Mongo: replace_one(filter, enriched_doc, upsert=True)
    Mongo-->>CLI: OK
```

### Enrichment Transform

```mermaid
graph LR
    A[Raw Trading Account] --> B{Extract first_name and last_name<br/>from accountName}
    A --> C{Extract email<br/>from emailAddress or contractNoteEmailAddress}
    A --> D{Extract telephone<br/>from mobilePhone, workPhone, or homePhone}
    A --> E[Set client_category = Wealth Management]
    B --> F[Enriched Client Document]
    C --> F
    D --> F
    E --> F
    F --> G[(MongoDB clients collection)]
```

### Portfolio Holdings Pipeline

```mermaid
sequenceDiagram
    participant CLI as 002_get_portfolio_holdings.py
    participant MongoClients as MongoDB clients
    participant Morrison as Morrison Securities API
    participant Normalize as Normalization Layer
    participant MongoPortfolios as MongoDB portfolios

    CLI->>MongoClients: find({}, {"_id": 0})
    MongoClients-->>CLI: client documents

    loop For each accountNumber
        CLI->>Morrison: GET /equityholdings/v1?...&includeZeroHoldings=false
        Morrison-->>CLI: equity holdings JSON
        CLI->>Normalize: _normalize_holdings_documents(data)
        Normalize-->>CLI: holdings list
        CLI->>Normalize: _normalize_holding_document(holding)
        Normalize-->>CLI: normalized holding
    end

    CLI->>MongoPortfolios: replace_one(filter, doc, upsert=True)
    MongoPortfolios-->>CLI: OK

    CLI->>MongoPortfolios: delete_many({accountNumber: {$nin: active}})
    MongoPortfolios-->>CLI: deleted count
```

### Holdings Normalisation Transform

```mermaid
graph LR
    A[Raw Holding] --> B{Map marketCode<br/>e.g. ASX → AX}
    A --> C[Title-case securityDescription]
    B --> D[Normalized Holding Document]
    C --> D
    D --> E[(MongoDB portfolios collection)]
```

### Pending Prospects Pipeline

```mermaid
sequenceDiagram
    participant Script as send_pending_prospects_email.py
    participant Mongo as MongoDB prospects
    participant Email as email/send_email.py
    participant Brevo as Brevo SMTP API

    Script->>Mongo: find({"status": {$regex: "^Pending$", $options: "i"}})
    Mongo-->>Script: prospects list
    Script->>Script: build_html_table(prospects)
    Script->>Email: send_email({email, subject, message})
    Email->>Brevo: POST /v3/smtp/email
    Brevo-->>Email: messageId
    Email-->>Script: response body
```

### Cash Balances Pipeline

```mermaid
sequenceDiagram
    participant Script as 003_get_cash_balances.py
    participant MongoClients as MongoDB clients
    participant Morrison as Morrison Securities API
    participant MongoPortfolios as MongoDB portfolios

    Script->>MongoClients: find({}, {"_id": 0})
    MongoClients-->>Script: client documents

    loop For each accountNumber
        Script->>Morrison: GET /cashbalances/v1?...&accountNumber=...
        Morrison-->>Script: cash balance JSON
        Script->>Script: _normalize_cash_documents(data)
    end

    Script->>MongoPortfolios: update_one({accountNumber}, {$set: {cash}})
    MongoPortfolios-->>Script: OK
```

### Trades History Pipeline

```mermaid
sequenceDiagram
    participant Script as 004_get_trades_history.py
    participant MongoClients as MongoDB clients
    participant Morrison as Morrison Securities API
    participant MongoTrades as MongoDB trades

    Script->>MongoClients: find({}, {"_id": 0})
    MongoClients-->>Script: client documents

    loop For each accountNumber
        Script->>Morrison: GET /equityholdingtransactions/v1?...&accountNumber=...
        Morrison-->>Script: trades JSON
        Script->>Script: _normalize_trades_documents(data)
        Script->>Script: _enrich_trades_with_pnl(trades)
    end

    Script->>MongoTrades: replace_one(filter, doc, upsert=True)
    MongoTrades-->>Script: OK
```

### Instruments Enrichment Pipeline

```mermaid
sequenceDiagram
    participant Script as batch/get_instruments.py
    participant MongoPortfolios as MongoDB portfolios
    participant MongoInstruments as MongoDB instruments
    participant Market as Market Data Provider

    Script->>MongoPortfolios: find({}, {"_id": 0})
    MongoPortfolios-->>Script: portfolio documents
    Script->>Script: extract_instruments(portfolios)

    Script->>MongoInstruments: find({symbol: {$in: symbols}}, {_id: 0})
    MongoInstruments-->>Script: existing instruments

    loop For each instrument missing metadata
        Script->>Market: fetch_instrument_metadata(symbol)
        Market-->>Script: {sector, industry, dividend_yield, currentPrice}
    end

    Script->>MongoInstruments: replace_one({symbol}, doc, upsert=True)
    MongoInstruments-->>Script: OK
```

### Instruments Timeseries Pipeline

```mermaid
sequenceDiagram
    participant Script as batch/get_instruments_timeseries.py
    participant MongoInstruments as MongoDB instruments
    participant Market as Market Data Provider

    Script->>MongoInstruments: find({}, {"_id": 0})
    MongoInstruments-->>Script: instrument documents

    loop For each instrument
        Script->>Market: fetch_instrument_timeseries(symbol, period="1y")
        Market-->>Script: [{date, price}, ...]
        Script->>Script: Append timeseries to instrument document
    end

    Script->>MongoInstruments: replace_one({symbol}, doc, upsert=True)
    MongoInstruments-->>Script: OK
```

### Portfolio NAV Pipeline

```mermaid
sequenceDiagram
    participant Script as batch/get_portfolios_nav.py
    participant MongoPortfolios as MongoDB portfolios
    participant MongoInstruments as MongoDB instruments
    participant Market as Market Data Provider

    Script->>MongoPortfolios: find({}, {"_id": 0})
    MongoPortfolios-->>Script: portfolio documents

    Script->>MongoInstruments: find({symbol: {$in: symbols}}, {_id: 0})
    MongoInstruments-->>Script: instrument documents

    loop For each portfolio
        Script->>Script: _calculate_nav_timeseries(holdings, price_map)
        Script->>MongoPortfolios: update_one({accountNumber}, {$set: {nav_timeseries}})
    end
```

### Fees Reporting Pipeline

```mermaid
sequenceDiagram
    participant Script as data/005_get_data_for_fees_reporting.py
    participant MongoClients as MongoDB clients
    participant MongoPortfolios as MongoDB portfolios
    participant MongoInstruments as MongoDB instruments
    participant MongoFees as MongoDB fees

    Script->>MongoClients: find({}, {"_id": 0})
    MongoClients-->>Script: client documents

    Script->>MongoPortfolios: find({}, {"_id": 0})
    MongoPortfolios-->>Script: portfolio documents

    Script->>MongoInstruments: find({}, {"_id": 0})
    MongoInstruments-->>Script: instrument documents

    loop For each client
        Script->>Script: _build_aum_document(client, portfolio, total_holdings)
        Script->>Script: _calculate_account_pnl(holdings, instruments, price_map, now)
    end

    Script->>MongoFees: update_one({accountNumber, generatedAt}, {$set: doc}, upsert=True)
    MongoFees-->>Script: OK

    Script->>Script: generate_daily_fee_report()
```

### Batch Data Collection Pipeline

```mermaid
sequenceDiagram
    participant Script as batch/data_collection.py
    participant S1 as 001_get_trading_account.py
    participant S2 as 002_get_portfolio_holdings.py
    participant S3 as 003_get_cash_balances.py
    participant S4 as 004_get_trades_history.py

    Script->>S1: subprocess.run([python, S1])
    S1-->>Script: exit code 0
    Script->>S2: subprocess.run([python, S2])
    S2-->>Script: exit code 0
    Script->>S3: subprocess.run([python, S3])
    S3-->>Script: exit code 0
    Script->>S4: subprocess.run([python, S4])
    S4-->>Script: exit code 0
```

### Batch Fees Reporting Pipeline

```mermaid
sequenceDiagram
    participant Script as batch/fees_reporting.py
    participant S1 as 001_get_trading_account.py
    participant S2 as 002_get_portfolio_holdings.py
    participant S3 as 003_get_cash_balances.py
    participant S4 as batch/get_instruments.py
    participant S5 as batch/get_instruments_timeseries.py
    participant S6 as batch/get_portfolios_nav.py
    participant S7 as data/005_get_data_for_fees_reporting.py

    Script->>S1: subprocess.run([python, S1])
    S1-->>Script: exit code 0
    Script->>S2: subprocess.run([python, S2])
    S2-->>Script: exit code 0
    Script->>S3: subprocess.run([python, S3])
    S3-->>Script: exit code 0
    Script->>S4: subprocess.run([python, S4])
    S4-->>Script: exit code 0
    Script->>S5: subprocess.run([python, S5])
    S5-->>Script: exit code 0
    Script->>S6: subprocess.run([python, S6])
    S6-->>Script: exit code 0
    Script->>S7: subprocess.run([python, S7])
    S7-->>Script: exit code 0
```

### Price Streamer Pipeline

```mermaid
sequenceDiagram
    participant Script as batch/price_streamer.py
    participant Mongo as MongoDB instruments
    participant Market as Market Data Provider

    Script->>Mongo: find({}, {"_id": 0, "symbol": 1})
    Mongo-->>Script: symbol list

    loop Every poll interval
        Script->>Market: fetch_current_prices(symbols)
        Market-->>Script: {symbol: price, ...}
        loop For each symbol/price
            Script->>Mongo: update_one({symbol}, {$set: {currentPrice}})
        end
    end
```

---

## Module Reference

### `data/configuration.py`

**Purpose:** Centralised configuration and HTTP client for the Morrison Securities Data Access API.

**Key exports:**

| Symbol | Type | Description |
|--------|------|-------------|
| `BASE_URL` | `str` | Base URL for the Morrison Securities API host. Overridable via `MORRISON_API_BASE_URL`. |
| `API_URL` | `str` | Full URL to the data-access endpoint (`BASE_URL + /dataaccess/v1`). |
| `HEADERS` | `Dict[str, str]` | Default request headers including `x-api-key` from `MORRISON_ACCESS_KEY`. |
| `fetch_data()` | `function` | Primary entry point. Performs a GET request and returns parsed JSON. |

**Design notes:**
- Loads `.env` at import time via `python-dotenv`.
- Raises `RuntimeError` on missing credentials, empty responses, or HTTP errors.
- Uses `urllib` (standard library) for zero external HTTP dependencies in the config layer.

---

### `data/001_get_trading_account.py`

**Purpose:** Fetch trading accounts for each adviser scope, enrich records, and upsert into MongoDB.

**Key exports:**

| Symbol | Type | Description |
|--------|------|-------------|
| `fetch_trading_accounts(scope_item)` | `function` | GET `/tradingaccounts/v2` with scoping parameters. Returns parsed JSON. |
| `upsert_clients(documents)` | `function` | Upsert a list of trading account dicts into `VESTRA_PROD.clients`. |
| `_normalize_to_documents(data)` | `function` | Unwrap API response envelope (`Data` field) into a flat list of account dicts. |
| `_enrich_client_document(doc)` | `function` | Add `first_name`, `last_name`, `email`, `telephone`, `client_category` to a raw account dict. |

**Enrichment logic:**

```mermaid
graph TD
    A[accountName input] --> B{Is empty?}
    B -->|Yes| C[Return empty strings]
    B -->|No| D[Split on plus sign for joint accounts]
    D --> E[Take primary person only]
    E --> F[Strip titles: MR, MRS, MS, DR, PROF, SIR, MADAM, LORD, LADY]
    F --> G{Remaining tokens?}
    G -->|0| C
    G -->|1| H[first = token, last = empty string]
    G -->|2+| I[first = first token, last = last token]
```

**Upsert strategy:**

- Documents with `accountNumber` are matched via `replace_one(filter={"accountNumber": ...}, upsert=True)`.
- Documents without `accountNumber` are inserted via `insert_one()`.
- The original document is not mutated; `_enrich_client_document()` returns a copy.

---

### `data/002_get_portfolio_holdings.py`

**Purpose:** Fetch account equity holdings from the Morrison Securities Equity Holdings API (`/equityholdings/v1`) with `includeZeroHoldings=false`, normalise documents (add `marketCode_yf`, title-case `securityDescription`), and upsert into MongoDB `portfolios` collection. Inactive accounts and accounts with empty holdings are removed from the collection after each upsert. The `006_get_risk_score.py` script subsequently enriches portfolio documents with `riskScore`, `riskLabel`, and drawdown metrics.

**Key exports:**

| Symbol | Type | Description |
|--------|------|-------------|
| `fetch_account_equity_holdings(scope_item)` | `function` | GET `/equityholdings/v1` with scoping parameters. Returns parsed JSON. |
| `upsert_portfolios(documents, active_account_numbers)` | `function` | Upsert portfolio documents into `VESTRA_PROD.portfolios` and remove inactive/empty accounts. |
| `_normalize_holding_document(doc)` | `function` | Return a copy of a holding document with derived fields (`marketCode_yf`, title-cased `securityDescription`). |
| `_normalize_holdings_documents(data)` | `function` | Unwrap API response envelope into a flat list of holding dicts. |

**Normalisation logic:**

- ``marketCode_yf`` is mapped from ``marketCode`` using a known exchange mapping (e.g. ``ASX`` → ``AX``).
- ``securityDescription`` is reformatted from ALL-CAPS to title case.

**Upsert strategy:**

- Each document represents one account and is matched by ``accountNumber`` via ``replace_one(filter={"accountNumber": ...}, upsert=True)``.
- Documents without ``accountNumber`` are inserted via ``insert_one()``.
- After upserting, any documents whose ``accountNumber`` is not present in the current active batch are removed, purging inactive accounts and accounts with empty holdings from the collection.

---

### `data/006_get_risk_score.py`

**Purpose:** Calculate portfolio risk scores and drawdown metrics from NAV timeseries and holdings, and persist results into MongoDB `portfolios`.

**Key exports:**

| Symbol | Type | Description |
|--------|------|-------------|
| `calculate_risk_score(nav_timeseries, holdings, sector_map)` | `function` | Calculate a portfolio risk score from 1 to 10 based on volatility (60%) and diversification (40%). |
| `get_risk_label(risk_score)` | `function` | Map a 1–10 risk score to a human-readable label (`Very Low`, `Low`, `Moderate`, `High`, `Very High`). |
| `calculate_drawdown_metrics(nav_timeseries)` | `function` | Calculate `maxDrawdown`, `maxWeeklyDrawdown`, and `maxYearlyDrawdown` from NAV timeseries. |
| `update_portfolio_metrics(db_name, account_number, risk_score, risk_label, drawdown_metrics)` | `function` | Upsert risk metrics into `VESTRA_PROD.portfolios` by `accountNumber`. |

**Risk score methodology:**

- **Volatility component (60%):** Computes daily NAV returns, calculates population standard deviation, annualises using `daily_std * sqrt(min(n, 252))`, and normalises by dividing by `0.40` (40% annualised volatility maps to 1.0), clamped to `[0.0, 1.0]`.
- **Diversification component (40%):** Computes Herfindahl-Hirschman Index (HHI) of market-value weights, normalises concentration to `[0.0, 1.0]`, computes sector diversity as `min(unique_sectors / 10, 1)`, and combines as `1.0 - (0.6 * concentration + 0.4 * (1.0 - sector_diversity))`, clamped to `[0.0, 1.0]`.
- **Final score:** `combined_risk = 0.6 * vol_score + 0.4 * (1.0 - div_score)`, then `risk_score = 1.0 + combined_risk * 9.0`, clamped to `[1.0, 10.0]` and rounded to the nearest integer.

**Risk labels:**

| `riskScore` | `riskLabel` |
|---|---|
| 1 | Very Low |
| 2–3 | Low |
| 4–5 | Moderate |
| 6–8 | High |
| 9–10 | Very High |

**Drawdown methodology:**

All drawdown values are expressed as positive decimals (e.g. `0.25` = 25% decline).

- `maxDrawdown`: Scans the full NAV series and records the largest peak-to-trough decline.
- `maxWeeklyDrawdown`: Slides a 5-day rolling window across the NAV series and records the largest peak-to-trough decline within any such window.
- `maxYearlyDrawdown`: Slides a 252-day rolling window across the NAV series and records the largest peak-to-trough decline within any such window. If fewer than 252 data points are available, the window size is capped to the series length.

**Database update:**

Each portfolio document is updated by `accountNumber`. The write uses `upsert=True`. Fields set:

- `riskScore`
- `riskLabel`
- `maxDrawdown`
- `maxWeeklyDrawdown`
- `maxYearlyDrawdown`

**Environment variables:**
- `MONGODB_SRV` — MongoDB connection string.
- `DATABASE_NAME` — Database name (defaults to `VESTRA_PROD`).

---

### `data/003_get_cash_balances.py`

**Purpose:** Fetch account cash balances from the Morrison Securities Cash Balance API (`/cashbalances/v1`), normalise records, and upsert them into MongoDB `portfolios` as a `cash` sibling alongside `holdings`.

**Key exports:**

| Symbol | Type | Description |
|--------|------|-------------|
| `fetch_account_cash_balance(scope_item)` | `function` | GET `/cashbalances/v1` with scoping parameters. Returns parsed JSON. |
| `upsert_cash_balances(documents)` | `function` | Upsert cash balance documents into `VESTRA_PROD.portfolios` by `accountNumber`. |
| `_normalize_cash_documents(data)` | `function` | Unwrap API response envelope into a flat list of cash balance dicts. |

**Upsert strategy:**

- Each document represents one account's cash balance and is matched by `accountNumber`.
- Documents without `accountNumber` are inserted via `insert_one()`.
- The `cash` field is set using `$set` so that existing `holdings` data is preserved.
- Documents without `accountNumber` are skipped.

**Environment variables:**
- `MONGODB_SRV` — MongoDB connection string.
- `DATABASE_NAME` — Database name (defaults to `VESTRA_PROD`).

---

### `data/004_get_trades_history.py`

**Purpose:** Fetch equity holding transactions from the Morrison Securities Equity Holding Transactions API (`/equityholdingtransactions/v1`), normalise records, enrich with realised P&L using FIFO lot matching, and upsert into MongoDB `trades`.

**Key exports:**

| Symbol | Type | Description |
|--------|------|-------------|
| `fetch_equity_holding_transactions(scope_item)` | `function` | GET `/equityholdingtransactions/v1` with scoping parameters. Returns parsed JSON. |
| `upsert_trades(documents)` | `function` | Upsert trade documents into `VESTRA_PROD.trades` and purge inactive accounts. |
| `_normalize_trades_documents(data)` | `function` | Unwrap API response envelope into a flat list of trade dicts. |
| `_enrich_trades_with_pnl(trades)` | `function` | Enrich trades with realised P&L using FIFO lot matching. |
| `_get_trade_filter(doc)` | `function` | Build a unique filter query for a trade document. |

**P&L enrichment:**

- Matches SELL transactions against the earliest unmatched BUY lot for the same `accountNumber` + `securityCode`.
- Matching is done at the quantity level so partial sells are handled correctly.
- Transaction costs are included in the cost basis for BUYs and deducted from proceeds for SELLs.

**Upsert strategy:**

- Each document is matched using a unique trade identifier when available (`reference`, `transactionId`, `tradeId`, etc.).
- Documents without an identifiable key are inserted as-is.
- After upserting, any documents whose `accountNumber` is not present in the current active batch are removed, ensuring that trades for inactive accounts are purged.

**Environment variables:**
- `MONGODB_SRV` — MongoDB connection string.
- `DATABASE_NAME` — Database name (defaults to `VESTRA_PROD`).

---

### `data/005_get_data_for_fees_reporting.py`

**Purpose:** Collect assets under management (AUM) and daily P&L data for fees reporting. Reads `clients`, `portfolios`, and `instruments` collections, builds per-client AUM snapshots, and upserts them into the `fees` collection.

**Key exports:**

| Symbol | Type | Description |
|--------|------|-------------|
| `insert_aum(documents)` | `function` | Upsert AUM documents into `VESTRA_PROD.fees` keyed by `accountNumber` and `generatedAt`. |
| `generate_daily_fee_report(rate)` | `function` | Generate and print a daily fee report from the `fees` collection. |
| `_build_aum_document(client, portfolio, rate, total_holdings)` | `function` | Build an AUM document for a single client/account. |
| `_calculate_account_pnl(holdings, instruments, price_map, today)` | `function` | Calculate total P&L for an account using current price, timeseries, and market price fallbacks. |
| `_latest_nav_on_or_before(nav_timeseries, target_date)` | `function` | Return the latest NAV from `nav_timeseries` on or before `target_date`. |

**AUM calculation:**

- `total_holdings` is taken from the latest entry in the portfolio's `nav_timeseries` array (populated by `batch/get_portfolios_nav.py`).
- `total_cash` is summed from all bank accounts in the portfolio's `cash.bankAccounts`.
- `total_aum = total_holdings + total_cash`.
- `collected_fees = (total_aum * rate / 100) / days_in_year`.
- `totalPnl = sum((currentPrice or marketPrice - averageCost) * totalHolding)`.

**Backfill:**

- `_backfill_missing_fee_fields()` backfills `feeReportRate` and `collectedFees` for existing documents missing these fields.
- `_backfill_missing_pnl_fields()` backfills `totalPnl` for existing documents missing the field.

**Environment variables:**
- `MONGODB_SRV` — MongoDB connection string.
- `DATABASE_NAME` — Database name (defaults to `VESTRA_PROD`).
- `FEES_REPORT_RATE` — Annual fee rate as a percentage (defaults to `1.5`).

---

### `batch/data_collection.py`

**Purpose:** Orchestrate the core data collection pipeline by running `001_get_trading_account.py`, `002_get_portfolio_holdings.py`, `003_get_cash_balances.py`, and `004_get_trades_history.py` sequentially.

**Key exports:**

| Symbol | Type | Description |
|--------|------|-------------|
| `SCRIPTS` | `List[str]` | Ordered list of data collection scripts to execute. |
| `main()` | `function` | Run the data collection pipeline sequentially. Stops at the first failure. |

**Execution:**

- Each script is executed using the current Python interpreter via `subprocess.run()`.
- Execution stops at the first failure and returns the offending exit code.
- Returns `0` if all scripts succeed.

**Usage:**

```bash
python batch/data_collection.py
```

---

### `batch/fees_reporting.py`

**Purpose:** Orchestrate the fees reporting pipeline by running data collection, instrument enrichment, NAV calculation, and AUM generation scripts sequentially.

**Key exports:**

| Symbol | Type | Description |
|--------|------|-------------|
| `SCRIPTS` | `List[str]` | Ordered list of scripts to execute for fees reporting. |
| `main()` | `function` | Run the fees reporting pipeline sequentially. Stops at the first failure. |

**Execution order:**

1. `data/001_get_trading_account.py`
2. `data/002_get_portfolio_holdings.py`
3. `data/003_get_cash_balances.py`
4. `batch/get_instruments.py`
5. `batch/get_instruments_timeseries.py`
6. `batch/get_portfolios_nav.py`
7. `data/005_get_data_for_fees_reporting.py`

**Usage:**

```bash
python batch/fees_reporting.py
```

---

### `batch/get_instruments.py`

**Purpose:** Extract unique instruments from the portfolios collection, enrich them with market metadata (sector, industry, dividend yield, current price), and upsert the results into the `instruments` collection.

**Key exports:**

| Symbol | Type | Description |
|--------|------|-------------|
| `extract_instruments(portfolio_documents)` | `function` | Flatten holdings arrays and deduplicate by `symbol`. |
| `upsert_instruments(db_name, instruments)` | `function` | Upsert instrument documents into `VESTRA_PROD.instruments`. |
| `_fetch_instruments_missing_metadata(db_name, symbols)` | `function` | Query the `instruments` collection for documents missing metadata. |
| `_enrich_instruments(provider, instruments, existing_docs)` | `function` | Fetch market metadata for instruments missing it and merge. |

**Enrichment logic:**

- Instruments that already have `sector`, `industry`, `dividend_yield`, and `currentPrice` stored are skipped.
- Only instruments missing at least one of these fields are enriched via the configured market data provider.

**Environment variables:**
- `MONGODB_SRV` — MongoDB connection string.
- `DATABASE_NAME` — Database name (defaults to `VESTRA_PROD`).
- `MARKET_DATA_PROVIDER` — Market data provider module path (defaults to `market.yfinance_provider.YFinanceProvider`).

---

### `batch/get_instruments_timeseries.py`

**Purpose:** Fetch 12-month price timeseries for instruments and store them back into the `instruments` collection.

**Key exports:**

| Symbol | Type | Description |
|--------|------|-------------|
| `fetch_instruments(db_name)` | `function` | Fetch all instruments from `VESTRA_PROD.instruments`. |
| `_upsert_instrument_timeseries(db_name, documents)` | `function` | Upsert instrument documents with timeseries into `VESTRA_PROD.instruments`. |

**Timeseries format:**

- `timeseries` field contains a list of `{"date": str, "price": float}` dicts sorted ascending by date.
- Only documents that successfully return history are updated; failures are logged but do not abort the run.

**Environment variables:**
- `MONGODB_SRV` — MongoDB connection string.
- `DATABASE_NAME` — Database name (defaults to `VESTRA_PROD`).
- `MARKET_DATA_PROVIDER` — Market data provider module path (defaults to `market.yfinance_provider.YFinanceProvider`).

---

### `batch/get_portfolios_nav.py`

**Purpose:** Calculate portfolio NAV time series over the last 12 months. For each portfolio, reads `holdings`, matches to instruments via `securityCode.marketCode_yf` → `symbol`, multiplies `totalHolding` by instrument daily price, and stores the resulting `nav_timeseries` back onto the portfolio document.

**Key exports:**

| Symbol | Type | Description |
|--------|------|-------------|
| `fetch_portfolios(db_name)` | `function` | Fetch all portfolio documents from `VESTRA_PROD.portfolios`. |
| `fetch_instruments(db_name, symbols)` | `function` | Fetch instrument documents by symbol list from `VESTRA_PROD.instruments`. |
| `_build_instrument_price_map(instruments)` | `function` | Build a date-keyed price map from instrument timeseries. |
| `_smooth_price_map(price_map)` | `function` | Fill missing prices using last known price. |
| `_calculate_nav_timeseries(holdings, price_map)` | `function` | Calculate NAV timeseries from holdings and price map. |
| `_update_portfolio_nav(db_name, portfolio, nav_timeseries)` | `function` | Update portfolio document with `nav_timeseries`. |

**NAV calculation:**

- For each holding, multiplies `totalHolding` by the instrument's daily price (from `timeseries`).
- Sums across all holdings to produce a per-date NAV.
- Prices are smoothed using last known price for missing dates.

**Environment variables:**
- `MONGODB_SRV` — MongoDB connection string.
- `DATABASE_NAME` — Database name (defaults to `VESTRA_PROD`).

---

### `batch/price_streamer.py`

**Purpose:** Poll current prices from a market data provider and update MongoDB. Intended to run as a scheduled or long-lived process.

**Key exports:**

| Symbol | Type | Description |
|--------|------|-------------|
| `load_symbols(db_name)` | `function` | Return all unique instrument symbols from the `instruments` collection. |
| `create_price_updater(db_name)` | `function` | Return a callback that upserts `currentPrice` into MongoDB. |
| `get_market_data_provider()` | `function` | Instantiate and return the configured market data provider. |
| `get_poll_interval()` | `function` | Return the polling interval in seconds from environment. |

**Execution:**

- Loads all instrument symbols from the `instruments` collection.
- Fetches current prices from the configured market data provider on a polling interval.
- Updates each document's `currentPrice` field.

**Environment variables:**
- `MONGODB_SRV` — MongoDB connection string.
- `DATABASE_NAME` — Database name (defaults to `VESTRA_PROD`).
- `MARKET_DATA_PROVIDER` — Market data provider module path (defaults to `market.yfinance_provider.YFinanceProvider`).
- `PRICE_POLL_INTERVAL_SECONDS` — Seconds between price polls (defaults to `60`, minimum `10`).

---

### `market/provider.py`

**Purpose:** Abstraction layer for market data providers. Defines the `MarketDataProvider` interface, which decouples the rest of the pipeline from any specific market data vendor.

**Key exports:**

| Symbol | Type | Description |
|--------|------|-------------|
| `MarketDataProvider` | `ABC` | Abstract base class defining the market data provider interface. |

**Interface methods:**

- `fetch_instrument_metadata(symbol)` — Fetch static metadata for a single instrument.
- `fetch_current_prices(symbols)` — Fetch current prices for multiple symbols.
- `fetch_instrument_timeseries(symbol, period)` — Fetch historical timeseries data for a single instrument.

---

### `market/yfinance_provider.py`

**Purpose:** Yahoo Finance implementation of the market data provider abstraction.

**Key exports:**

| Symbol | Type | Description |
|--------|------|-------------|
| `YFinanceProvider` | `class` | Fetch instrument metadata and current prices from Yahoo Finance. |

**Implementation details:**

- Uses `yfinance` library to fetch data.
- `fetch_instrument_metadata()` returns `sector`, `industry`, `dividend_yield`, and `currentPrice`.
- `fetch_current_prices()` uses `yf.download()` with `period="1d"`.
- `fetch_instrument_timeseries()` uses `yf.Ticker.history()` with `auto_adjust=True`.

---

### `email/send_email.py`

**Purpose:** Send branded transactional emails via the Brevo SMTP transactional API.

**Key exports:**

| Symbol | Type | Description |
|--------|------|-------------|
| `EMAIL_HEADER` | `str` | HTML header fragment with Vestra Capital branding. |
| `EMAIL_FOOTER` | `str` | HTML footer fragment with contact details and legal links. |
| `send_email(options)` | `function` | Send an email. Wraps `options['message']` with header/footer. |

**Required options dict:**

```python
{
    'email': 'recipient@example.com',   # Recipient address
    'subject': 'Email subject',          # Subject line
    'message': '<p>HTML body</p>',       # HTML content between header and footer
}
```

**Environment variables:**
- `BREVO_API_KEY` — Brevo API key.
- `BREVO_EMAIL_SENDER` — Verified sender email address.

**Brevo endpoint:** `POST https://api.brevo.com/v3/smtp/email`

---

### `scripts/send_pending_prospects_email.py`

**Purpose:** Query the MongoDB `prospects` collection for documents with status `"Pending"` (case-insensitive) and send a branded HTML table email via `email/send_email.py`.

**Key exports:**

| Symbol | Type | Description |
|--------|------|-------------|
| `send_pending_prospects_email(recipient)` | `function` | Fetch pending prospects, build an HTML table, and send a branded email. |
| `fetch_pending_prospects()` | `function` | Query the `prospects` collection for documents matching `status: "Pending"`. |
| `build_html_table(prospects)` | `function` | Build an HTML table string from a list of prospect dicts. |

**Required environment variables:**
- `MONGODB_SRV` — MongoDB connection string.
- `DATABASE_NAME` — Database name (defaults to `VESTRA_PROD`).
- `COLLECTION_NAME` — Collection name (defaults to `prospects`).
- `BREVO_API_KEY` — Brevo API key.
- `BREVO_EMAIL_SENDER` — Verified Brevo sender address.
- `NOTIFICATION_EMAIL` — Recipient address for the pending-prospects report (optional; currently defaults to `daiviet@vestracapital.com.au`; can also be passed as the `recipient` argument to `send_pending_prospects_email()`).

**Email table columns:**
- `first_name`
- `last_name`
- `email`
- `telephone`
- `preferredTopic`

---

### `test/test_send_email.py`

**Purpose:** Manual integration test for the Brevo email utility.

Run:
```bash
python test/test_send_email.py
```

This sends a test email to `daiviet@vestracapital.com.au`. Change the recipient in the script before running.

---

## Setup

### Prerequisites

- **Python:** 3.9 or higher
- **Package manager:** `pip`
- **MongoDB:** MongoDB Atlas cluster or compatible instance with network access from your deployment environment
- **Morrison Securities:** Valid API credentials (`MORRISON_ACCESS_KEY`)
- **Brevo:** Valid API key (`BREVO_API_KEY`) and verified sender email

### Installation

```bash
# 1. Clone the repository
git clone <repository-url>
cd VES-data-collection-processing

# 2. Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # macOS/Linux
# OR
venv\Scripts\activate     # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env   # if .env.example exists; otherwise create .env manually
# Edit .env with your credentials (see Configuration section below)
```

### Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `requests` | `>=2.31.0` | HTTP client for Brevo email API |
| `python-dotenv` | `>=1.0.0` | Load environment variables from `.env` |
| `pymongo` | `>=4.0.0` | MongoDB driver for document upsert |

---

## Configuration

All configuration is managed via environment variables loaded from `.env` in the project root.

### `.env` Reference

```env
# ===========================================
# Morrison Securities API
# ===========================================
MORRISON_API_BASE_URL=https://api.morrison.fortrez.com.au
MORRISON_ACCESS_KEY=sk_...

# ===========================================
# MongoDB
# ===========================================
MONGODB_SRV=mongodb+srv://user:pass@cluster0.mongodb.net/?retryWrites=true&w=majority
DATABASE_NAME=VESTRA_PROD

# ===========================================
# Market Data Provider
# ===========================================
MARKET_DATA_PROVIDER=market.yfinance_provider.YFinanceProvider
PRICE_POLL_INTERVAL_SECONDS=60

# ===========================================
# Fees Reporting
# ===========================================
FEES_REPORT_RATE=1.5

# ===========================================
# Brevo Transactional Email
# ===========================================
BREVO_API_KEY=xkeysib-...
BREVO_EMAIL_SENDER=team@vestracapital.com.au

# ===========================================
# Pending Prospects Email (optional)
# ===========================================
NOTIFICATION_EMAIL=daiviet@vestracapital.com.au
COLLECTION_NAME=prospects
```

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MORRISON_API_BASE_URL` | No | `https://api.morrisonsecurities.com/backoffice` | Base URL for Morrison Securities APIs. Override for staging or regional endpoints. |
| `MORRISON_ACCESS_KEY` | **Yes** | — | API key for Morrison Securities authentication. |
| `MONGODB_SRV` | **Yes** | — | Full MongoDB connection string (SRV format recommended). |
| `DATABASE_NAME` | No | `VESTRA_PROD` | Target MongoDB database name. |
| `MARKET_DATA_PROVIDER` | No | `market.yfinance_provider.YFinanceProvider` | Dotted import path for the market data provider class. |
| `PRICE_POLL_INTERVAL_SECONDS` | No | `60` | Seconds between price polls for `batch/price_streamer.py`. Minimum is 10. |
| `FEES_REPORT_RATE` | No | `1.5` | Annual fee rate as a percentage for fees reporting. |
| `COLLECTION_NAME` | No | `prospects` | Target MongoDB collection name for the pending-prospects script. |
| `BREVO_API_KEY` | **Yes** | — | Brevo API key for transactional email. |
| `BREVO_EMAIL_SENDER` | **Yes** | — | Verified sender email address for Brevo. |
| `NOTIFICATION_EMAIL` | No | `daiviet@vestracapital.com.au` | Recipient address for the pending-prospects report. Can also be passed as the `recipient` argument to `send_pending_prospects_email()`. |

> **Security note:** Never commit `.env` to version control. It is listed in `.gitignore`.

---

## Usage

### 1. Fetch and Upsert Trading Accounts

This is the primary pipeline. It discovers adviser scopes, fetches trading accounts for each, enriches them, and upserts into MongoDB.

```bash
python data/001_get_trading_account.py
```

**What happens:**

1. `fetch_data()` calls `GET /dataaccess/v1` to retrieve adviser/branch/organisation scope items.
2. For each unique `adviserCode`, the script makes one API call to `/tradingaccounts/v2` with `includeInactive=false` to fetch active accounts only.
3. Each response is normalized via `_normalize_to_documents()`, which unwraps the `Data` envelope.
4. Each account document is enriched via `_enrich_client_document()`.
5. Documents are upserted into `VESTRA_PROD.clients` by `accountNumber`.

**Sample output:**

```
Requesting: https://api.morrison.fortrez.com.au/tradingaccounts/v2?organisationCode=TPSSCS&branchCode=SO&adviserCode=VO2&includeInactive=false

--- Result for adviserCode=VO2 includeInactive=False ---
{
  "RequestID": "...",
  "Type": "TradingAccountsV2Response",
  "Success": true,
  "Data": [ ... ]
}

Upserted 14 document(s) into 'VESTRA_PROD.clients'.
```

### 2. Send Pending Prospects Report

```bash
python scripts/send_pending_prospects_email.py
```

**What happens:**
1. Queries the `prospects` collection for documents with `status` equal to `"Pending"` (case-insensitive).
2. Builds an HTML table containing `first_name`, `last_name`, `email`, `telephone`, and `preferredTopic`.
3. Sends a branded email to the address configured in `NOTIFICATION_EMAIL` (or the `recipient` argument if provided).

### 3. Fetch and Upsert Portfolio Holdings

```bash
python data/002_get_portfolio_holdings.py
```

**What happens:**

1. Reads the `clients` collection from MongoDB to get active account numbers.
2. For each account, calls `GET /equityholdings/v1` with `includeZeroHoldings=false`.
3. Each response is normalized via `_normalize_holdings_documents()`, which unwraps the response envelope.
4. Each holding document is normalized via `_normalize_holding_document()`.
5. Documents are grouped by `accountNumber` and upserted into `VESTRA_PROD.portfolios`.
6. Accounts with empty holdings or accounts no longer present in the `clients` collection are removed from `portfolios`.

**Sample output:**

```
Requesting: https://api.morrison.fortrez.com.au/equityholdings/v1?organisationCode=TPSSCS&branchCode=SO&adviserCode=VO2&accountNumber=12345&includeZeroHoldings=false

--- Result for adviserCode=VO2 accountNumber=12345 includeZeroHoldings=False ---
{
  "RequestID": "...",
  "Type": "EquityHoldingsV1Response",
  "Success": true,
  "Data": [ ... ]
}

Upserted 1 document(s) into 'VESTRA_PROD.portfolios'.
Removed 0 inactive/empty portfolio document(s).
```

### 4. Fetch Cash Balances

```bash
python data/003_get_cash_balances.py
```

**What happens:**

1. Reads the `clients` collection from MongoDB to get account numbers.
2. For each account, calls `GET /cashbalances/v1` with scoping parameters.
3. Each response is normalized via `_normalize_cash_documents()`, which unwraps the response envelope.
4. Cash balance documents are upserted into `VESTRA_PROD.portfolios` by `accountNumber` in the `cash` field.

### 5. Fetch Trades History

```bash
python data/004_get_trades_history.py
```

**What happens:**

1. Reads the `clients` collection from MongoDB to get active account numbers.
2. For each account, calls `GET /equityholdingtransactions/v1` with scoping parameters.
3. Each response is normalized via `_normalize_trades_documents()`, which unwraps the response envelope.
4. Trades are enriched with realised P&L using FIFO lot matching via `_enrich_trades_with_pnl()`.
5. Trades are upserted into `VESTRA_PROD.trades` by `accountNumber`.
6. Trades for inactive accounts are purged from the collection.

### 6. Enrich Instruments

```bash
python batch/get_instruments.py
```

**What happens:**

1. Reads the `portfolios` collection and extracts unique instruments from holdings.
2. Checks the `instruments` collection for documents missing `sector`, `industry`, `dividend_yield`, or `currentPrice`.
3. For each instrument missing metadata, fetches it from the configured market data provider.
4. Upserts enriched instrument documents into `VESTRA_PROD.instruments`.

### 7. Fetch Instruments Timeseries

```bash
python batch/get_instruments_timeseries.py
```

**What happens:**

1. Reads all instruments from `VESTRA_PROD.instruments`.
2. For each instrument, fetches 12-month historical price timeseries from the configured market data provider.
3. Upserts the `timeseries` array onto each instrument document.

### 8. Calculate Portfolio NAV

```bash
python batch/get_portfolios_nav.py
```

**What happens:**

1. Reads all portfolio documents from `VESTRA_PROD.portfolios`.
2. Reads matching instruments from `VESTRA_PROD.instruments` to obtain price timeseries.
3. For each portfolio, calculates NAV by multiplying `totalHolding` by instrument daily price and summing across holdings.
4. Stores the resulting `nav_timeseries` back onto each portfolio document.

### 9. Generate Fees Reporting Data

```bash
python data/005_get_data_for_fees_reporting.py
```

**What happens:**

1. Reads `clients`, `portfolios`, and `instruments` collections.
2. For each client, builds an AUM snapshot:
   - `total_holdings` from latest `nav_timeseries` entry.
   - `total_cash` from portfolio `cash.bankAccounts`.
   - `total_aum = total_holdings + total_cash`.
   - `collected_fees = (total_aum * rate / 100) / days_in_year`.
   - `totalPnl` from holdings using current price, timeseries, and market price fallbacks.
3. Upserts AUM documents into `VESTRA_PROD.fees` keyed by `accountNumber` and `generatedAt`.
4. Backfills missing `feeReportRate`, `collectedFees`, and `totalPnl` fields.
5. Prints a daily fee report.

### 10. Calculate Portfolio Risk Scores

```bash
python data/006_get_risk_score.py
```

**What happens:**

1. Fetches all portfolio documents from `VESTRA_PROD.portfolios`.
2. For each portfolio, extracts `nav_timeseries` and `holdings`.
3. Fetches matching instruments from `VESTRA_PROD.instruments` to obtain sector information.
4. Calculates:
   - `riskScore` (1–10) based on volatility (60%) and diversification (40%).
   - `riskLabel` — human-readable category mapped from `riskScore`.
   - `maxDrawdown`, `maxWeeklyDrawdown`, `maxYearlyDrawdown` from NAV timeseries.
5. Upserts the calculated metrics into each portfolio document by `accountNumber`.

**Sample output:**

```
Updated metrics for accountNumber=12345: riskScore=4, riskLabel=Moderate, maxDrawdown=0.15, maxWeeklyDrawdown=0.05, maxYearlyDrawdown=0.22
```

### 11. Run Batch Data Collection

```bash
python batch/data_collection.py
```

**What happens:**

- Runs `001_get_trading_account.py`, `002_get_portfolio_holdings.py`, `003_get_cash_balances.py`, and `004_get_trades_history.py` sequentially.
- Stops at the first failure and returns the offending exit code.

### 12. Run Batch Fees Reporting

```bash
python batch/fees_reporting.py
```

**What happens:**

- Runs the full fees reporting pipeline sequentially: trading accounts, portfolio holdings, cash balances, instrument enrichment, instrument timeseries, portfolio NAV, and AUM generation.
- Stops at the first failure and returns the offending exit code.

### 13. Start Price Streamer

```bash
python batch/price_streamer.py
```

**What happens:**

- Loads all instrument symbols from `VESTRA_PROD.instruments`.
- Polls current prices from the configured market data provider at the configured interval.
- Updates each instrument's `currentPrice` field in MongoDB.
- Runs until interrupted (Ctrl+C).

**Environment variables:**
- `PRICE_POLL_INTERVAL_SECONDS` — Seconds between price polls (defaults to `60`, minimum `10`).

---

### 14. Send a Test Email

```bash
python test/test_send_email.py
```

**What happens:**
1. Constructs a test email payload.
2. Calls `send_email()` from `email/send_email.py`.
3. The message is wrapped with the Vestra Capital branded header and footer.
4. The email is submitted to Brevo's SMTP transactional endpoint.

---

---

## Error Handling

The pipeline uses explicit error handling with rich diagnostic context:

| Scenario | Raised Exception | Diagnostic Context |
|----------|------------------|-------------------|
| Missing `MORRISON_ACCESS_KEY` | `RuntimeError` | Environment variable name and `.env` hint |
| Empty API response body | `RuntimeError` | URL and "API returned an empty response" |
| Non-JSON response with 200 status | `RuntimeError` | Status code, Content-Type, URL, and 200-char response preview |
| HTTP error status | `RuntimeError` | HTTP status code, reason, and response body |
| Missing `MONGODB_SRV` | `RuntimeError` | Environment variable name and `.env` hint |
| MongoDB upsert failure | `RuntimeError` | Original `PyMongoError` chained as cause |
| Missing Brevo credentials | `ValueError` | Variable names and setup hint |
| Brevo API HTTP error | `requests.HTTPError` | Raised by `response.raise_for_status()` |

### Retry and Resilience

- **MongoDB:** Uses `replace_one(..., upsert=True)`, making the operation idempotent. Re-running the script is safe.
- **HTTP:** No automatic retries are configured. For production use, consider wrapping API calls with `urllib` retry logic or switching to `requests` with `urllib3` retry adapters.
- **Timeouts:** `urlopen` uses system defaults. For production, set explicit timeouts via `urlopen(request, timeout=30)`.

---

## Troubleshooting

### MongoDB Connection Issues

**Symptom:** `Failed to upsert documents into MongoDB: ...`

**Checks:**
1. Verify `MONGODB_SRV` is correctly set in `.env`.
2. Ensure your IP address is whitelisted in MongoDB Atlas.
3. Verify the database user has `readWrite` permissions on `VESTRA_PROD`.
4. Test the connection string with `mongosh` or Compass.

### Morrison Securities API Errors

**Symptom:** `API request failed: 401 Unauthorized` or `MORRISON_ACCESS_KEY is missing`

**Checks:**
1. Verify `MORRISON_ACCESS_KEY` is set in `.env`.
2. Verify `MORRISON_API_BASE_URL` points to the correct environment (staging vs production).
3. Check if the API key has expired or been revoked.

### Empty Response from Morrison API

**Symptom:** `API returned an empty response.`

**Checks:**
1. Verify the adviser scope returned by `fetch_data()` is valid.
2. Check if the `organisationCode` and `branchCode` are correct.
3. The API may return empty for adviser codes with no trading accounts; this is handled gracefully.

### Brevo Email Failures

**Symptom:** `BREVO_API_KEY and BREVO_EMAIL_SENDER must be set in environment variables.`

**Checks:**
1. Verify both variables are set in `.env`.
2. Verify `BREVO_EMAIL_SENDER` is a verified sender address in your Brevo dashboard.

---

## Testing

### Manual Testing

| Test | Command | Expected Outcome |
|------|---------|------------------|
| Fetch data access scope | `python -c "from data.configuration import fetch_data; print(fetch_data())"` | JSON with adviser/branch scope |
| Fetch trading accounts | `python data/001_get_trading_account.py` | Console output with account JSON and upsert count |
| Fetch portfolio holdings | `python data/002_get_portfolio_holdings.py` | Console output with holdings JSON and upsert count |
| Fetch cash balances | `python data/003_get_cash_balances.py` | Console output with cash balance JSON and upsert count |
| Fetch trades history | `python data/004_get_trades_history.py` | Console output with trades JSON and upsert count |
| Enrich instruments | `python batch/get_instruments.py` | Console output with enrichment count |
| Fetch instruments timeseries | `python batch/get_instruments_timeseries.py` | Console output with timeseries fetch count |
| Calculate portfolio NAV | `python batch/get_portfolios_nav.py` | Console output with NAV update count |
| Generate fees reporting data | `python data/005_get_data_for_fees_reporting.py` | Console output with AUM records and fee report |
| Calculate risk scores | `python data/006_get_risk_score.py` | Console output with riskScore, riskLabel, and drawdown metrics for each portfolio |
| Run batch data collection | `python batch/data_collection.py` | Sequential execution of data collection scripts |
| Run batch fees reporting | `python batch/fees_reporting.py` | Sequential execution of full fees reporting pipeline |
| Start price streamer | `python batch/price_streamer.py` | Continuous price polling until interrupted |
| Send pending prospects report | `python scripts/send_pending_prospects_email.py` | Email delivered with HTML table of pending prospects |
| Send test email | `python test/test_send_email.py` | Email delivered to test recipient |

### Recommended Automated Tests

For production use, consider adding:

- **Unit tests** for `_extract_first_last_name()`, `_extract_email()`, `_extract_telephone()`, `_normalize_to_documents()`, and `_enrich_client_document()`.
- **Unit tests** for `build_html_table()` and `fetch_pending_prospects()` in `scripts/send_pending_prospects_email.py`.
- **Unit tests** for `calculate_risk_score()`, `get_risk_label()`, `calculate_drawdown_metrics()`, and `_calculate_*` helpers in `data/006_get_risk_score.py`.
- **Unit tests** for `fetch_account_cash_balance()`, `upsert_cash_balances()`, and `_normalize_cash_documents()` in `data/003_get_cash_balances.py`.
- **Unit tests** for `fetch_equity_holding_transactions()`, `upsert_trades()`, `_normalize_trades_documents()`, and `_enrich_trades_with_pnl()` in `data/004_get_trades_history.py`.
- **Unit tests** for `_build_aum_document()`, `_calculate_account_pnl()`, and `insert_aum()` in `data/005_get_data_for_fees_reporting.py`.
- **Unit tests** for `extract_instruments()`, `upsert_instruments()`, and `_enrich_instruments()` in `batch/get_instruments.py`.
- **Unit tests** for `_calculate_nav_timeseries()` and `_smooth_price_map()` in `batch/get_portfolios_nav.py`.
- **Integration tests** with a test MongoDB database and mocked Morrison API responses.
- **Email tests** using `responses` or `requests-mock` to mock the Brevo API.

---

## Deployment Considerations

### Production Checklist

- [ ] Store `.env` securely using a secrets manager (e.g., AWS Secrets Manager, HashiCorp Vault) rather than plaintext files.
- [ ] Set `MORRISON_API_BASE_URL` explicitly to avoid relying on defaults.
- [ ] Configure MongoDB Atlas IP whitelist for your deployment environment.
- [ ] Enable MongoDB authentication and use a dedicated read/write user.
- [ ] Add HTTP timeouts to `urlopen` calls.
- [ ] Consider adding retry logic with exponential backoff for transient API failures.
- [ ] Rotate `MORRISON_ACCESS_KEY` and `BREVO_API_KEY` regularly.
- [ ] Monitor Brevo sending limits and bounces in the Brevo dashboard.

### Scheduling

To run the pipeline on a schedule:

```bash
# Example crontab entry: run daily at 2 AM
0 2 * * * /path/to/venv/bin/python /path/to/VES-data-collection-processing/data/001_get_trading_account.py >> /var/log/ves-pipeline.log 2>&1

# Example crontab entry: run daily at 3 AM
0 3 * * * /path/to/venv/bin/python /path/to/VES-data-collection-processing/data/002_get_portfolio_holdings.py >> /var/log/ves-portfolios.log 2>&1

# Example crontab entry: run daily at 3:30 AM
0 3 * * * /path/to/venv/bin/python /path/to/VES-data-collection-processing/data/003_get_cash_balances.py >> /var/log/ves-cash.log 2>&1

# Example crontab entry: run daily at 4 AM
0 4 * * * /path/to/venv/bin/python /path/to/VES-data-collection-processing/data/004_get_trades_history.py >> /var/log/ves-trades.log 2>&1

# Example crontab entry: enrich instruments daily at 4:30 AM
0 4 * * * /path/to/venv/bin/python /path/to/VES-data-collection-processing/batch/get_instruments.py >> /var/log/ves-instruments.log 2>&1

# Example crontab entry: fetch instruments timeseries daily at 5 AM
0 5 * * * /path/to/venv/bin/python /path/to/VES-data-collection-processing/batch/get_instruments_timeseries.py >> /var/log/ves-instruments-ts.log 2>&1

# Example crontab entry: calculate portfolio NAV daily at 5:30 AM
0 5 * * * /path/to/venv/bin/python /path/to/VES-data-collection-processing/batch/get_portfolios_nav.py >> /var/log/ves-nav.log 2>&1

# Example crontab entry: generate fees reporting data daily at 6 AM
0 6 * * * /path/to/venv/bin/python /path/to/VES-data-collection-processing/data/005_get_data_for_fees_reporting.py >> /var/log/ves-fees.log 2>&1

# Example crontab entry: calculate portfolio risk scores daily at 6:30 AM
0 6 * * * /path/to/venv/bin/python /path/to/VES-data-collection-processing/data/006_get_risk_score.py >> /var/log/ves-risk.log 2>&1

# Example crontab entry: send pending prospects report daily at 8 AM
0 8 * * * /path/to/venv/bin/python /path/to/VES-data-collection-processing/scripts/send_pending_prospects_email.py >> /var/log/ves-prospects.log 2>&1
```

Or use a workflow orchestrator such as:
- **Apache Airflow** — for complex dependency management and SLA monitoring.
- **Prefect** — for modern Python-native orchestration.
- **GitHub Actions** — for scheduled CI runs.

---

## License

Proprietary — Vestra Capital
