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
- **Fetching** trading accounts for each adviser scope via the Morrison Securities Trading Accounts API (`/tradingaccounts/v2`), for both active and inactive records.
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
    A --> D[(MongoDB<br/>VESTRA_PROD.clients)]
    A --> E[Brevo SMTP API<br/>v3/smtp/email]
    A --> F[(MongoDB<br/>VESTRA_PROD.prospects)]
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
    end

    subgraph "External Services"
        MORRISON_CONFIG["Morrison Securities<br/>Data Access API"]
        MORRISON_TRADE["Morrison Securities<br/>Trading Accounts API"]
        MORRISON_HOLDINGS["Morrison Securities<br/>Equity Holdings API"]
        MONGO[(MongoDB Atlas<br/>VESTRA_PROD.clients)]
        PORTFOLIO_MONGO[(MongoDB Atlas<br/>VESTRA_PROD.portfolios)]
        PROSPECTS_MONGO[(MongoDB Atlas<br/>VESTRA_PROD.prospects)]
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
```

### Module Dependency Graph

```mermaid
graph TD
    MAIN["001_get_trading_account.py<br/>(__main__)"] --> CONFIG["configuration.py"]
    MAIN --> ENRICH["Enrichment Helpers<br/>(internal)"]
    MAIN --> MONGO["MongoDB Client<br/>(internal)"]
    HOLDINGS["002_get_portfolio_holdings.py<br/>(__main__)"] --> CONFIG
    HOLDINGS --> MONGO
    EMAIL["email/send_email.py"] --> DOTENV["python-dotenv"]
    CONFIG --> DOTENV
    MAIN --> DOTENV
    HOLDINGS --> DOTENV
    MAIN --> PYTHON_MONGO["pymongo"]
    HOLDINGS --> PYTHON_MONGO
    EMAIL --> REQUESTS["requests"]
    PROSPECTS["scripts/send_pending_prospects_email.py"] --> DOTENV
    PROSPECTS --> PYTHON_MONGO
    PROSPECTS --> EMAIL
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

**Purpose:** Fetch account equity holdings from the Morrison Securities Equity Holdings API (`/equityholdings/v1`) with `includeZeroHoldings=false`, normalise documents (add `marketCode_yf`, title-case `securityDescription`), and upsert into MongoDB `portfolios` collection. Inactive accounts and accounts with empty holdings are removed from the collection after each upsert.

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

### 4. Send a Test Email

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
| Send pending prospects report | `python scripts/send_pending_prospects_email.py` | Email delivered with HTML table of pending prospects |
| Send test email | `python test/test_send_email.py` | Email delivered to test recipient |

### Recommended Automated Tests

For production use, consider adding:

- **Unit tests** for `_extract_first_last_name()`, `_extract_email()`, `_extract_telephone()`, `_normalize_to_documents()`, and `_enrich_client_document()`.
- **Unit tests** for `build_html_table()` and `fetch_pending_prospects()` in `scripts/send_pending_prospects_email.py`.
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
