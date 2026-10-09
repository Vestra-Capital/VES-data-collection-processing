"""Batch script to update NAV timeseries and risk metrics for portfolios missing them.

Queries the ``portfolios`` collection for documents where ``nav_timeseries`` is
missing or empty, then for each:

1. Ensures all instruments referenced by portfolio holdings exist in the
   ``instruments`` collection with metadata and historical ``timeseries``.
2. Calculates ``nav_timeseries`` from ``holdings`` and instrument ``timeseries``.
3. Stores the computed ``nav_timeseries`` back to the portfolio.
4. Calculates ``riskScore``, ``riskLabel``, and drawdown metrics from the new
   ``nav_timeseries`` and ``holdings``.
5. Stores the risk metrics back to the portfolio.

This module is intended to run as a long-lived daemon. It polls the
``portfolios`` collection on a configurable interval and updates any documents
missing ``nav_timeseries`` or risk metrics. Before calculating NAV, it verifies
that all required instruments are present in the ``instruments`` collection
with metadata and timeseries, enriching them via the configured market data
provider if necessary.

Environment variables:
    MONGODB_SRV — MongoDB connection string.
    DATABASE_NAME — Database name (defaults to ``VESTRA_PROD``).
    UPDATE_PORTFOLIOS_POLL_INTERVAL_SECONDS — Seconds between polling cycles
        (defaults to ``300``, minimum ``60``).
    MARKET_DATA_PROVIDER — Market data provider module path
        (defaults to ``market.yfinance_provider.YFinanceProvider``).
"""

import importlib
import logging
import os
import sys
import time
from collections import OrderedDict
from datetime import date, datetime
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

logger = logging.getLogger(__name__)

SYDNEY_TZ = ZoneInfo("Australia/Sydney")
MARKET_VALUE_UPDATE_HOUR = 17
_last_market_value_update_date: date | None = None


def _get_market_value_update_state_collection(db_name: str):
    client = get_mongo_client()
    try:
        db = client[db_name]
        return db["batch_state"]
    except PyMongoError as e:
        raise RuntimeError(f"Failed to access batch_state collection: {e}") from e


def _load_last_market_value_update_date(db_name: str) -> date | None:
    collection = _get_market_value_update_state_collection(db_name)
    doc = collection.find_one({"_id": "last_market_value_update"})
    if not doc:
        return None
    raw = doc.get("date")
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except (TypeError, ValueError):
        return None


def _save_market_value_update_date(db_name: str, update_date: date) -> None:
    collection = _get_market_value_update_state_collection(db_name)
    collection.update_one(
        {"_id": "last_market_value_update"},
        {"$set": {"date": update_date.isoformat()}},
        upsert=True,
    )


def get_mongo_client() -> MongoClient:
    mongo_srv = os.getenv("MONGODB_SRV")
    if not mongo_srv:
        raise RuntimeError("MONGODB_SRV is missing. Check your .env file.")
    return MongoClient(mongo_srv)


def _get_excluded_categories() -> List[str]:
    raw = os.getenv("EXCLUDED_CLIENT_CATEGORIES", "Wealth Management, Brokerage, Inactive")
    return [cat.strip() for cat in raw.split(",") if cat.strip()]


def _should_run_market_value_update(db_name: str) -> bool:
    global _last_market_value_update_date
    now_sydney = datetime.now(SYDNEY_TZ)
    today = now_sydney.date()

    if _last_market_value_update_date is None:
        _last_market_value_update_date = _load_last_market_value_update_date(db_name)

    if _last_market_value_update_date == today:
        return False

    return now_sydney.hour >= MARKET_VALUE_UPDATE_HOUR


def _run_daily_market_value_update(db_name: str, excluded_categories: List[str]) -> None:
    from batch.get_portfolios_nav import (
        _build_instrument_map,
        _build_instrument_price_map,
        _enrich_holdings,
        _get_latest_price_map,
        _smooth_price_map,
        _update_holdings_market_values,
        fetch_instruments,
        fetch_portfolios_with_client_exclusion,
    )

    portfolios = fetch_portfolios_with_client_exclusion(db_name, excluded_categories)
    if not portfolios:
        logger.info("No portfolios found for daily market value update.")
        return

    all_symbols: set = set()
    portfolio_data: List[tuple] = []

    for portfolio in portfolios:
        holdings = portfolio.get("holdings") or []
        if not isinstance(holdings, list):
            portfolio_data.append((portfolio, []))
            continue

        symbols = _extract_symbols_from_holdings(holdings)
        all_symbols.update(symbols)
        portfolio_data.append((portfolio, holdings))

    if not all_symbols:
        logger.info("No symbols found for daily market value update.")
        return

    _ensure_instruments_and_timeseries(db_name, list(all_symbols), portfolio_data)

    instruments = fetch_instruments(db_name, list(all_symbols))
    price_map = _smooth_price_map(_build_instrument_price_map(instruments))
    instrument_map = _build_instrument_map(instruments)
    latest_price_map = _get_latest_price_map(price_map)

    updated = 0
    failed: List[str] = []

    for portfolio, holdings in portfolio_data:
        account_number = portfolio.get("accountNumber", "N/A")
        if not account_number:
            continue
        try:
            _update_holdings_market_values(
                db_name, account_number, holdings, instrument_map, latest_price_map
            )
            logger.info("Daily market value update for accountNumber=%s.", account_number)
            updated += 1
        except RuntimeError as e:
            logger.error(
                "Daily market value update failed for accountNumber=%s: %s",
                account_number,
                e,
            )
            failed.append(str(account_number))

    logger.info(
        "Daily market value update summary: %d updated, %d failed: %s",
        updated,
        len(failed),
        failed,
    )


def fetch_portfolios_missing_nav(db_name: str) -> List[Dict[str, Any]]:
    client = get_mongo_client()
    try:
        db = client[db_name]
        excluded_categories = _get_excluded_categories()

        clients_collection = db["clients"]
        cursor = clients_collection.find(
            {"$nor": [{"client_category": cat} for cat in excluded_categories]},
            {"accountNumber": 1, "_id": 0},
        )
        allowed_account_numbers = {
            str(doc.get("accountNumber", ""))
            for doc in cursor
            if isinstance(doc, dict) and doc.get("accountNumber") is not None
        }

        collection = db["portfolios"]
        cursor = collection.find(
            {
                "accountNumber": {"$in": list(allowed_account_numbers)},
                "$or": [
                    {"nav_timeseries": {"$exists": False}},
                    {"nav_timeseries": []},
                ],
            }
        )
        return [doc for doc in cursor if isinstance(doc, dict)]
    except PyMongoError as e:
        raise RuntimeError(f"Failed to fetch portfolios from MongoDB: {e}") from e
    finally:
        client.close()


from batch.get_instruments import (
    _fetch_instruments_missing_metadata,
    _enrich_instruments,
    upsert_instruments,
)
from batch.get_instruments_timeseries import _upsert_instrument_timeseries as _upsert_instrument_timeseries_ts
from batch.get_portfolios_nav import (
    _build_instrument_price_map,
    _build_instrument_map,
    _calculate_nav_timeseries,
    _calculate_portfolio_values,
    _enrich_holdings,
    _get_latest_price_map,
    _smooth_price_map,
    _update_portfolio_nav,
    _update_holdings_market_values,
    fetch_instruments,
    fetch_portfolios_with_client_exclusion,
)

_risk_module = importlib.import_module("data.006_get_risk_score")
calculate_risk_score = _risk_module.calculate_risk_score
get_risk_label = _risk_module.get_risk_label
calculate_drawdown_metrics = _risk_module.calculate_drawdown_metrics
update_portfolio_metrics = _risk_module.update_portfolio_metrics
_build_symbol_sector_map = _risk_module._build_symbol_sector_map


def _get_market_data_provider() -> Any:
    import importlib

    provider_path = os.getenv(
        "MARKET_DATA_PROVIDER",
        "market.yfinance_provider.YFinanceProvider",
    )
    module_path, class_name = provider_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    provider_class = getattr(module, class_name)
    return provider_class()


def _extract_symbols_from_holdings(holdings: List[Dict[str, Any]]) -> List[str]:
    symbols: List[str] = []
    for holding in holdings:
        if not isinstance(holding, dict):
            continue
        security_code = holding.get("securityCode")
        market_code_yf = holding.get("marketCode_yf")
        if security_code and market_code_yf:
            symbols.append(f"{security_code}.{market_code_yf}")
    return symbols


def _build_instrument_docs_from_holdings(
    portfolio_data: List[tuple],
) -> List[Dict[str, Any]]:
    seen: Dict[str, Dict[str, Any]] = OrderedDict()
    for _, holdings in portfolio_data:
        if not isinstance(holdings, list):
            continue
        for holding in holdings:
            if not isinstance(holding, dict):
                continue
            security_code = holding.get("securityCode")
            market_code_yf = holding.get("marketCode_yf")
            if not security_code or not market_code_yf:
                continue
            symbol = f"{security_code}.{market_code_yf}"
            if symbol in seen:
                continue
            seen[symbol] = {
                "symbol": symbol,
                "securityCode": security_code,
                "marketCode_yf": market_code_yf,
                "securityDescription": holding.get("securityDescription", ""),
                "currency": holding.get("currency", ""),
            }
    return list(seen.values())


def _ensure_instruments_and_timeseries(
    db_name: str,
    symbols: List[str],
    portfolio_data: List[tuple],
) -> None:
    if not symbols:
        return

    existing_docs = _fetch_instruments_missing_metadata(db_name, symbols)
    existing_missing_symbols = set(existing_docs.keys())

    all_existing = fetch_instruments(db_name, symbols)
    present_symbols = {doc.get("symbol") for doc in all_existing if doc.get("symbol")}
    completely_missing = [s for s in symbols if s not in present_symbols]

    if not completely_missing and not existing_missing_symbols:
        logger.info("All required instruments already present with metadata.")
    else:
        provider = _get_market_data_provider()
        logger.info("Using market data provider: %s", provider.__class__.__name__)

        if completely_missing:
            base_docs = _build_instrument_docs_from_holdings(portfolio_data)
            docs_to_insert = [
                doc for doc in base_docs if doc["symbol"] in completely_missing
            ]
            if docs_to_insert:
                enriched = []
                for doc in docs_to_insert:
                    metadata = provider.fetch_instrument_metadata(doc["symbol"])
                    if metadata:
                        enriched.append({**doc, **metadata})
                if enriched:
                    upsert_instruments(db_name, enriched)
                    logger.info("Upserted %d new instrument(s) with metadata.", len(enriched))

        if existing_missing_symbols:
            base_docs = _build_instrument_docs_from_holdings(portfolio_data)
            enriched_existing = _enrich_instruments(provider, base_docs, existing_docs)
            if enriched_existing:
                upsert_instruments(db_name, enriched_existing)
                logger.info(
                    "Enriched %d existing instrument(s) with missing metadata.",
                    len(enriched_existing),
                )

    all_instruments = fetch_instruments(db_name, symbols)
    missing_timeseries = [
        doc for doc in all_instruments if doc.get("symbol") and not doc.get("timeseries")
    ]

    if missing_timeseries:
        provider = _get_market_data_provider()
        enriched_ts = []
        for doc in missing_timeseries:
            symbol = doc["symbol"]
            try:
                timeseries = provider.fetch_instrument_timeseries(symbol, period="1y")
                if timeseries:
                    updated = dict(doc)
                    updated["timeseries"] = timeseries
                    enriched_ts.append(updated)
            except Exception as exc:
                logger.warning(
                    "Failed to fetch timeseries for symbol=%s: %s", symbol, exc
                )

        if enriched_ts:
            _upsert_instrument_timeseries_ts(db_name, enriched_ts)
            logger.info(
                "Upserted %d instrument(s) with timeseries.", len(enriched_ts)
            )
    else:
        logger.info("All required instruments already have timeseries.")


def get_poll_interval() -> int:
    """Return the polling interval in seconds from environment."""
    value = os.getenv("UPDATE_PORTFOLIOS_POLL_INTERVAL_SECONDS", "300")
    try:
        interval = int(value)
        return max(interval, 60)
    except (TypeError, ValueError):
        return 300


def run_once(db_name: str) -> None:
    """Run one polling cycle: find portfolios missing NAV and update them."""
    if _should_run_market_value_update(db_name):
        excluded_categories = _get_excluded_categories()
        _run_daily_market_value_update(db_name, excluded_categories)
        global _last_market_value_update_date
        _last_market_value_update_date = datetime.now(SYDNEY_TZ).date()
        _save_market_value_update_date(db_name, _last_market_value_update_date)

    logger.info("Fetching portfolios missing nav_timeseries from '%s.portfolios'...", db_name)
    portfolios = fetch_portfolios_missing_nav(db_name)
    logger.info("Found %d portfolio(s) missing nav_timeseries.", len(portfolios))

    if not portfolios:
        logger.info("Nothing to do this cycle.")
        return

    all_symbols: set = set()
    portfolio_data: List[tuple] = []

    for portfolio in portfolios:
        holdings = portfolio.get("holdings") or []
        if not isinstance(holdings, list):
            portfolio_data.append((portfolio, []))
            continue

        symbols = _extract_symbols_from_holdings(holdings)
        all_symbols.update(symbols)
        portfolio_data.append((portfolio, holdings))

    logger.info("Found %d unique instrument symbol(s).", len(all_symbols))

    price_map: Dict[str, Dict[str, float]] = {}
    sector_map: Dict[str, str] = {}

    if all_symbols:
        _ensure_instruments_and_timeseries(db_name, list(all_symbols), portfolio_data)

        logger.info(
            "Fetching instruments for %d symbol(s) from '%s.instruments'...",
            len(all_symbols),
            db_name,
        )
        instruments = fetch_instruments(db_name, list(all_symbols))
        logger.info("Fetched %d instrument(s).", len(instruments))
        price_map = _smooth_price_map(_build_instrument_price_map(instruments))
        instrument_map = _build_instrument_map(instruments)
        sector_map = _build_symbol_sector_map(instruments)

    updated_nav = 0
    updated_risk = 0
    failed: List[str] = []

    for portfolio, holdings in portfolio_data:
        account_number = portfolio.get("accountNumber", "N/A")
        try:
            nav_timeseries = _calculate_nav_timeseries(holdings, price_map)
            if nav_timeseries:
                portfolio_values = _calculate_portfolio_values(holdings, instrument_map)
                latest_price_map = _get_latest_price_map(price_map)
                enriched_holdings = _enrich_holdings(holdings, instrument_map, latest_price_map)
                _update_portfolio_nav(db_name, portfolio, nav_timeseries, portfolio_values, enriched_holdings)
                logger.info(
                    "Updated NAV for accountNumber=%s: %d date(s).",
                    account_number,
                    len(nav_timeseries),
                )
                updated_nav += 1

                risk_score = calculate_risk_score(nav_timeseries, holdings, sector_map)
                risk_label = get_risk_label(risk_score)
                drawdown_metrics = calculate_drawdown_metrics(nav_timeseries)
                update_portfolio_metrics(db_name, account_number, risk_score, risk_label, drawdown_metrics)
                logger.info(
                    "Updated risk for accountNumber=%s: riskScore=%s, riskLabel=%s, "
                    "maxDrawdown=%s, maxWeeklyDrawdown=%s, maxYearlyDrawdown=%s",
                    account_number,
                    risk_score,
                    risk_label,
                    drawdown_metrics["maxDrawdown"],
                    drawdown_metrics["maxWeeklyDrawdown"],
                    drawdown_metrics["maxYearlyDrawdown"],
                )
                updated_risk += 1
            else:
                logger.info("No NAV data generated for accountNumber=%s; skipping risk.", account_number)
        except RuntimeError as e:
            logger.error("Failed for accountNumber=%s: %s", account_number, e)
            failed.append(str(account_number))

    logger.info(
        "Cycle summary: %d portfolio(s) NAV updated, %d risk updated, %d failed: %s",
        updated_nav,
        updated_risk,
        len(failed),
        failed,
    )


def main() -> None:
    db_name = os.getenv("DATABASE_NAME", "VESTRA_PROD")
    poll_interval = get_poll_interval()

    logger.info("Starting portfolio/risk metrics updater daemon.")
    logger.info("Database: %s", db_name)
    logger.info("Poll interval: %d seconds.", poll_interval)
    logger.info("Press Ctrl+C to stop.")

    while True:
        try:
            run_once(db_name)
        except KeyboardInterrupt:
            logger.info("Interrupted; shutting down.")
            break
        except Exception as e:
            logger.error("Poll cycle failed: %s", e, exc_info=True)

        try:
            time.sleep(poll_interval)
        except KeyboardInterrupt:
            logger.info("Interrupted; shutting down.")
            break


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
