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
from typing import Any, Dict, List

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

logger = logging.getLogger(__name__)


def get_mongo_client() -> MongoClient:
    mongo_srv = os.getenv("MONGODB_SRV")
    if not mongo_srv:
        raise RuntimeError("MONGODB_SRV is missing. Check your .env file.")
    return MongoClient(mongo_srv)


def fetch_portfolios_missing_nav(db_name: str) -> List[Dict[str, Any]]:
    client = get_mongo_client()
    try:
        db = client[db_name]
        collection = db["portfolios"]
        cursor = collection.find(
            {"$or": [{"nav_timeseries": {"$exists": False}}, {"nav_timeseries": []}]}
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
    _calculate_nav_timeseries,
    _smooth_price_map,
    _update_portfolio_nav,
    fetch_instruments,
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
        sector_map = _build_symbol_sector_map(instruments)

    updated_nav = 0
    updated_risk = 0
    failed: List[str] = []

    for portfolio, holdings in portfolio_data:
        account_number = portfolio.get("accountNumber", "N/A")
        try:
            nav_timeseries = _calculate_nav_timeseries(holdings, price_map)
            if nav_timeseries:
                _update_portfolio_nav(db_name, portfolio, nav_timeseries)
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
