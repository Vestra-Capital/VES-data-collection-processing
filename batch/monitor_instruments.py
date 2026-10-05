"""Persistent daemon to monitor portfolios and add new instruments to the collection.

Polls the ``portfolios`` collection on a configurable interval, checks holdings
for instrument symbols that are missing from the ``instruments`` collection,
and upserts them with metadata fetched from the configured market data provider.

Only portfolios whose ``client_category`` is not ``"Inactive"`` are considered.

Environment variables:
    MONGODB_SRV — MongoDB connection string.
    DATABASE_NAME — Database name (defaults to ``VESTRA_PROD``).
    MONITOR_INSTRUMENTS_POLL_INTERVAL_SECONDS — Seconds between polling cycles
        (defaults to ``300``, minimum ``60``).
    MARKET_DATA_PROVIDER — Market data provider module path
        (defaults to ``market.yfinance_provider.YFinanceProvider``).
"""

import importlib
import logging
import os
import sys
import time
from typing import Any, Dict, List, Set

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


def _get_market_data_provider() -> Any:
    """Instantiate and return the configured market data provider."""
    provider_path = os.getenv(
        "MARKET_DATA_PROVIDER",
        "market.yfinance_provider.YFinanceProvider",
    )
    module_path, class_name = provider_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    provider_class = getattr(module, class_name)
    return provider_class()


def get_poll_interval() -> int:
    """Return the polling interval in seconds from environment."""
    value = os.getenv("MONITOR_INSTRUMENTS_POLL_INTERVAL_SECONDS", "300")
    try:
        interval = int(value)
        return max(interval, 60)
    except (TypeError, ValueError):
        return 300


def fetch_active_portfolios(db_name: str) -> List[Dict[str, Any]]:
    """Fetch portfolios for clients whose category is not 'Inactive'."""
    client = get_mongo_client()
    try:
        db = client[db_name]
        clients_collection = db["clients"]
        cursor = clients_collection.find(
            {"client_category": {"$ne": "Inactive"}},
            {"accountNumber": 1, "_id": 0},
        )
        allowed_account_numbers = {
            str(doc.get("accountNumber", ""))
            for doc in cursor
            if isinstance(doc, dict) and doc.get("accountNumber") is not None
        }

        collection = db["portfolios"]
        cursor = collection.find(
            {"accountNumber": {"$in": list(allowed_account_numbers)}}
        )
        return [doc for doc in cursor if isinstance(doc, dict)]
    except PyMongoError as e:
        raise RuntimeError(f"Failed to fetch active portfolios from MongoDB: {e}") from e
    finally:
        client.close()


def _extract_symbols_from_holdings(holdings: List[Dict[str, Any]]) -> List[str]:
    """Extract unique instrument symbols from portfolio holdings."""
    symbols: List[str] = []
    for holding in holdings:
        if not isinstance(holding, dict):
            continue
        security_code = holding.get("securityCode")
        market_code_yf = holding.get("marketCode_yf")
        if security_code and market_code_yf:
            symbols.append(f"{security_code}.{market_code_yf}")
    return symbols


def fetch_instruments(db_name: str, symbols: List[str]) -> List[Dict[str, Any]]:
    """Fetch instrument documents from MongoDB for the given symbols."""
    if not symbols:
        return []

    client = get_mongo_client()
    try:
        db = client[db_name]
        collection = db["instruments"]
        cursor = collection.find({"symbol": {"$in": symbols}}, {"_id": 0})
        return [doc for doc in cursor if isinstance(doc, dict)]
    except PyMongoError as e:
        raise RuntimeError(f"Failed to fetch instruments from MongoDB: {e}") from e
    finally:
        client.close()


def build_instrument_docs_from_holdings(
    portfolio_data: List[tuple],
) -> List[Dict[str, Any]]:
    """Build unique instrument documents from portfolio holdings."""
    seen: Dict[str, Dict[str, Any]] = {}
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


def upsert_instruments(db_name: str, instruments: List[Dict[str, Any]]) -> None:
    """Upsert instrument documents into MongoDB."""
    if not instruments:
        return

    client = get_mongo_client()
    try:
        db = client[db_name]
        collection = db["instruments"]

        upserted = 0
        for instrument in instruments:
            symbol = instrument.get("symbol")
            if not symbol:
                continue
            collection.replace_one({"symbol": symbol}, instrument, upsert=True)
            upserted += 1

        logger.info("Upserted %d instrument(s) into '%s.instruments'.", upserted, db_name)
    except PyMongoError as e:
        raise RuntimeError(f"Failed to upsert instruments into MongoDB: {e}") from e
    finally:
        client.close()


def run_once(db_name: str) -> None:
    """Run one polling cycle: detect and add new instruments."""
    logger.info("Fetching active portfolios from '%s.portfolios'...", db_name)
    portfolios = fetch_active_portfolios(db_name)
    logger.info("Fetched %d active portfolio(s).", len(portfolios))

    if not portfolios:
        logger.info("No active portfolios found.")
        return

    portfolio_data: List[tuple] = []
    all_symbols: Set[str] = set()

    for portfolio in portfolios:
        holdings = portfolio.get("holdings") or []
        if not isinstance(holdings, list):
            portfolio_data.append((portfolio, []))
            continue
        symbols = _extract_symbols_from_holdings(holdings)
        all_symbols.update(symbols)
        portfolio_data.append((portfolio, holdings))

    if not all_symbols:
        logger.info("No instrument symbols found in active portfolios.")
        return

    logger.info("Found %d unique instrument symbol(s) in active portfolios.", len(all_symbols))

    existing_instruments = fetch_instruments(db_name, list(all_symbols))
    present_symbols: Set[str] = {
        doc.get("symbol") for doc in existing_instruments if doc.get("symbol")
    }
    missing_symbols = [s for s in all_symbols if s not in present_symbols]

    if not missing_symbols:
        logger.info("All instruments already present in collection.")
        return

    logger.info("Found %d new instrument(s) missing from collection.", len(missing_symbols))

    provider = _get_market_data_provider()
    logger.info("Using market data provider: %s", provider.__class__.__name__)

    base_docs = build_instrument_docs_from_holdings(portfolio_data)
    docs_to_insert = [doc for doc in base_docs if doc["symbol"] in missing_symbols]

    enriched: List[Dict[str, Any]] = []
    for doc in docs_to_insert:
        symbol = doc.get("symbol")
        if not symbol:
            continue
        try:
            metadata = provider.fetch_instrument_metadata(symbol)
            if metadata:
                enriched.append({**doc, **metadata})
        except Exception as exc:
            logger.warning("Failed to fetch metadata for symbol=%s: %s", symbol, exc)

    if enriched:
        upsert_instruments(db_name, enriched)
        logger.info("Added %d new instrument(s) to collection.", len(enriched))
    else:
        logger.info("No new instruments could be enriched.")


def main() -> None:
    db_name = os.getenv("DATABASE_NAME", "VESTRA_PROD")
    poll_interval = get_poll_interval()

    logger.info("Starting instrument monitor daemon.")
    logger.info("Database: %s", db_name)
    logger.info("Poll interval: %d seconds.", poll_interval)
    logger.info("Press Ctrl+C to stop.")

    while True:
        try:
            run_once(db_name)
        except KeyboardInterrupt:
            logger.info("Interrupted; shutting down.")
            break
        except Exception as exc:
            logger.error("Poll cycle failed: %s", exc, exc_info=True)

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
