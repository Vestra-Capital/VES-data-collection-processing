"""Batch script to extract unique instruments from the portfolios collection,
enrich them with market metadata (sector, industry, dividend yield), and
upsert the results into the ``instruments`` collection.

Reads all documents from the ``portfolios`` collection, flattens the holdings
arrays, deduplicates by ``symbol`` (``securityCode.marketCode_yf``), and
upserts the resulting instrument documents into the ``instruments`` collection.

Instruments that already have ``sector``, ``industry``, and ``dividend_yield``
stored are skipped; only instruments missing at least one of these fields are
enriched via the configured market data provider.

Columns produced:
    - symbol
    - securityCode
    - marketCode_yf
    - securityDescription
    - currency
    - sector (enriched)
    - industry (enriched)
    - dividend_yield (enriched)

Environment variables:
    MONGODB_SRV — MongoDB connection string.
    DATABASE_NAME — Database name (defaults to ``VESTRA_PROD``).
    MARKET_DATA_PROVIDER — Market data provider module path
        (defaults to ``market.yfinance_provider.YFinanceProvider``).
"""

import os
import sys
from collections import OrderedDict
from typing import Any, Dict

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


def get_mongo_client() -> MongoClient:
    mongo_srv = os.getenv("MONGODB_SRV")
    if not mongo_srv:
        raise RuntimeError("MONGODB_SRV is missing. Check your .env file.")
    return MongoClient(mongo_srv)


def fetch_portfolios(db_name: str) -> list:
    client = get_mongo_client()
    try:
        db = client[db_name]
        collection = db["portfolios"]
        return list(collection.find({}, {"_id": 0}))
    except PyMongoError as e:
        raise RuntimeError(f"Failed to fetch portfolios from MongoDB: {e}") from e
    finally:
        client.close()


def extract_instruments(portfolio_documents: list) -> list:
    seen = OrderedDict()

    for portfolio in portfolio_documents:
        holdings = portfolio.get("holdings") or []
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


def _get_market_data_provider() -> Any:
    """Instantiate and return the configured market data provider.

    The provider class is resolved from the ``MARKET_DATA_PROVIDER``
    environment variable, which should be a dotted import path
    (e.g. ``market.yfinance_provider.YFinanceProvider``).

    Returns:
        An instance of a ``MarketDataProvider`` subclass.
    """
    import importlib

    provider_path = os.getenv(
        "MARKET_DATA_PROVIDER",
        "market.yfinance_provider.YFinanceProvider",
    )
    module_path, class_name = provider_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    provider_class = getattr(module, class_name)
    return provider_class()


def _fetch_instruments_missing_metadata(db_name: str, symbols: list) -> Dict[str, Dict[str, Any]]:
    """Query the ``instruments`` collection for documents missing metadata.

    Args:
        db_name: MongoDB database name.
        symbols: List of symbol strings to check.

    Returns:
        Mapping of ``symbol`` to the existing instrument document for symbols
        that are present in the collection but are missing at least one of
        ``sector``, ``industry``, or ``dividend_yield``.
    """
    if not symbols:
        return {}

    client = get_mongo_client()
    try:
        db = client[db_name]
        collection = db["instruments"]

        query = {
            "symbol": {"$in": symbols},
            "$or": [
                {"sector": {"$exists": False}},
                {"industry": {"$exists": False}},
                {"dividend_yield": {"$exists": False}},
            ],
        }
        cursor = collection.find(query, {"_id": 0})
        return {doc["symbol"]: doc for doc in cursor if isinstance(doc, dict) and "symbol" in doc}
    except PyMongoError as e:
        raise RuntimeError(f"Failed to query instruments from MongoDB: {e}") from e
    finally:
        client.close()


def _enrich_instruments(
    provider: Any,
    instruments: list,
    existing_docs: Dict[str, Dict[str, Any]],
) -> list:
    """Fetch market metadata for instruments that are missing it and merge.

    Args:
        provider: A ``MarketDataProvider`` instance.
        instruments: List of extracted instrument dicts.
        existing_docs: Mapping of symbol to existing MongoDB document for
            instruments that already exist in the collection but are missing
            metadata fields.

    Returns:
        List of instrument dicts enriched with market metadata, ready for
        upsert.
    """
    enriched = []
    for instrument in instruments:
        symbol = instrument.get("symbol")
        if not symbol:
            continue

        if symbol not in existing_docs:
            continue

        metadata = provider.fetch_instrument_metadata(symbol)
        if not metadata:
            continue

        existing = existing_docs[symbol]
        merged = {**existing, **metadata}
        enriched.append(merged)

    return enriched


def upsert_instruments(db_name: str, instruments: list) -> None:
    if not instruments:
        print("No instruments to upsert.")
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

        print(f"Upserted {upserted} instrument(s) into '{db_name}.instruments'.")
    except PyMongoError as e:
        raise RuntimeError(f"Failed to upsert instruments into MongoDB: {e}") from e
    finally:
        client.close()


def main() -> None:
    db_name = os.getenv("DATABASE_NAME", "VESTRA_PROD")
    print(f"Fetching portfolios from '{db_name}.portfolios'...")
    portfolio_documents = fetch_portfolios(db_name)
    print(f"Fetched {len(portfolio_documents)} portfolio document(s).")

    instruments = extract_instruments(portfolio_documents)
    print(f"Extracted {len(instruments)} unique instrument(s).")

    if not instruments:
        print("No instruments extracted; exiting.")
        return

    symbols = [inst["symbol"] for inst in instruments if inst.get("symbol")]
    print(f"Checking {len(symbols)} instrument(s) for missing metadata in '{db_name}.instruments'...")
    existing_docs = _fetch_instruments_missing_metadata(db_name, symbols)
    missing_count = len(existing_docs)
    print(f"Found {missing_count} instrument(s) missing sector/industry/dividend_yield.")

    if missing_count == 0:
        print("All instruments already have metadata; nothing to enrich.")
        return

    provider = _get_market_data_provider()
    print(f"Using market data provider: {provider.__class__.__name__}")

    enriched_instruments = _enrich_instruments(provider, instruments, existing_docs)
    print(f"Enriched {len(enriched_instruments)} instrument(s).")

    upsert_instruments(db_name, enriched_instruments)


if __name__ == "__main__":
    main()
