"""Batch script to fetch 12-month price timeseries for instruments and store
them back into the ``instruments`` collection.

Reads all instruments from the ``instruments`` collection, fetches historical
price data (date + close price) for each symbol via the configured market data
provider, and upserts the resulting ``timeseries`` array onto each instrument
document.

The ``timeseries`` field contains a list of ``{"date": str, "price": float}``
dicts sorted ascending by date.  Only documents that successfully return
history are updated; failures are logged but do not abort the run.

Columns produced / stored:
    - symbol (existing)
    - securityCode (existing)
    - marketCode_yf (existing)
    - securityDescription (existing)
    - currency (existing)
    - sector (existing)
    - industry (existing)
    - dividend_yield (existing)
    - currentPrice (existing)
    - timeseries (added/updated): list of {"date": str, "price": float}

Environment variables:
    MONGODB_SRV — MongoDB connection string.
    DATABASE_NAME — Database name (defaults to ``VESTRA_PROD``).
    MARKET_DATA_PROVIDER — Market data provider module path
        (defaults to ``market.yfinance_provider.YFinanceProvider``).
"""

import logging
import os
import sys
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


def fetch_instruments(db_name: str) -> List[Dict[str, Any]]:
    client = get_mongo_client()
    try:
        db = client[db_name]
        collection = db["instruments"]
        return list(collection.find({}, {"_id": 0}))
    except PyMongoError as e:
        raise RuntimeError(f"Failed to fetch instruments from MongoDB: {e}") from e
    finally:
        client.close()


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


def _upsert_instrument_timeseries(db_name: str, documents: List[Dict[str, Any]]) -> None:
    if not documents:
        print("No instrument documents to upsert.")
        return

    client = get_mongo_client()
    try:
        db = client[db_name]
        collection = db["instruments"]

        upserted = 0
        for doc in documents:
            symbol = doc.get("symbol")
            if not symbol:
                continue
            collection.replace_one({"symbol": symbol}, doc, upsert=True)
            upserted += 1

        print(f"Upserted {upserted} instrument(s) with timeseries into '{db_name}.instruments'.")
    except PyMongoError as e:
        raise RuntimeError(f"Failed to upsert instrument timeseries into MongoDB: {e}") from e
    finally:
        client.close()


def main() -> None:
    db_name = os.getenv("DATABASE_NAME", "VESTRA_PROD")
    print(f"Fetching instruments from '{db_name}.instruments'...")
    instruments = fetch_instruments(db_name)
    print(f"Fetched {len(instruments)} instrument(s).")

    if not instruments:
        print("No instruments found; exiting.")
        return

    provider = _get_market_data_provider()
    print(f"Using market data provider: {provider.__class__.__name__}")

    enriched: List[Dict[str, Any]] = []
    failed: List[str] = []

    for instrument in instruments:
        symbol = instrument.get("symbol")
        if not symbol:
            continue

        try:
            timeseries = provider.fetch_instrument_timeseries(symbol, period="1y")
        except Exception as exc:
            logger.warning("Failed to fetch timeseries for symbol=%s: %s", symbol, exc)
            failed.append(symbol)
            continue

        if timeseries:
            updated = dict(instrument)
            updated["timeseries"] = timeseries
            enriched.append(updated)
        else:
            logger.info("No timeseries data returned for symbol=%s; skipping.", symbol)

    print(f"Enriched {len(enriched)} instrument(s) with timeseries. Failed: {len(failed)}.")

    if enriched:
        _upsert_instrument_timeseries(db_name, enriched)
    else:
        print("No instrument timeseries to upsert.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
