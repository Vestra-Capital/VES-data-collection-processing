"""Batch script to fetch 12-month price timeseries for instruments and store
them back into the ``instruments`` collection.

Reads all instruments from the ``instruments`` collection, fetches historical
price data (date + close price) for each symbol via the configured market data
provider, and upserts the resulting ``timeseries`` array onto each instrument
document.

Additionally calculates performance percentages based on the timeseries:
    - performance_1m: price change over the last ~30 days
    - performance_3m: price change over the last ~90 days
    - performance_1y: price change over the last ~365 days

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
    - performance_1m (added/updated): percentage change over ~30 days
    - performance_3m (added/updated): percentage change over ~90 days
    - performance_1y (added/updated): percentage change over ~365 days

Environment variables:
    MONGODB_SRV — MongoDB connection string.
    DATABASE_NAME — Database name (defaults to ``VESTRA_PROD``).
    MARKET_DATA_PROVIDER — Market data provider module path
        (defaults to ``market.yfinance_provider.YFinanceProvider``).
"""

import logging
import os
import sys
from datetime import date, datetime, timedelta
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


def _calculate_performance(timeseries: List[Dict[str, Any]]) -> Dict[str, float]:
    """Calculate 1M, 3M, 1Y performance percentages from timeseries data.

    For each period, finds the closest price on or before
    (latest_date - N days) and computes:
        (latest_price - past_price) / past_price * 100
    """
    if not timeseries:
        return {}

    sorted_ts = sorted(timeseries, key=lambda x: x.get("date", ""))

    latest_entry = sorted_ts[-1]
    latest_price = latest_entry.get("price")
    latest_date = latest_entry.get("date")

    if not latest_price:
        return {}

    try:
        latest_dt = datetime.strptime(latest_date, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return {}

    result: Dict[str, float] = {}

    for label, days in [("1m", 30), ("3m", 90), ("1y", 365)]:
        target_date = latest_dt - timedelta(days=days)

        closest_price = None
        for entry in sorted_ts:
            try:
                entry_date = datetime.strptime(entry.get("date", ""), "%Y-%m-%d").date()
            except (ValueError, TypeError):
                continue
            if entry_date <= target_date:
                closest_price = entry.get("price")
            else:
                break

        if closest_price and closest_price > 0:
            performance = (latest_price - closest_price) / closest_price * 100
            result[f"performance_{label}"] = round(performance, 2)

    return result


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

        print(f"Upserted {upserted} instrument(s) with timeseries and performance into '{db_name}.instruments'.")
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
            performance = _calculate_performance(timeseries)
            updated.update(performance)
            enriched.append(updated)
        else:
            logger.info("No timeseries data returned for symbol=%s; skipping.", symbol)

    print(f"Enriched {len(enriched)} instrument(s) with timeseries and performance. Failed: {len(failed)}.")

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
