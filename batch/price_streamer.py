"""Poll current prices from a market data provider and update MongoDB.

This module is intended to run as a scheduled or long-lived process.  It
loads all instrument symbols from the ``instruments`` collection, fetches
current prices from the configured market data provider on a polling
interval, and updates each document's ``currentPrice`` field.

Environment variables:
    MONGODB_SRV — MongoDB connection string.
    DATABASE_NAME — Database name (defaults to ``VESTRA_PROD``).
    MARKET_DATA_PROVIDER — Market data provider module path
        (defaults to ``market.yfinance_provider.YFinanceProvider``).
    PRICE_POLL_INTERVAL_SECONDS — Seconds between price polls
        (defaults to ``15``).
    PRICE_BATCH_SIZE — Max number of symbols per yfinance batch
        (defaults to ``50``).
    PRICE_BATCH_DELAY_SECONDS — Seconds to wait between batches
        (defaults to ``2``).
"""

import logging
import math
import os
import sys
import time
from typing import Any, Callable, Dict

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from market.provider import MarketDataProvider

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


def get_mongo_client() -> MongoClient:
    mongo_srv = os.getenv("MONGODB_SRV")
    if not mongo_srv:
        raise RuntimeError("MONGODB_SRV is missing. Check your .env file.")
    return MongoClient(mongo_srv)


def load_symbols(db_name: str) -> list:
    """Return all unique instrument symbols from the ``instruments`` collection."""
    client = get_mongo_client()
    try:
        db = client[db_name]
        collection = db["instruments"]
        cursor = collection.find({}, {"_id": 0, "symbol": 1})
        return [doc["symbol"] for doc in cursor if isinstance(doc, dict) and doc.get("symbol")]
    except PyMongoError as e:
        raise RuntimeError(f"Failed to load symbols from MongoDB: {e}") from e
    finally:
        client.close()


def create_price_updater(db_name: str) -> Callable[[str, float], None]:
    """Return a callback that upserts ``currentPrice`` into MongoDB."""
    client = get_mongo_client()
    db = client[db_name]
    collection = db["instruments"]

    def on_price(symbol: str, price: float) -> None:
        try:
            collection.update_one(
                {"symbol": symbol},
                {"$set": {"currentPrice": price}},
                upsert=True,
            )
        except PyMongoError as e:
            logger.error("Failed to upsert price for %s: %s", symbol, e)

    return on_price


def get_market_data_provider() -> MarketDataProvider:
    """Instantiate and return the configured market data provider."""
    import importlib

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
    value = os.getenv("PRICE_POLL_INTERVAL_SECONDS", "15")
    try:
        interval = int(value)
        return max(interval, 10)
    except (TypeError, ValueError):
        return 15


def get_batch_size() -> int:
    """Return the max number of symbols per yfinance batch."""
    value = os.getenv("PRICE_BATCH_SIZE", "50")
    try:
        size = int(value)
        return max(size, 1)
    except (TypeError, ValueError):
        return 50


def get_batch_delay() -> float:
    """Return the seconds to wait between symbol batches."""
    value = os.getenv("PRICE_BATCH_DELAY_SECONDS", "2")
    try:
        delay = float(value)
        return max(delay, 0)
    except (TypeError, ValueError):
        return 2.0


def _chunk_list(items: list, size: int) -> list:
    """Split a list into chunks of at most *size* items."""
    return [items[i : i + size] for i in range(0, len(items), size)]


def main() -> None:
    db_name = os.getenv("DATABASE_NAME", "VESTRA_PROD")
    poll_interval = get_poll_interval()

    symbols = load_symbols(db_name)
    if not symbols:
        logger.info("No symbols found in instruments collection; exiting.")
        return

    logger.info("Loaded %d symbol(s) from '%s.instruments'.", len(symbols), db_name)
    logger.info("Poll interval: %d seconds.", poll_interval)
    logger.info("Batch size: %d symbol(s) per yfinance call.", get_batch_size())
    logger.info("Batch delay: %fs between batches.", get_batch_delay())

    provider = get_market_data_provider()
    logger.info("Using market data provider: %s", provider.__class__.__name__)

    on_price = create_price_updater(db_name)
    logger.info("Starting price poller (Ctrl+C to stop)...")

    last_valid_price: Dict[str, float] = {}

    def _is_valid_price(price: Any) -> bool:
        if not isinstance(price, (int, float)) or isinstance(price, bool):
            return False
        if not math.isfinite(price):
            return False
        return price > 0

    batch_size = get_batch_size()
    batch_delay = get_batch_delay()

    while True:
        try:
            batches = _chunk_list(symbols, batch_size)
            logger.info("Fetching prices in %d batch(es) of up to %d symbol(s).", len(batches), batch_size)
            all_prices: Dict[str, float] = {}
            for idx, batch in enumerate(batches):
                if idx > 0 and batch_delay > 0:
                    time.sleep(batch_delay)
                batch_prices = provider.fetch_current_prices(batch)
                all_prices.update(batch_prices)
            prices = all_prices
            for symbol, price in prices.items():
                if not _is_valid_price(price):
                    if symbol in last_valid_price:
                        logger.warning(
                            "Invalid price %s for %s; falling back to previous valid price %s.",
                            price,
                            symbol,
                            last_valid_price[symbol],
                        )
                        on_price(symbol, last_valid_price[symbol])
                    else:
                        logger.warning(
                            "Skipping invalid price %s for %s (no previous valid price).",
                            price,
                            symbol,
                        )
                    continue
                on_price(symbol, price)
                last_valid_price[symbol] = price
        except KeyboardInterrupt:
            logger.info("Interrupted; shutting down.")
            break
        except Exception as e:
            logger.error("Poll error: %s", e, exc_info=True)

        try:
            time.sleep(poll_interval)
        except KeyboardInterrupt:
            logger.info("Interrupted; shutting down.")
            break


if __name__ == "__main__":
    main()
