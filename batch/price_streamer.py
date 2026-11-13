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
    MAX_PRICE_CONSECUTIVE_FAILURES — Max consecutive failures before a symbol
        is paused for the day (defaults to ``10``). Set to ``0`` to disable.
    PRICE_RATE_LIMIT_PAUSE_SECONDS — Seconds to pause when Yahoo Finance
        rate-limits the request (defaults to ``60``).
"""

import datetime
import logging
import math
import os
import sys
import time
from datetime import date
from typing import Any, Callable, Dict, Optional, Set, Type

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

# Try to import yfinance's rate-limit exception so we can pause longer when
# Yahoo Finance rate-limits us. Falls back to None if yfinance isn't installed
# or doesn't expose this exception in this version.
try:  # pragma: no cover - depends on yfinance version
    from yfinance.exceptions import YFRateLimitError  # type: ignore
except Exception:  # pragma: no cover
    YFRateLimitError: Optional[Type[BaseException]] = None


def _is_rate_limit_error(exc: BaseException) -> bool:
    """Return True if ``exc`` is a Yahoo Finance rate-limit error."""
    if YFRateLimitError is not None and isinstance(exc, YFRateLimitError):
        return True
    message = str(exc)
    return "Rate limited" in message or "Too Many Requests" in message


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


def get_rate_limit_pause_seconds() -> int:
    """Return the seconds to pause when Yahoo Finance rate-limits the request."""
    value = os.getenv("PRICE_RATE_LIMIT_PAUSE_SECONDS", "60")
    try:
        return max(int(value), 1)
    except (TypeError, ValueError):
        return 60


def main() -> None:
    db_name = os.getenv("DATABASE_NAME", "VESTRA_PROD")
    poll_interval = get_poll_interval()
    max_failures = _get_max_consecutive_failures()
    rate_limit_pause = get_rate_limit_pause_seconds()

    symbols = load_symbols(db_name)
    if not symbols:
        logger.info("No symbols found in instruments collection; exiting.")
        return

    logger.info("Loaded %d symbol(s) from '%s.instruments'.", len(symbols), db_name)
    logger.info("Poll interval: %d seconds.", poll_interval)
    logger.info("Rate-limit pause: %d seconds.", rate_limit_pause)
    if max_failures > 0:
        logger.info("Max consecutive failures before pausing a symbol: %d.", max_failures)

    provider = get_market_data_provider()
    logger.info("Using market data provider: %s", provider.__class__.__name__)

    on_price = create_price_updater(db_name)
    logger.info("Starting price poller (Ctrl+C to stop)...")

    last_valid_price: Dict[str, float] = {}
    consecutive_failures: Dict[str, int] = {}
    paused_symbols: Set[str] = set()
    last_reset_date: date | None = None

    def _is_valid_price(price: Any) -> bool:
        if not isinstance(price, (int, float)) or isinstance(price, bool):
            return False
        if not math.isfinite(price):
            return False
        return price > 0

    def _reset_if_new_day() -> None:
        nonlocal last_reset_date
        today = date.today()
        if last_reset_date != today:
            consecutive_failures.clear()
            paused_symbols.clear()
            last_reset_date = today
            logger.info("Reset per-symbol failure counters for %s.", today.isoformat())

    while True:
        try:
            _reset_if_new_day()
            prices = provider.fetch_current_prices(symbols)
            successful_symbols = set(prices.keys())

            for symbol in symbols:
                if symbol in paused_symbols:
                    continue

                if symbol not in successful_symbols:
                    consecutive_failures[symbol] = consecutive_failures.get(symbol, 0) + 1
                    failures = consecutive_failures[symbol]
                    if max_failures > 0 and failures >= max_failures:
                        paused_symbols.add(symbol)
                        logger.warning(
                            "Pausing symbol %s for the day after %d consecutive failures "
                            "(no price data).",
                            symbol,
                            failures,
                        )
                    else:
                        remaining = max_failures - failures if max_failures > 0 else 0
                        logger.warning(
                            "No price data for %s (failure %d/%s).",
                            symbol,
                            failures,
                            remaining if max_failures > 0 else "unlimited",
                        )
                    continue

                price = prices[symbol]
                if not _is_valid_price(price):
                    consecutive_failures[symbol] = consecutive_failures.get(symbol, 0) + 1
                    failures = consecutive_failures[symbol]
                    if max_failures > 0 and failures >= max_failures:
                        paused_symbols.add(symbol)
                        logger.warning(
                            "Pausing symbol %s for the day after %d consecutive failures "
                            "(invalid price %s).",
                            symbol,
                            failures,
                            price,
                        )
                    else:
                        remaining = max_failures - failures if max_failures > 0 else 0
                        logger.warning(
                            "Invalid price %s for %s (failure %d/%s).",
                            price,
                            symbol,
                            failures,
                            remaining if max_failures > 0 else "unlimited",
                        )
                    continue

                on_price(symbol, price)
                last_valid_price[symbol] = price
                consecutive_failures[symbol] = 0

        except KeyboardInterrupt:
            logger.info("Interrupted; shutting down.")
            break
        except Exception as e:
            logger.error("Poll error: %s", e, exc_info=True)
            if _is_rate_limit_error(e):
                logger.warning(
                    "Yahoo Finance rate-limited the request; pausing %ds before retrying.",
                    rate_limit_pause,
                )
                try:
                    time.sleep(rate_limit_pause)
                except KeyboardInterrupt:
                    logger.info("Interrupted; shutting down.")
                    break
                continue

        try:
            time.sleep(poll_interval)
        except KeyboardInterrupt:
            logger.info("Interrupted; shutting down.")
            break


def _get_max_consecutive_failures() -> int:
    """Return the max consecutive failures before pausing a symbol for the day."""
    value = os.getenv("MAX_PRICE_CONSECUTIVE_FAILURES", "10")
    try:
        return int(value)
    except (TypeError, ValueError):
        return 10


if __name__ == "__main__":
    main()
