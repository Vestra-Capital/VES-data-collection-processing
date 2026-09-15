"""Batch script to calculate portfolio NAV time series over the last 12 months.

For each document in the ``portfolios`` collection, this script reads the
``holdings`` array, matches each holding to an instrument in the ``instruments``
collection via ``securityCode.marketCode_yf`` -> ``symbol``, then multiplies
each holding's ``totalHolding`` by the instrument's daily price (from the
``timeseries`` array) and sums across all holdings to produce a per-date NAV.

The resulting ``nav_timeseries`` list of ``{"date": str, "nav": float}`` dicts
is stored back onto the portfolio document in MongoDB.

Environment variables:
    MONGODB_SRV — MongoDB connection string.
    DATABASE_NAME — Database name (defaults to ``VESTRA_PROD``).
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


def fetch_portfolios(db_name: str) -> List[Dict[str, Any]]:
    client = get_mongo_client()
    try:
        db = client[db_name]
        collection = db["portfolios"]
        return list(collection.find({}))
    except PyMongoError as e:
        raise RuntimeError(f"Failed to fetch portfolios from MongoDB: {e}") from e
    finally:
        client.close()


def fetch_instruments(db_name: str, symbols: List[str]) -> List[Dict[str, Any]]:
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


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _build_instrument_price_map(instruments: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    price_map: Dict[str, Dict[str, float]] = {}

    for instrument in instruments:
        symbol = instrument.get("symbol")
        if not symbol:
            continue

        timeseries = instrument.get("timeseries") or []
        if not isinstance(timeseries, list):
            continue

        date_prices: Dict[str, float] = {}
        for entry in timeseries:
            if not isinstance(entry, dict):
                continue
            date = entry.get("date")
            price = entry.get("price")
            if date and price is not None:
                date_prices[date] = _to_float(price)

        if date_prices:
            price_map[symbol] = date_prices

    return price_map


def _calculate_nav_timeseries(
    holdings: List[Dict[str, Any]],
    price_map: Dict[str, Dict[str, float]],
) -> List[Dict[str, float]]:
    nav_by_date: Dict[str, float] = {}

    for holding in holdings:
        security_code = holding.get("securityCode")
        market_code_yf = holding.get("marketCode_yf")
        total_holding = _to_float(holding.get("totalHolding"))

        if not security_code or not market_code_yf or total_holding <= 0:
            continue

        symbol = f"{security_code}.{market_code_yf}"
        date_prices = price_map.get(symbol)
        if not date_prices:
            continue

        for date, price in date_prices.items():
            nav_by_date[date] = nav_by_date.get(date, 0.0) + total_holding * price

    nav_timeseries = [
        {"date": date, "nav": round(nav, 2)}
        for date, nav in sorted(nav_by_date.items())
    ]
    return nav_timeseries


def _update_portfolio_nav(
    db_name: str,
    portfolio: Dict[str, Any],
    nav_timeseries: List[Dict[str, float]],
) -> None:
    client = get_mongo_client()
    try:
        db = client[db_name]
        collection = db["portfolios"]
        account_number = portfolio.get("accountNumber")
        if not account_number:
            return
        collection.update_one(
            {"accountNumber": account_number},
            {"$set": {"nav_timeseries": nav_timeseries}},
        )
    except PyMongoError as e:
        raise RuntimeError(f"Failed to update portfolio NAV for accountNumber={account_number}: {e}") from e
    finally:
        client.close()


def main() -> None:
    db_name = os.getenv("DATABASE_NAME", "VESTRA_PROD")
    print(f"Fetching portfolios from '{db_name}.portfolios'...")
    portfolios = fetch_portfolios(db_name)
    print(f"Fetched {len(portfolios)} portfolio document(s).")

    if not portfolios:
        print("No portfolios found; exiting.")
        return

    all_symbols: set = set()
    portfolio_holdings: List[tuple] = []

    for portfolio in portfolios:
        holdings = portfolio.get("holdings") or []
        if not isinstance(holdings, list):
            portfolio_holdings.append((portfolio, []))
            continue

        symbols: List[str] = []
        for holding in holdings:
            if not isinstance(holding, dict):
                continue
            security_code = holding.get("securityCode")
            market_code_yf = holding.get("marketCode_yf")
            if security_code and market_code_yf:
                symbol = f"{security_code}.{market_code_yf}"
                symbols.append(symbol)
                all_symbols.add(symbol)

        portfolio_holdings.append((portfolio, holdings))

    print(f"Found {len(all_symbols)} unique instrument symbol(s).")

    if not all_symbols:
        print("No instruments to process; exiting.")
        return

    print(f"Fetching instruments for {len(all_symbols)} symbol(s) from '{db_name}.instruments'...")
    instruments = fetch_instruments(db_name, list(all_symbols))
    print(f"Fetched {len(instruments)} instrument(s).")

    price_map = _build_instrument_price_map(instruments)
    print(f"Built price map for {len(price_map)} instrument(s).")

    updated = 0
    failed: List[str] = []

    for portfolio, holdings in portfolio_holdings:
        account_number = portfolio.get("accountNumber", "N/A")
        try:
            nav_timeseries = _calculate_nav_timeseries(holdings, price_map)
            _update_portfolio_nav(db_name, portfolio, nav_timeseries)
            print(f"Updated NAV for accountNumber={account_number}: {len(nav_timeseries)} date(s).")
            updated += 1
        except RuntimeError as e:
            logger.error("Failed to update NAV for accountNumber=%s: %s", account_number, e)
            failed.append(str(account_number))

    print(f"\nSummary: {updated} portfolio(s) updated, {len(failed)} failed: {failed}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
