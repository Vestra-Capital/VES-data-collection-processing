"""Batch script to update NAV timeseries and risk metrics for portfolios missing them.

Queries the ``portfolios`` collection for documents where ``nav_timeseries`` is
missing or empty, then for each:

1. Calculates ``nav_timeseries`` from ``holdings`` and instrument ``timeseries``.
2. Stores the computed ``nav_timeseries`` back to the portfolio.
3. Calculates ``riskScore``, ``riskLabel``, and drawdown metrics from the new
   ``nav_timeseries`` and ``holdings``.
4. Stores the risk metrics back to the portfolio.

Environment variables:
    MONGODB_SRV — MongoDB connection string.
    DATABASE_NAME — Database name (defaults to ``VESTRA_PROD``).
"""

import importlib
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


def main() -> None:
    db_name = os.getenv("DATABASE_NAME", "VESTRA_PROD")
    print(f"Fetching portfolios missing nav_timeseries from '{db_name}.portfolios'...")
    portfolios = fetch_portfolios_missing_nav(db_name)
    print(f"Found {len(portfolios)} portfolio(s) missing nav_timeseries.")

    if not portfolios:
        print("Nothing to do; exiting.")
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

    print(f"Found {len(all_symbols)} unique instrument symbol(s).")

    price_map: Dict[str, Dict[str, float]] = {}
    sector_map: Dict[str, str] = {}

    if all_symbols:
        print(f"Fetching instruments for {len(all_symbols)} symbol(s) from '{db_name}.instruments'...")
        instruments = fetch_instruments(db_name, list(all_symbols))
        print(f"Fetched {len(instruments)} instrument(s).")
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
                print(f"Updated NAV for accountNumber={account_number}: {len(nav_timeseries)} date(s).")
                updated_nav += 1

                risk_score = calculate_risk_score(nav_timeseries, holdings, sector_map)
                risk_label = get_risk_label(risk_score)
                drawdown_metrics = calculate_drawdown_metrics(nav_timeseries)
                update_portfolio_metrics(db_name, account_number, risk_score, risk_label, drawdown_metrics)
                print(
                    f"Updated risk for accountNumber={account_number}: "
                    f"riskScore={risk_score}, riskLabel={risk_label}, "
                    f"maxDrawdown={drawdown_metrics['maxDrawdown']}, "
                    f"maxWeeklyDrawdown={drawdown_metrics['maxWeeklyDrawdown']}, "
                    f"maxYearlyDrawdown={drawdown_metrics['maxYearlyDrawdown']}"
                )
                updated_risk += 1
            else:
                print(f"No NAV data generated for accountNumber={account_number}; skipping risk.")
        except RuntimeError as e:
            logger.error("Failed for accountNumber=%s: %s", account_number, e)
            failed.append(str(account_number))

    print(f"\nSummary: {updated_nav} portfolio(s) NAV updated, {updated_risk} risk updated, {len(failed)} failed: {failed}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
