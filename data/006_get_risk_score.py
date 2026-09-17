"""Batch script to calculate portfolio risk scores and drawdown metrics.

For each document in the ``portfolios`` collection, this script reads the
``nav_timeseries`` (portfolio NAV over the available history) and ``holdings``
array, matches each holding to an instrument in the ``instruments`` collection
via ``securityCode.marketCode_yf`` -> ``symbol``, then calculates:

- ``riskScore`` — integer from 1 to 10 using portfolio volatility and asset diversification.
- ``maxDrawdown`` — maximum peak-to-trough decline over the full history.
- ``maxWeeklyDrawdown`` — maximum peak-to-trough decline over any 5-day window.
- ``maxYearlyDrawdown`` — maximum peak-to-trough decline over any 252-day window.

Values are stored back onto the portfolio document in MongoDB.  If fewer than
12 months of ``nav_timeseries`` data are available, the script uses whatever
is present rather than failing.

Risk score methodology
----------------------
``riskScore`` combines two components on a 0..1 scale and maps the result to
1..10.

1. Volatility component (60% weight)
   - Computes daily NAV returns from ``nav_timeseries``.
   - Calculates the population standard deviation of those returns.
   - Annualises using ``daily_std * sqrt(min(n, 252))`` where ``n`` is the
     number of available returns.
   - Normalises by dividing by 0.40 (40% annualised volatility maps to 1.0),
     then clamps to [0.0, 1.0].

2. Diversification component (40% weight, inverted in final score)
   - Computes the Herfindahl-Hirschman Index (HHI) of market-value weights.
   - Normalises concentration to [0.0, 1.0] where 1.0 is a single-stock
     portfolio.
   - Computes sector diversity as ``unique_sectors / 10`` capped at 1.0.
   - Combines as ``1.0 - (0.6 * concentration + 0.4 * (1.0 - sector_diversity))``
     and clamps to [0.0, 1.0].

Final score:
    combined_risk = 0.6 * vol_score + 0.4 * (1.0 - div_score)
    risk_score = 1.0 + combined_risk * 9.0
    result is clamped to [1.0, 10.0] and rounded to the nearest integer.

Drawdown methodology
--------------------
All drawdown values are expressed as positive decimals (e.g. 0.25 = 25%
peak-to-trough decline).

- ``maxDrawdown``
  Scans the full NAV series and records the largest peak-to-trough decline
  observed between any two points.

- ``maxWeeklyDrawdown``
  Slides a 5-day window across the NAV series and records the largest
  peak-to-trough decline observed within any such window.

- ``maxYearlyDrawdown``
  Slides a 252-day window across the NAV series and records the largest
  peak-to-trough decline observed within any such window.  If fewer than 252
  data points are available, the window size is capped to the series length.

Environment variables:
    MONGODB_SRV — MongoDB connection string.
    DATABASE_NAME — Database name (defaults to ``VESTRA_PROD``).
"""

import logging
import math
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


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


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


def _build_symbol_sector_map(instruments: List[Dict[str, Any]]) -> Dict[str, str]:
    sector_map: Dict[str, str] = {}
    for instrument in instruments:
        symbol = instrument.get("symbol")
        sector = instrument.get("sector")
        if symbol and sector:
            sector_map[symbol] = sector
    return sector_map


def _calculate_volatility_score(nav_timeseries: List[Dict[str, Any]]) -> float:
    """Annualised volatility component, normalised to 0..1.

    Uses all available ``nav_timeseries`` data points.  Falls back to a neutral
    score of 0.5 when fewer than two data points are present.
    """
    if not nav_timeseries or len(nav_timeseries) < 2:
        return 0.5

    navs = [_to_float(entry.get("nav")) for entry in nav_timeseries if entry.get("nav") is not None]
    if len(navs) < 2:
        return 0.5

    returns = []
    for i in range(1, len(navs)):
        if navs[i - 1] > 0:
            ret = (navs[i] - navs[i - 1]) / navs[i - 1]
            returns.append(ret)

    if not returns:
        return 0.5

    mean_ret = sum(returns) / len(returns)
    variance = sum((r - mean_ret) ** 2 for r in returns) / len(returns)
    daily_std = math.sqrt(variance)

    trading_days = min(len(returns), 252)
    annualised_vol = daily_std * math.sqrt(trading_days)

    return max(0.0, min(annualised_vol / 0.4, 1.0))


def _calculate_diversification_score(
    holdings: List[Dict[str, Any]],
    sector_map: Dict[str, str],
) -> float:
    """Diversification component, normalised to 0..1.

    0 = poorly diversified (high concentration, few sectors).
    1 = well diversified (many holdings, many sectors, low concentration).
    """
    if not holdings:
        return 0.0

    total_market_value = sum(
        _to_float(h.get("marketValue")) for h in holdings if isinstance(h, dict)
    )
    if total_market_value <= 0:
        return 0.0

    weights: List[float] = []
    sectors: List[str] = []
    for holding in holdings:
        if not isinstance(holding, dict):
            continue
        mv = _to_float(holding.get("marketValue"))
        weights.append(mv / total_market_value)

        security_code = holding.get("securityCode")
        market_code_yf = holding.get("marketCode_yf")
        if security_code and market_code_yf:
            symbol = f"{security_code}.{market_code_yf}"
            sector = sector_map.get(symbol)
            if sector:
                sectors.append(sector)

    n = len(weights)
    hhi = sum(w ** 2 for w in weights)
    min_hhi = 1.0 / n if n > 0 else 1.0
    concentration = (hhi - min_hhi) / (1.0 - min_hhi) if n > 1 else 1.0
    concentration = max(0.0, min(1.0, concentration))

    unique_sectors = len(set(sectors))
    sector_diversity = min(unique_sectors / 10.0, 1.0) if unique_sectors > 0 else 0.0

    return max(0.0, min(1.0 - (0.6 * concentration + 0.4 * (1.0 - sector_diversity)), 1.0))


def calculate_risk_score(
    nav_timeseries: List[Dict[str, Any]],
    holdings: List[Dict[str, Any]],
    sector_map: Dict[str, str],
) -> int:
    """Calculate a portfolio risk score from 1 to 10."""
    vol_score = _calculate_volatility_score(nav_timeseries)
    div_score = _calculate_diversification_score(holdings, sector_map)

    combined_risk = 0.6 * vol_score + 0.4 * (1.0 - div_score)
    risk_score = 1.0 + combined_risk * 9.0
    return int(round(max(1.0, min(10.0, risk_score))))


RISK_LABELS = {
    1: "Very Low",
    2: "Low",
    3: "Low",
    4: "Moderate",
    5: "Moderate",
    6: "High",
    7: "High",
    8: "High",
    9: "Very High",
    10: "Very High",
}


def get_risk_label(risk_score: int) -> str:
    """Map a 1-10 risk score to a human-readable label."""
    return RISK_LABELS.get(risk_score, "Unknown")


def _calculate_max_drawdown(navs: List[float]) -> float:
    """Maximum peak-to-trough decline over the provided NAV series."""
    if not navs or len(navs) < 2:
        return 0.0

    peak = navs[0]
    max_dd = 0.0
    for nav in navs:
        if nav > peak:
            peak = nav
        if peak > 0:
            dd = (peak - nav) / peak
            if dd > max_dd:
                max_dd = dd
    return max_dd


def _calculate_max_drawdown_in_window(navs: List[float], window_size: int) -> float:
    """Maximum drawdown observed in any rolling window of ``window_size``."""
    if not navs or len(navs) < 2 or window_size < 2:
        return 0.0

    max_dd = 0.0
    end = len(navs) - window_size + 1
    if end <= 0:
        return _calculate_max_drawdown(navs)

    for i in range(end):
        window = navs[i : i + window_size]
        dd = _calculate_max_drawdown(window)
        if dd > max_dd:
            max_dd = dd
    return max_dd


def calculate_drawdown_metrics(nav_timeseries: List[Dict[str, Any]]) -> Dict[str, float]:
    """Return drawdown metrics derived from ``nav_timeseries``."""
    navs = [_to_float(entry.get("nav")) for entry in nav_timeseries if entry.get("nav") is not None]
    if not navs or len(navs) < 2:
        return {
            "maxDrawdown": 0.0,
            "maxWeeklyDrawdown": 0.0,
            "maxYearlyDrawdown": 0.0,
        }

    max_dd = _calculate_max_drawdown(navs)
    max_weekly_dd = _calculate_max_drawdown_in_window(navs, 5)
    max_yearly_dd = _calculate_max_drawdown_in_window(navs, min(252, len(navs)))

    return {
        "maxDrawdown": round(max_dd, 6),
        "maxWeeklyDrawdown": round(max_weekly_dd, 6),
        "maxYearlyDrawdown": round(max_yearly_dd, 6),
    }


def update_portfolio_metrics(
    db_name: str,
    account_number: str,
    risk_score: int,
    risk_label: str,
    drawdown_metrics: Dict[str, float],
) -> None:
    client = get_mongo_client()
    try:
        db = client[db_name]
        collection = db["portfolios"]
        collection.update_one(
            {"accountNumber": account_number},
            {"$set": {"riskScore": risk_score, "riskLabel": risk_label, **drawdown_metrics}},
            upsert=True,
        )
    except PyMongoError as e:
        raise RuntimeError(
            f"Failed to update metrics for accountNumber={account_number}: {e}"
        ) from e
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
    portfolio_data: List[tuple] = []

    for portfolio in portfolios:
        holdings = portfolio.get("holdings") or []
        if not isinstance(holdings, list):
            portfolio_data.append((portfolio, []))
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

        portfolio_data.append((portfolio, holdings))

    print(f"Found {len(all_symbols)} unique instrument symbol(s).")

    sector_map: Dict[str, str] = {}
    if all_symbols:
        print(f"Fetching instruments for {len(all_symbols)} symbol(s) from '{db_name}.instruments'...")
        instruments = fetch_instruments(db_name, list(all_symbols))
        print(f"Fetched {len(instruments)} instrument(s).")
        sector_map = _build_symbol_sector_map(instruments)

    updated = 0
    failed: List[str] = []

    for portfolio, holdings in portfolio_data:
        account_number = portfolio.get("accountNumber", "N/A")
        try:
            nav_timeseries = portfolio.get("nav_timeseries") or []
            risk_score = calculate_risk_score(nav_timeseries, holdings, sector_map)
            risk_label = get_risk_label(risk_score)
            drawdown_metrics = calculate_drawdown_metrics(nav_timeseries)
            update_portfolio_metrics(db_name, account_number, risk_score, risk_label, drawdown_metrics)
            print(
                f"Updated metrics for accountNumber={account_number}: "
                f"riskScore={risk_score}, riskLabel={risk_label}, "
                f"maxDrawdown={drawdown_metrics['maxDrawdown']}, "
                f"maxWeeklyDrawdown={drawdown_metrics['maxWeeklyDrawdown']}, "
                f"maxYearlyDrawdown={drawdown_metrics['maxYearlyDrawdown']}"
            )
            updated += 1
        except RuntimeError as e:
            logger.error("Failed to update metrics for accountNumber=%s: %s", account_number, e)
            failed.append(str(account_number))

    print(f"\nSummary: {updated} portfolio(s) updated, {len(failed)} failed: {failed}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
