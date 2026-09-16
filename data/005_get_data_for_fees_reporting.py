"""Assets under management (AUM) collector for fees reporting.

Reads the MongoDB ``clients``, ``portfolios``, and ``instruments`` collections
and produces a per-client AUM snapshot with daily P&L:

    total_holdings + total_cash = total_aum
    totalPnl = sum((currentPrice or marketPrice - averageCost) * totalHolding)

``total_holdings`` is taken from the latest entry in the portfolio's
``nav_timeseries`` array (populated by ``batch/get_portfolios_nav.py``),
not by summing ``marketValue`` across holdings.

The resulting records are upserted into the ``fees`` collection
keyed by ``accountNumber``.  Each document is stamped with ``generatedAt``
so that historical AUM snapshots can be retained.

Typical usage::

    python data/005_get_data_for_fees_reporting.py

Environment variables required:

    MONGODB_SRV — MongoDB connection string.
    DATABASE_NAME — Database name (defaults to ``VESTRA_PROD``).
    FEES_REPORT_RATE — Annual fee rate as a percentage (defaults to ``1.5``).
"""

import logging
import math
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

load_dotenv()

logger = logging.getLogger(__name__)

DATABASE_NAME = os.getenv("DATABASE_NAME", "VESTRA_PROD")
CLIENTS_COLLECTION = "clients"
PORTFOLIOS_COLLECTION = "portfolios"
INSTRUMENTS_COLLECTION = "instruments"
FEES_COLLECTION = "fees"
FEES_REPORT_RATE = float(os.getenv("FEES_REPORT_RATE", "1.5"))


def _get_mongo_client() -> MongoClient:
    """Create and return a MongoDB client from environment configuration.

    Returns:
        A ``MongoClient`` instance connected to the cluster specified by
        ``MONGODB_SRV`` in ``.env``.

    Raises:
        RuntimeError: If ``MONGODB_SRV`` is not configured.
    """
    mongo_srv = os.getenv("MONGODB_SRV")
    if not mongo_srv:
        raise RuntimeError("MONGODB_SRV is missing. Check your .env file.")
    return MongoClient(mongo_srv)


def _fetch_clients() -> List[Dict[str, Any]]:
    """Fetch all client documents from the MongoDB ``clients`` collection.

    Returns:
        List of client document dicts, each containing at least ``accountNumber``.

    Raises:
        RuntimeError: If the MongoDB connection or query fails.
    """
    client = _get_mongo_client()
    try:
        db = client[DATABASE_NAME]
        collection = db[CLIENTS_COLLECTION]
        documents = list(collection.find({}, {"_id": 0}))
        return [doc for doc in documents if isinstance(doc, dict)]
    except PyMongoError as e:
        raise RuntimeError(f"Failed to fetch clients from MongoDB: {e}") from e
    finally:
        client.close()


def _fetch_portfolios() -> Dict[str, Dict[str, Any]]:
    """Fetch all portfolio documents keyed by ``accountNumber``.

    Returns:
        Dictionary mapping each ``accountNumber`` string to its portfolio
        document (containing ``holdings`` and optionally ``cash``).

    Raises:
        RuntimeError: If the MongoDB connection or query fails.
    """
    client = _get_mongo_client()
    try:
        db = client[DATABASE_NAME]
        collection = db[PORTFOLIOS_COLLECTION]
        documents = list(collection.find({}))
        portfolios: Dict[str, Dict[str, Any]] = {}
        for doc in documents:
            if isinstance(doc, dict):
                account_number = doc.get("accountNumber")
                if account_number:
                    portfolios[str(account_number)] = doc
        return portfolios
    except PyMongoError as e:
        raise RuntimeError(f"Failed to fetch portfolios from MongoDB: {e}") from e
    finally:
        client.close()


def _fetch_instruments() -> Dict[str, Dict[str, Any]]:
    """Fetch all instrument documents keyed by ``symbol``.

    Returns:
        Dictionary mapping each ``symbol`` string to its instrument document.

    Raises:
        RuntimeError: If the MongoDB connection or query fails.
    """
    client = _get_mongo_client()
    try:
        db = client[DATABASE_NAME]
        collection = db[INSTRUMENTS_COLLECTION]
        documents = list(collection.find({}))
        instruments: Dict[str, Dict[str, Any]] = {}
        for doc in documents:
            if isinstance(doc, dict):
                symbol = doc.get("symbol")
                if symbol:
                    instruments[str(symbol)] = doc
        return instruments
    except PyMongoError as e:
        raise RuntimeError(f"Failed to fetch instruments from MongoDB: {e}") from e
    finally:
        client.close()


def _to_float(value: Any) -> float:
    """Safely convert a value to float.

    Args:
        value: Value to convert.

    Returns:
        Numeric float value, or ``0.0`` if the value is missing, not numeric,
        or NaN.
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        result = float(value)
        if isinstance(result, float) and math.isnan(result):
            return 0.0
        return result
    if isinstance(value, str):
        try:
            result = float(value)
            if math.isnan(result):
                return 0.0
            return result
        except (TypeError, ValueError):
            return 0.0
    return 0.0


def _is_leap_year(year: int) -> bool:
    """Return True if the given year is a leap year."""
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def _days_in_year(d: datetime) -> int:
    """Return the number of days in the year of the given datetime."""
    return 366 if _is_leap_year(d.year) else 365


def _latest_nav_on_or_before(
    nav_timeseries: List[Dict[str, Any]],
    target_date: datetime,
) -> Optional[float]:
    """Return the latest ``nav`` from ``nav_timeseries`` on or before ``target_date``.

    Args:
        nav_timeseries: List of ``{"date": "YYYY-MM-DD", "nav": float}`` dicts.
        target_date: The cutoff datetime. Only entries with a date on or before
            this datetime are considered.

    Returns:
        The latest ``nav`` value as a float, or ``None`` if no matching entry
        exists.
    """
    latest_nav: Optional[float] = None
    target_date_only = target_date.date()

    for entry in nav_timeseries:
        if not isinstance(entry, dict):
            continue
        raw_date = entry.get("date")
        nav = _to_float(entry.get("nav"))
        if not raw_date or nav <= 0:
            continue
        try:
            entry_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            continue
        if entry_date <= target_date_only:
            latest_nav = nav
        else:
            break

    return latest_nav


def _calculate_total_cash(cash_data: Any) -> float:
    """Sum the bank balance across all cash bank accounts for an account.

    Args:
        cash_data: Cash sub-document from the portfolio document.

    Returns:
        Total cash value as a float.
    """
    total = 0.0
    if not isinstance(cash_data, dict):
        return total
    bank_accounts = cash_data.get("bankAccounts") or []
    if not isinstance(bank_accounts, list):
        return total
    for bank_account in bank_accounts:
        if not isinstance(bank_account, dict):
            continue
        total += _to_float(bank_account.get("bankBalance"))
    return total


def _build_symbol(holding: Dict[str, Any]) -> str:
    """Build the Yahoo Finance symbol from a holding's security code and market code.

    Args:
        holding: Single holding dict from the portfolio document.

    Returns:
        Symbol string in the format ``securityCode.marketCode_yf``.
    """
    security_code = holding.get("securityCode", "")
    market_code_yf = holding.get("marketCode_yf", "")
    if security_code and market_code_yf:
        return f"{security_code}.{market_code_yf}"
    return ""


def _build_instrument_price_map(instruments: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    """Build a date-keyed price map from the instruments ``timeseries``.

    Args:
        instruments: List of instrument documents from the ``instruments``
            collection.

    Returns:
        Dictionary mapping each ``symbol`` to a dict of
        ``{"YYYY-MM-DD": price}`` entries containing only valid numeric
        prices.
    """
    price_map: Dict[str, Dict[str, float]] = {}

    for instrument in instruments:
        if not isinstance(instrument, dict):
            continue

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
            raw_date = entry.get("date")
            price = entry.get("price")
            if raw_date and price is not None:
                price_value = _to_float(price)
                if price_value > 0 and not math.isnan(price_value):
                    date_prices[str(raw_date)] = price_value

        if date_prices:
            price_map[str(symbol)] = date_prices

    return price_map


def _latest_price_on_or_before(
    date_prices: Dict[str, float],
    target_date: datetime,
) -> float:
    """Return the latest price from ``date_prices`` on or before ``target_date``.

    Args:
        date_prices: Dictionary mapping ``"YYYY-MM-DD"`` strings to prices.
        target_date: The cutoff datetime. Only entries with a date on or
            before this datetime are considered.

    Returns:
        The latest valid price as a float, or ``0.0`` if no matching entry
        exists.
    """
    target_str = target_date.strftime("%Y-%m-%d")
    latest_price = 0.0

    sorted_dates = sorted(date_prices.keys())
    for date_str in sorted_dates:
        if date_str > target_str:
            break
        price = date_prices[date_str]
        if price > 0 and not math.isnan(price):
            latest_price = price

    return latest_price


def _calculate_account_pnl(
    holdings: List[Dict[str, Any]],
    instruments: Dict[str, Dict[str, Any]],
    price_map: Dict[str, Dict[str, float]],
    today: datetime,
) -> float:
    """Calculate the total P&L for an account.

    Uses the following price source fallback chain for each holding:
      1. ``currentPrice`` from the instruments collection.
      2. Latest valid price from the instruments ``timeseries`` on or before
         ``today``.
      3. ``marketPrice`` from the portfolio holding document.

    Args:
        holdings: List of holding dicts from the portfolio document.
        instruments: Dictionary mapping symbol strings to instrument documents.
        price_map: Dictionary mapping symbol strings to date-keyed price maps
            from the instruments ``timeseries``.
        today: The current datetime used as the cutoff for timeseries lookups.

    Returns:
        Total P&L for the account as a float.
    """
    total_pnl = 0.0
    if not isinstance(holdings, list):
        return total_pnl

    for holding in holdings:
        if not isinstance(holding, dict):
            continue

        symbol = _build_symbol(holding)
        if not symbol:
            continue

        instrument = instruments.get(symbol)
        current_price = _to_float(instrument.get("currentPrice")) if instrument else 0.0

        if current_price <= 0 or math.isnan(current_price):
            date_prices = price_map.get(symbol)
            if date_prices:
                current_price = _latest_price_on_or_before(date_prices, today)

        market_price = _to_float(holding.get("marketPrice"))

        effective_price = current_price if current_price > 0 else market_price
        if effective_price <= 0 or math.isnan(effective_price):
            continue

        average_cost = _to_float(holding.get("averageCost"))
        total_holding = _to_float(holding.get("totalHolding"))

        if math.isnan(average_cost) or math.isnan(total_holding):
            continue

        cost_value = average_cost * total_holding
        market_value = effective_price * total_holding
        total_pnl += market_value - cost_value

    return total_pnl


def _build_aum_document(
    client: Dict[str, Any],
    portfolio: Dict[str, Any],
    rate: float = FEES_REPORT_RATE,
    total_holdings: Optional[float] = None,
) -> Dict[str, Any]:
    """Build an AUM document for a single client/account.

    Args:
        client: Client document dict from the ``clients`` collection.
        portfolio: Portfolio document dict from the ``portfolios`` collection,
            or an empty dict if the client has no portfolio.
        rate: Annual fee rate as a percentage.
        total_holdings: Total holdings value to use.  This must be provided;
            it is not derived from ``marketValue``.

    Returns:
        AUM document dict containing ``accountNumber``, identity fields,
        ``totalHoldings``, ``totalCash``, ``totalAUM``, ``feeReportRate``,
        ``collectedFees``, ``totalPnl``, ``selected``, and
        ``generatedAt``.
    """
    if total_holdings is None:
        raise ValueError("total_holdings must be provided and cannot be None.")

    account_number = str(client.get("accountNumber", ""))
    cash_data = portfolio.get("cash") or {}

    total_cash = _calculate_total_cash(cash_data)
    total_aum = total_holdings + total_cash
    days_in_year = 366 if _is_leap_year(datetime.now(timezone.utc).year) else 365
    collected_fees = round((total_aum * (rate / 100)) / days_in_year, 2)

    return {
        "accountNumber": account_number,
        "accountName": client.get("accountName", ""),
        "totalHoldings": round(total_holdings, 2),
        "totalCash": round(total_cash, 2),
        "totalAUM": round(total_aum, 2),
        "feeReportRate": rate,
        "collectedFees": collected_fees,
        "totalPnl": 0.0,
        "selected": True,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
    }


def _generate_run_id() -> str:
    """Generate a unique run ID based on the current UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def _backfill_missing_fee_fields() -> None:
    """Backfill ``feeReportRate`` and ``collectedFees`` for existing documents.

    Finds any document in the ``fees`` collection missing ``feeReportRate``
    or ``collectedFees`` and updates them using the current default rate
    and the document's existing ``totalAUM``.
    """
    client = _get_mongo_client()
    try:
        db = client[DATABASE_NAME]
        collection = db[FEES_COLLECTION]

        updated = 0
        for doc in collection.find({"$or": [{"feeReportRate": {"$exists": False}}, {"collectedFees": {"$exists": False}}]}):
            total_aum = _to_float(doc.get("totalAUM"))
            rate = _to_float(doc.get("feeReportRate", FEES_REPORT_RATE))
            days_in_year = 366 if _is_leap_year(datetime.now(timezone.utc).year) else 365
            collected_fees = round((total_aum * (rate / 100)) / days_in_year, 2)

            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {"feeReportRate": rate, "collectedFees": collected_fees}},
            )
            updated += 1

        if updated:
            print(f"Backfilled {updated} document(s) in '{DATABASE_NAME}.{FEES_COLLECTION}' with missing fee fields.")
    except PyMongoError as e:
        raise RuntimeError(f"Failed to backfill missing fee fields: {e}") from e
    finally:
        client.close()


def _backfill_missing_pnl_fields() -> None:
    """Backfill ``totalPnl`` for existing documents missing the field.

    Finds any document in the ``fees`` collection missing ``totalPnl``
    and sets it to ``0.0``.
    """
    client = _get_mongo_client()
    try:
        db = client[DATABASE_NAME]
        collection = db[FEES_COLLECTION]

        updated = 0
        for doc in collection.find({"totalPnl": {"$exists": False}}):
            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {"totalPnl": 0.0}},
            )
            updated += 1

        if updated:
            print(f"Backfilled {updated} document(s) in '{DATABASE_NAME}.{FEES_COLLECTION}' with missing totalPnl field.")
    except PyMongoError as e:
        raise RuntimeError(f"Failed to backfill missing totalPnl fields: {e}") from e
    finally:
        client.close()


def insert_aum(documents: List[Dict[str, Any]]) -> None:
    """Upsert AUM documents into the ``fees`` collection.

    Each document is keyed by ``accountNumber`` and ``generatedAt`` date.
    If a matching document already exists, it is updated with the latest
    values.  Otherwise, a new document is inserted.

    Args:
        documents: List of AUM document dicts to upsert.

    Raises:
        RuntimeError: If the MongoDB connection or upsert operation fails.
    """
    if not documents:
        print("No AUM documents to upsert.")
        return

    client = _get_mongo_client()
    try:
        db = client[DATABASE_NAME]
        collection = db[FEES_COLLECTION]

        inserted = 0
        updated = 0
        for doc in documents:
            account_number = doc.get("accountNumber")
            generated_at = doc.get("generatedAt", "")
            date_part = generated_at.split("T")[0] if generated_at else ""

            if not account_number or not date_part:
                continue

            result = collection.update_one(
                {
                    "accountNumber": account_number,
                    "generatedAt": {"$regex": f"^{date_part}"},
                },
                {"$set": doc},
                upsert=True,
            )
            if result.upserted_id:
                inserted += 1
            elif result.modified_count:
                updated += 1

        print(f"Upserted {inserted + updated} AUM document(s) into '{DATABASE_NAME}.{FEES_COLLECTION}' ({inserted} inserted, {updated} updated).")
    except PyMongoError as e:
        raise RuntimeError(f"Failed to upsert AUM documents into MongoDB: {e}") from e
    finally:
        client.close()


def _is_leap_year(year: int) -> bool:
    """Return True if the given year is a leap year."""
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def _fetch_aum_documents() -> List[Dict[str, Any]]:
    """Fetch all AUM documents from the ``fees`` collection.

    Returns:
        List of AUM document dicts, each containing at least ``accountNumber``
        and ``totalAUM``.

    Raises:
        RuntimeError: If the MongoDB connection or query fails.
    """
    client = _get_mongo_client()
    try:
        db = client[DATABASE_NAME]
        collection = db[FEES_COLLECTION]
        documents = list(collection.find({}, {"_id": 0}))
        return [doc for doc in documents if isinstance(doc, dict)]
    except PyMongoError as e:
        raise RuntimeError(f"Failed to fetch AUM documents from MongoDB: {e}") from e
    finally:
        client.close()


def generate_daily_fee_report(rate: float = FEES_REPORT_RATE) -> None:
    """Generate and print a daily fee report from the ``fees`` collection.

    For each AUM record, calculates:
        Fee = (totalAUM * rate / 100) / days_in_year

    where ``days_in_year`` is 366 for leap years and 365 otherwise.

    Args:
        rate: Annual fee rate as a percentage (e.g. ``1.5`` for 1.5%/annum).
    """
    documents = _fetch_aum_documents()

    if not documents:
        print("No AUM documents found in 'fees' collection.")
        return

    today = datetime.now(timezone.utc)
    days_in_year = 366 if _is_leap_year(today.year) else 365
    date_str = today.strftime("%Y-%m-%d")

    rows: List[Dict[str, Any]] = []
    for doc in documents:
        aum = _to_float(doc.get("totalAUM"))
        doc_rate = _to_float(doc.get("feeReportRate", rate))
        daily_fee = _to_float(doc.get("collectedFees"))
        if not daily_fee:
            annual_fee = aum * (doc_rate / 100)
            daily_fee = annual_fee / days_in_year

        rows.append({
            "Account": str(doc.get("accountNumber", "")),
            "AccountName": doc.get("accountName", ""),
            "Date": date_str,
            "Holdings": f"{_to_float(doc.get('totalHoldings')):.2f}",
            "Cash": f"{_to_float(doc.get('totalCash')):.2f}",
            "AUM": f"{aum:.2f}",
            "Rate": f"{doc_rate:.2f}%",
            "Fee": f"{daily_fee:.2f}",
            "P&L": f"{_to_float(doc.get('totalPnl')):.2f}",
            "Selected": "true" if doc.get("selected", True) else "false",
        })

    rows.sort(key=lambda x: x["Account"])

    print(f"\nDaily Fee Report (Rate: {rate}%/annum, Date: {date_str})")
    print("-" * 165)
    print(f"{'Account':<15} {'AccountName':<25} {'Date':<12} {'Holdings':<15} {'Cash':<15} {'AUM':<15} {'Rate':<8} {'Fee':<15} {'P&L':<15} {'Selected':<10}")
    print("-" * 165)
    for row in rows:
        print(f"{row['Account']:<15} {row['AccountName']:<25} {row['Date']:<12} {row['Holdings']:<15} {row['Cash']:<15} {row['AUM']:<15} {row['Rate']:<8} {row['Fee']:<15} {row['P&L']:<15} {row['Selected']:<10}")
    print("-" * 165)
    print(f"Total accounts: {len(rows)}\n")


def main() -> None:
    """Collect AUM and P&L data for each client and upsert into MongoDB."""
    clients = _fetch_clients()

    if not clients:
        print("No client documents found in MongoDB 'clients' collection; exiting.")
        return

    print(f"Fetched {len(clients)} client document(s) from MongoDB 'clients' collection.")

    portfolios = _fetch_portfolios()
    print(f"Fetched {len(portfolios)} portfolio document(s) from MongoDB 'portfolios' collection.")

    instruments = _fetch_instruments()
    print(f"Fetched {len(instruments)} instrument(s) from MongoDB '{INSTRUMENTS_COLLECTION}' collection.")

    price_map = _build_instrument_price_map(instruments)
    print(f"Built price map for {len(price_map)} instrument(s) with valid timeseries prices.")

    seen_account_numbers = set()
    skipped_missing_account = 0
    aum_documents: List[Dict[str, Any]] = []

    now = datetime.now(timezone.utc)

    for client in clients:
        account_number = str(client.get("accountNumber", ""))
        if not account_number:
            skipped_missing_account += 1
            continue
        if account_number in seen_account_numbers:
            continue
        seen_account_numbers.add(account_number)

        portfolio = portfolios.get(account_number, {})
        nav_timeseries = portfolio.get("nav_timeseries") or []
        # total_holdings is the latest NAV value from the portfolio's nav_timeseries
        total_holdings = _latest_nav_on_or_before(nav_timeseries, now)
        if total_holdings is None:
            total_holdings = 0.0

        aum_doc = _build_aum_document(client, portfolio, total_holdings=total_holdings)

        holdings = portfolio.get("holdings", []) or []
        total_pnl = _calculate_account_pnl(holdings, instruments, price_map, now)
        aum_doc["totalPnl"] = round(total_pnl, 2)

        aum_documents.append(aum_doc)

    print(f"\nSummary: {len(aum_documents)} AUM/P&L record(s) built, {skipped_missing_account} skipped (missing accountNumber).")

    if aum_documents:
        insert_aum(aum_documents)
    else:
        print("No AUM documents collected; skipping database insert.")

    _backfill_missing_fee_fields()
    _backfill_missing_pnl_fields()
    generate_daily_fee_report()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
