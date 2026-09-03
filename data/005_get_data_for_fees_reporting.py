"""Assets under management (AUM) collector for fees reporting.

Reads the MongoDB ``clients`` and ``portfolios`` collections and produces a
per-client AUM snapshot:

    total_holdings + total_cash = total_aum

where:

- ``total_holdings`` is the sum of ``marketValue`` across all equity holdings.
- ``total_cash`` is the sum of ``bankBalance`` across all cash bank accounts.

The resulting records are upserted into the ``fees`` collection
keyed by ``accountNumber``.  Each document is stamped with ``generatedAt``
so that historical AUM snapshots can be retained.

Typical usage::

    python data/get_data_for_fees.py

Environment variables required:

    MONGODB_SRV: MongoDB connection string.
    DATABASE_NAME: Database name (defaults to ``VESTRA_PROD``).
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

load_dotenv()

logger = logging.getLogger(__name__)

DATABASE_NAME = os.getenv("DATABASE_NAME", "VESTRA_PROD")
CLIENTS_COLLECTION = "clients"
PORTFOLIOS_COLLECTION = "portfolios"
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
        documents = list(collection.find({}, {"_id": 0}))
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


def _to_float(value: Any) -> float:
    """Safely convert a value to float.

    Args:
        value: Value to convert.

    Returns:
        Numeric float value, or ``0.0`` if the value is missing or not numeric.
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
    return 0.0


def _calculate_total_holdings(holdings: List[Dict[str, Any]]) -> float:
    """Sum the market value of all equity holdings for an account.

    Args:
        holdings: List of holding dicts from the portfolio document.

    Returns:
        Total holdings value as a float.
    """
    total = 0.0
    if not isinstance(holdings, list):
        return total
    for holding in holdings:
        if not isinstance(holding, dict):
            continue
        total += _to_float(holding.get("marketValue"))
    return total


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


def _build_aum_document(client: Dict[str, Any], portfolio: Dict[str, Any]) -> Dict[str, Any]:
    """Build an AUM document for a single client/account.

    Args:
        client: Client document dict from the ``clients`` collection.
        portfolio: Portfolio document dict from the ``portfolios`` collection,
            or an empty dict if the client has no portfolio.

    Returns:
        AUM document dict containing ``accountNumber``, identity fields,
        ``totalHoldings``, ``totalCash``, ``totalAUM``, and ``generatedAt``.
    """
    account_number = str(client.get("accountNumber", ""))
    holdings = portfolio.get("holdings", []) or []
    cash_data = portfolio.get("cash") or {}

    total_holdings = _calculate_total_holdings(holdings)
    total_cash = _calculate_total_cash(cash_data)
    total_aum = total_holdings + total_cash

    return {
        "accountNumber": account_number,
        "accountName": client.get("accountName", ""),
        "totalHoldings": round(total_holdings, 2),
        "totalCash": round(total_cash, 2),
        "totalAUM": round(total_aum, 2),
        "selected": True,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
    }


def _generate_run_id() -> str:
    """Generate a unique run ID based on the current UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def insert_aum(documents: List[Dict[str, Any]]) -> None:
    """Insert AUM documents into the ``fees`` collection.

    Each document is inserted as a new record.  Duplicate ``accountNumber``
    values are allowed so that historical snapshots accumulate over time.

    A document is considered a duplicate if another document with the same
    ``accountNumber`` and ``generatedAt`` date already exists in the
    collection.  Such duplicates are skipped.

    Args:
        documents: List of AUM document dicts to insert.

    Raises:
        RuntimeError: If the MongoDB connection or insert operation fails.
    """
    if not documents:
        print("No AUM documents to insert.")
        return

    client = _get_mongo_client()
    try:
        db = client[DATABASE_NAME]
        collection = db[FEES_COLLECTION]

        inserted = 0
        skipped = 0
        for doc in documents:
            account_number = doc.get("accountNumber")
            generated_at = doc.get("generatedAt", "")
            date_part = generated_at.split("T")[0] if generated_at else ""

            if account_number and date_part:
                existing = collection.find_one({
                    "accountNumber": account_number,
                    "generatedAt": {"$regex": f"^{date_part}"},
                })
                if existing:
                    skipped += 1
                    continue

            collection.insert_one(doc)
            inserted += 1

        print(f"Inserted {inserted} AUM document(s) into '{DATABASE_NAME}.{FEES_COLLECTION}'.")
        if skipped:
            print(f"Skipped {skipped} duplicate AUM document(s).")
    except PyMongoError as e:
        raise RuntimeError(f"Failed to insert AUM documents into MongoDB: {e}") from e
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
        annual_fee = aum * (rate / 100)
        daily_fee = annual_fee / days_in_year

        rows.append({
            "Account": str(doc.get("accountNumber", "")),
            "AccountName": doc.get("accountName", ""),
            "Date": date_str,
            "Holdings": f"{_to_float(doc.get('totalHoldings')):.2f}",
            "Cash": f"{_to_float(doc.get('totalCash')):.2f}",
            "AUM": f"{aum:.2f}",
            "Rate": f"{rate:.2f}%",
            "Fee": f"{daily_fee:.2f}",
            "Selected": "true" if doc.get("selected", True) else "false",
        })

    rows.sort(key=lambda x: x["Account"])

    print(f"\nDaily Fee Report (Rate: {rate}%/annum, Date: {date_str})")
    print("-" * 145)
    print(f"{'Account':<15} {'AccountName':<25} {'Date':<12} {'Holdings':<15} {'Cash':<15} {'AUM':<15} {'Rate':<8} {'Fee':<15} {'Selected':<10}")
    print("-" * 145)
    for row in rows:
        print(f"{row['Account']:<15} {row['AccountName']:<25} {row['Date']:<12} {row['Holdings']:<15} {row['Cash']:<15} {row['AUM']:<15} {row['Rate']:<8} {row['Fee']:<15} {row['Selected']:<10}")
    print("-" * 145)
    print(f"Total accounts: {len(rows)}\n")


def main() -> None:
    """Collect AUM data for each client and upsert into MongoDB."""
    clients = _fetch_clients()

    if not clients:
        print("No client documents found in MongoDB 'clients' collection; exiting.")
        return

    print(f"Fetched {len(clients)} client document(s) from MongoDB 'clients' collection.")

    portfolios = _fetch_portfolios()
    print(f"Fetched {len(portfolios)} portfolio document(s) from MongoDB 'portfolios' collection.")

    seen_account_numbers = set()
    skipped_missing_account = 0
    aum_documents: List[Dict[str, Any]] = []

    for client in clients:
        account_number = str(client.get("accountNumber", ""))
        if not account_number:
            skipped_missing_account += 1
            continue
        if account_number in seen_account_numbers:
            continue
        seen_account_numbers.add(account_number)

        portfolio = portfolios.get(account_number, {})
        aum_doc = _build_aum_document(client, portfolio)
        aum_documents.append(aum_doc)

    print(f"\nSummary: {len(aum_documents)} AUM record(s) built, {skipped_missing_account} skipped (missing accountNumber).")

    if aum_documents:
        insert_aum(aum_documents)
    else:
        print("No AUM documents collected; skipping database insert.")

    generate_daily_fee_report()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
