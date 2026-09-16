"""Recalculate and backfill fees reporting documents for past dates.

For each account, this script walks from a given start date through the latest
available date and uses the ``nav_timeseries`` stored on the ``portfolios``
collection to reconstruct ``totalHoldings`` for that date.  Current cash is
combined with those holdings to compute ``totalAUM``, ``collectedFees``, and
a generated ``fees`` collection document keyed by ``accountNumber`` and date.

Usage::

    python scripts/get_past_fees_recalc.py
    python scripts/get_past_fees_recalc.py --start-date 2026-08-01 --end-date 2026-09-15

Environment variables required:

    MONGODB_SRV — MongoDB connection string.
    DATABASE_NAME — Database name (defaults to ``VESTRA_PROD``).
    FEES_REPORT_RATE — Annual fee rate as a percentage (defaults to ``1.5``).
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
load_dotenv(os.path.join(project_root, ".env"))

logger = logging.getLogger(__name__)

DATABASE_NAME = os.getenv("DATABASE_NAME", "VESTRA_PROD")
FEES_REPORT_RATE = float(os.getenv("FEES_REPORT_RATE", "1.5"))
DEFAULT_START_DATE = date(2026, 1, 1)


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


def _is_leap_year(year: int) -> bool:
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def _days_in_year(d: date) -> int:
    return 366 if _is_leap_year(d.year) else 365


def fetch_clients(db_name: str) -> List[Dict[str, Any]]:
    client = get_mongo_client()
    try:
        db = client[db_name]
        return list(db["clients"].find({}, {"_id": 0}))
    except PyMongoError as e:
        raise RuntimeError(f"Failed to fetch clients: {e}") from e
    finally:
        client.close()


def fetch_portfolios(db_name: str) -> List[Dict[str, Any]]:
    client = get_mongo_client()
    try:
        db = client[db_name]
        return list(db["portfolios"].find({}))
    except PyMongoError as e:
        raise RuntimeError(f"Failed to fetch portfolios: {e}") from e
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
        raise RuntimeError(f"Failed to fetch instruments: {e}") from e
    finally:
        client.close()


def _sorted_nav_series(portfolio: Dict[str, Any]) -> List[Tuple[date, float]]:
    nav_timeseries = portfolio.get("nav_timeseries") or []
    if not isinstance(nav_timeseries, list):
        return []

    series: List[Tuple[date, float]] = []
    for entry in nav_timeseries:
        if not isinstance(entry, dict):
            continue
        raw_date = entry.get("date")
        nav = _to_float(entry.get("nav"))
        if not raw_date or nav <= 0:
            continue
        try:
            parsed_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            continue
        series.append((parsed_date, nav))

    series.sort(key=lambda item: item[0])
    return series


def _latest_nav_on_or_before(
    series: List[Tuple[date, float]],
    target_date: date,
) -> Optional[float]:
    latest_nav: Optional[float] = None
    for nav_date, nav in series:
        if nav_date <= target_date:
            latest_nav = nav
        else:
            break
    return latest_nav


def _build_symbol(holding: Dict[str, Any]) -> str:
    security_code = holding.get("securityCode", "")
    market_code_yf = holding.get("marketCode_yf", "")
    if security_code and market_code_yf:
        return f"{security_code}.{market_code_yf}"
    return ""


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
            raw_date = entry.get("date")
            price = entry.get("price")
            if raw_date and price is not None:
                date_prices[str(raw_date)] = _to_float(price)

        if date_prices:
            price_map[str(symbol)] = date_prices

    return price_map


def _latest_price_on_or_before(
    date_prices: Dict[str, float],
    target_date: date,
) -> Optional[float]:
    latest_price: Optional[float] = None
    target_str = target_date.strftime("%Y-%m-%d")

    sorted_dates = sorted(date_prices.keys())
    for date_str in sorted_dates:
        if date_str <= target_str:
            latest_price = date_prices[date_str]
        else:
            break

    return latest_price


def _calculate_account_pnl(
    holdings: List[Dict[str, Any]],
    price_map: Dict[str, Dict[str, float]],
    target_date: date,
) -> float:
    total_pnl = 0.0
    if not isinstance(holdings, list):
        return total_pnl

    for holding in holdings:
        if not isinstance(holding, dict):
            continue

        symbol = _build_symbol(holding)
        if not symbol:
            continue

        date_prices = price_map.get(symbol)
        if not date_prices:
            continue

        price = _latest_price_on_or_before(date_prices, target_date)
        if price is None or price <= 0:
            continue

        average_cost = _to_float(holding.get("averageCost"))
        total_holding = _to_float(holding.get("totalHolding"))

        cost_value = average_cost * total_holding
        market_value = price * total_holding
        total_pnl += market_value - cost_value

    return total_pnl


def _build_aum_document(
    client: Dict[str, Any],
    total_holdings: float,
    total_cash: float,
    rate: float,
    target_date: date,
    total_pnl: float = 0.0,
) -> Dict[str, Any]:
    total_aum = total_holdings + total_cash
    collected_fees = round((total_aum * (rate / 100)) / _days_in_year(target_date), 2)
    generated_at = datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        tzinfo=timezone.utc,
    ).isoformat()

    return {
        "accountNumber": str(client.get("accountNumber", "")),
        "accountName": client.get("accountName", ""),
        "totalHoldings": round(total_holdings, 2),
        "totalCash": round(total_cash, 2),
        "totalAUM": round(total_aum, 2),
        "feeReportRate": rate,
        "collectedFees": collected_fees,
        "totalPnl": round(total_pnl, 2),
        "selected": True,
        "generatedAt": generated_at,
    }


def upsert_aum_documents(db_name: str, documents: List[Dict[str, Any]]) -> None:
    if not documents:
        return

    client = get_mongo_client()
    try:
        db = client[db_name]
        collection = db["fees"]

        inserted = 0
        updated = 0
        total = len(documents)
        for index, doc in enumerate(documents, start=1):
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

            if index % 500 == 0 or index == total:
                print(
                    f"Upsert progress: {index}/{total} documents processed "
                    f"({inserted} inserted, {updated} updated so far)."
                )

        print(
            f"Upserted {inserted + updated} AUM document(s) into "
            f"'{db_name}.fees' ({inserted} inserted, {updated} updated)."
        )
    except PyMongoError as e:
        raise RuntimeError(f"Failed to upsert AUM documents: {e}") from e
    finally:
        client.close()


def generate_daily_fee_report(db_name: str, rate: float, report_date: date) -> None:
    client = get_mongo_client()
    try:
        db = client[db_name]
        collection = db["fees"]

        date_prefix = report_date.strftime("%Y-%m-%d")
        cursor = collection.find(
            {"generatedAt": {"$regex": f"^{date_prefix}"}},
            {"_id": 0},
        )
        documents = [doc for doc in cursor if isinstance(doc, dict)]
    except PyMongoError as e:
        raise RuntimeError(f"Failed to fetch AUM documents for report: {e}") from e
    finally:
        client.close()

    if not documents:
        print(f"No AUM documents found for {date_prefix}.")
        return

    rows: List[Dict[str, Any]] = []
    days_in_year = _days_in_year(report_date)
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
            "Date": date_prefix,
            "Holdings": f"{_to_float(doc.get('totalHoldings')):.2f}",
            "Cash": f"{_to_float(doc.get('totalCash')):.2f}",
            "AUM": f"{aum:.2f}",
            "Rate": f"{doc_rate:.2f}%",
            "Fee": f"{daily_fee:.2f}",
            "P&L": f"{_to_float(doc.get('totalPnl')):.2f}",
            "Selected": "true" if doc.get("selected", True) else "false",
        })

    rows.sort(key=lambda x: x["Account"])

    print(
        f"\nDaily Fee Report (Rate: {rate}%/annum, Date: {date_prefix})"
    )
    print("-" * 165)
    print(
        f"{'Account':<15} {'AccountName':<25} {'Date':<12} {'Holdings':<15} "
        f"{'Cash':<15} {'AUM':<15} {'Rate':<8} {'Fee':<15} {'P&L':<15} {'Selected':<10}"
    )
    print("-" * 165)
    for row in rows:
        print(
            f"{row['Account']:<15} {row['AccountName']:<25} {row['Date']:<12} "
            f"{row['Holdings']:<15} {row['Cash']:<15} {row['AUM']:<15} "
            f"{row['Rate']:<8} {row['Fee']:<15} {row['P&L']:<15} {row['Selected']:<10}"
        )
    print("-" * 165)
    print(f"Total accounts: {len(rows)}\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recalculate fees reporting documents for past dates."
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=DEFAULT_START_DATE.strftime("%Y-%m-%d"),
        help="Start date inclusive (YYYY-MM-DD). Defaults to 2026-08-01.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date inclusive (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=FEES_REPORT_RATE,
        help="Annual fee rate as a percentage. Defaults to FEES_REPORT_RATE from env.",
    )
    parser.add_argument(
        "--print-report",
        action="store_true",
        help="Print the daily fee report for the end date after upserting.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"Invalid start date: {args.start_date}. Expected YYYY-MM-DD.")

    end_date = date.today()
    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError(f"Invalid end date: {args.end_date}. Expected YYYY-MM-DD.")

    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date.")

    print(
        f"Recalculating fees from {start_date.isoformat()} to {end_date.isoformat()} "
        f"at rate {args.rate}%/annum."
    )

    clients = fetch_clients(DATABASE_NAME)
    if not clients:
        print("No client documents found; exiting.")
        return

    print(f"Fetched {len(clients)} client document(s).")
    clients_by_account = {
        str(client.get("accountNumber", "")): client for client in clients
    }

    portfolios = fetch_portfolios(DATABASE_NAME)
    print(f"Fetched {len(portfolios)} portfolio document(s).")

    portfolios_by_account: Dict[str, Dict[str, Any]] = {}
    for portfolio in portfolios:
        account_number = portfolio.get("accountNumber")
        if account_number:
            portfolios_by_account[str(account_number)] = portfolio

    all_symbols: set = set()
    for portfolio in portfolios:
        holdings = portfolio.get("holdings") or []
        if not isinstance(holdings, list):
            continue
        for holding in holdings:
            if not isinstance(holding, dict):
                continue
            security_code = holding.get("securityCode")
            market_code_yf = holding.get("marketCode_yf")
            if security_code and market_code_yf:
                all_symbols.add(f"{security_code}.{market_code_yf}")

    instruments = fetch_instruments(DATABASE_NAME, list(all_symbols))
    print(f"Fetched {len(instruments)} instrument(s).")
    price_map = _build_instrument_price_map(instruments)
    print(f"Built price map for {len(price_map)} instrument(s).")

    current = start_date
    aum_documents: List[Dict[str, Any]] = []
    processed_dates = 0

    while current <= end_date:
        print(f"Processing date: {current.isoformat()}")
        for account_number, client in clients_by_account.items():
            portfolio = portfolios_by_account.get(account_number, {})
            nav_series = _sorted_nav_series(portfolio)
            total_holdings = _latest_nav_on_or_before(nav_series, current) or 0.0

            cash_data = portfolio.get("cash") or {}
            if not isinstance(cash_data, dict):
                cash_data = {}
            bank_accounts = cash_data.get("bankAccounts") or []
            if not isinstance(bank_accounts, list):
                bank_accounts = []
            total_cash = sum(
                _to_float(bank_account.get("bankBalance")) for bank_account in bank_accounts
                if isinstance(bank_account, dict)
            )

            holdings = portfolio.get("holdings", []) or []
            total_pnl = _calculate_account_pnl(holdings, price_map, current)

            aum_doc = _build_aum_document(client, total_holdings, total_cash, args.rate, current, total_pnl=total_pnl)
            aum_documents.append(aum_doc)

        processed_dates += 1
        if processed_dates % 10 == 0 or current == end_date:
            print(
                f"Progress: {processed_dates} date(s) processed "
                f"({current.isoformat()}), {len(aum_documents)} document(s) built so far."
            )

        current += timedelta(days=1)

    print(
        f"Built {len(aum_documents)} AUM document(s) for "
        f"{processed_dates} date(s) x {len(clients_by_account)} account(s)."
    )

    if aum_documents:
        upsert_aum_documents(DATABASE_NAME, aum_documents)
    else:
        print("No AUM documents built; skipping database upsert.")

    accounts_with_zero_holdings: Dict[str, int] = {}
    total_docs = len(aum_documents)
    for idx, doc in enumerate(aum_documents, start=1):
        account = str(doc.get("accountNumber", ""))
        if _to_float(doc.get("totalHoldings")) == 0.0:
            accounts_with_zero_holdings[account] = accounts_with_zero_holdings.get(account, 0) + 1

        if idx % 1000 == 0 or idx == total_docs:
            print(f"Post-processing: {idx}/{total_docs} documents checked for zero holdings.")

    if accounts_with_zero_holdings:
        print("\nAccounts with zero totalHoldings for one or more dates:")
        for account, count in sorted(accounts_with_zero_holdings.items()):
            print(f"  {account}: {count} date(s)")

    total_pnl_summary = []
    total_accounts = len(clients_by_account)
    for idx, (account_number, client) in enumerate(clients_by_account.items(), start=1):
        portfolio = portfolios_by_account.get(account_number, {})
        holdings = portfolio.get("holdings", []) or []
        total_pnl = _calculate_account_pnl(holdings, price_map, end_date)
        total_pnl_summary.append((account_number, total_pnl))

        if idx % 10 == 0 or idx == total_accounts:
            print(f"P&L calculation: {idx}/{total_accounts} accounts processed.")

    print("\nTotal P&L per account (based on current holdings/prices):")
    print(f"{'Account':<15} {'Total P&L':>15}")
    print("-" * 32)
    for account_number, total_pnl in sorted(total_pnl_summary):
        print(f"{account_number:<15} {total_pnl:>15.2f}")

    if args.print_report:
        generate_daily_fee_report(DATABASE_NAME, args.rate, end_date)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
