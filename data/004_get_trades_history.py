"""HTTP client for the Morrison Securities Equity Holding Transactions API.

Imports the shared base configuration from ``configuration`` and exposes a
``fetch_equity_holding_transactions()`` helper that performs a GET request against the
``equityholdingtransactions/v1`` endpoint.  The module is intentionally side-effect-free
on import, so it can be safely reused by tests or other Python modules.

When executed directly, this module fetches equity holding transactions for each
active client account in the MongoDB ``clients`` collection, normalises the records,
and upserts them into the MongoDB ``trades`` collection.
"""

import json
import os
from typing import Any, Dict, List
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from configuration import (
    BASE_URL,
    HEADERS,
    _raise_for_status,
)

# Load environment variables from ``.env`` in the project root.
load_dotenv()

# API endpoint path for equity holding transactions.
ENDPOINT_PATH: str = "/equityholdingtransactions/v1"

# Full request URL composed from shared base host and endpoint path.
API_URL: str = BASE_URL + ENDPOINT_PATH


def _build_url(scope_item: Dict[str, Any]) -> str:
    """Build the equity holding transactions API URL from a scoping item."""
    url = API_URL
    params: Dict[str, Any] = {}
    if scope_item.get("organisationCode"):
        params["organisationCode"] = scope_item["organisationCode"]
    if scope_item.get("branchCode"):
        params["branchCode"] = scope_item["branchCode"]
    if scope_item.get("adviserCode"):
        params["adviserCode"] = scope_item["adviserCode"]
    if scope_item.get("accountNumber"):
        params["accountNumber"] = scope_item["accountNumber"]
    if scope_item.get("startDate"):
        params["startDate"] = scope_item["startDate"]
    if scope_item.get("endDate"):
        params["endDate"] = scope_item["endDate"]

    if params:
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{API_URL}?{query_string}"

    return url


def fetch_equity_holding_transactions(scope_item: Dict[str, Any]) -> Dict[str, Any]:
    """Fetch equity holding transactions from the Morrison Securities API.

    Args:
        scope_item: Dictionary containing scoping parameters such as
            ``organisationCode``, ``branchCode``, ``adviserCode``,
            ``accountNumber``, ``startDate``, and ``endDate``.

    Returns:
        Parsed JSON response as a dictionary.

    Raises:
        RuntimeError: If the API key is missing, the response is empty,
            the response body is not valid JSON, or the server returns
            an HTTP error.
    """
    if not HEADERS["x-api-key"]:
        raise RuntimeError("MORRISON_ACCESS_KEY is missing. Check your .env file.")

    url = _build_url(scope_item)
    print(f"Requesting: {url}")

    request = Request(url, headers=HEADERS, method="GET")

    try:
        with urlopen(request) as response:
            raw = response.read().decode("utf-8", errors="replace")

            if not raw.strip():
                raise RuntimeError("API returned an empty response.")

            try:
                return json.loads(raw)
            except json.JSONDecodeError as e:
                content_type = response.headers.get("Content-Type", "unknown")
                status = getattr(response, "status", "unknown")
                raise RuntimeError(
                    f"API returned non-JSON content (status={status}, "
                    f"Content-Type={content_type}). URL: {url} "
                    f"Response preview: {raw[:200]}"
                ) from e
    except HTTPError as e:
        _raise_for_status(e)

    raise RuntimeError("Unexpected state: request completed without returning data.")


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


def _fetch_active_client_documents_from_mongo() -> List[Dict[str, Any]]:
    """Fetch active client documents from the MongoDB ``clients`` collection.

    Returns:
        List of active client document dicts, each containing at least
        ``accountNumber`` and the adviser/branch/organisation scope fields.

    Raises:
        RuntimeError: If the MongoDB connection or query fails.
    """
    db_name = os.getenv("DATABASE_NAME", "VESTRA_PROD")
    client = _get_mongo_client()
    try:
        db = client[db_name]
        collection = db["clients"]
        documents = list(collection.find({}, {"_id": 0}))
        return [doc for doc in documents if isinstance(doc, dict)]
    except PyMongoError as e:
        raise RuntimeError(f"Failed to fetch active clients from MongoDB: {e}") from e
    finally:
        client.close()


def _normalize_trades_documents(data: Any) -> List[Dict[str, Any]]:
    """Normalize API response data into a list of trade document dicts.

    The equity holding transactions API wraps results in a top-level object
    whose ``data``/``Data``/``results``/``items`` field contains the array of
    transaction records.  This helper unwraps that envelope and returns only
    the trade dicts.

    Args:
        data: Parsed JSON from ``fetch_equity_holding_transactions()``.

    Returns:
        List of trade dictionaries.
    """
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        for key in ("data", "Data", "results", "items", "transactions", "equityHoldingTransactions"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [data]
    return []


def _to_float(value: Any) -> float:
    """Safely convert a value to float."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def _enrich_trades_with_pnl(trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Enrich trades with realized P&L using FIFO lot matching.

    Matches SELL transactions against the earliest unmatched BUY lot for
    the same ``accountNumber`` + ``securityCode``.  Matching is done at
    the quantity level so partial sells are handled correctly.

    Transaction costs are included in the cost basis for BUYs and
    deducted from proceeds for SELLs so that P&L reflects true economic
    outcome.

    Adds the following fields to each trade document:
    - ``costBasis``: original purchase value for the matched lot
    - ``sellValue``: sale value for SELL transactions
    - ``pnl``: profit or loss amount
    - ``pnlPercent``: profit or loss percentage
    - ``matchedBuyDate``: transactionDate of the matched buy lot

    Args:
        trades: List of trade dicts to enrich.

    Returns:
        Enriched list of trade dicts.
    """
    enriched: List[Dict[str, Any]] = []
    lots: Dict[str, List[Dict[str, Any]]] = {}

    for trade in trades:
        enriched_trade = dict(trade)
        account_number = enriched_trade.get("accountNumber")
        security_code = enriched_trade.get("securityCode")
        transaction_code = str(enriched_trade.get("transactionCode", "")).upper()
        transaction_value = _to_float(enriched_trade.get("transactionValue"))
        transaction_cost = _to_float(enriched_trade.get("transactionCost"))
        quantity = _to_float(enriched_trade.get("quantity"))
        transaction_date = enriched_trade.get("transactionDate")

        key = f"{account_number}::{security_code}"
        if key not in lots:
            lots[key] = []

        if transaction_code == "BUY":
            enriched_trade["costBasis"] = transaction_value + transaction_cost
            enriched_trade["sellValue"] = None
            enriched_trade["pnl"] = None
            enriched_trade["pnlPercent"] = None
            enriched_trade["matchedBuyDate"] = transaction_date
            lots[key].append(enriched_trade)
        elif transaction_code == "SELL":
            sell_value = abs(transaction_value) - transaction_cost
            sell_quantity = quantity if quantity and quantity > 0 else None

            cost_basis = 0.0
            matched_buy_dates: List[str] = []
            remaining_sell = sell_quantity

            if remaining_sell is not None:
                while remaining_sell > 0 and lots[key]:
                    lot = lots[key][0]
                    lot_cost = _to_float(lot.get("costBasis"))
                    lot_quantity = _to_float(lot.get("quantity"))
                    if lot_quantity <= 0:
                        lots[key].pop(0)
                        continue

                    matched_quantity = min(remaining_sell, lot_quantity)
                    matched_cost = (matched_quantity / lot_quantity) * lot_cost if lot_quantity else 0.0
                    cost_basis += matched_cost
                    matched_buy_dates.append(str(lot.get("transactionDate")))

                    lot_quantity -= matched_quantity
                    remaining_sell -= matched_quantity

                    if lot_quantity <= 0:
                        lots[key].pop(0)
                    else:
                        lot["quantity"] = lot_quantity
                        lot["costBasis"] = lot_cost - matched_cost
            else:
                if lots[key]:
                    lot = lots[key].pop(0)
                    lot_cost = _to_float(lot.get("costBasis"))
                    cost_basis = lot_cost
                    matched_buy_dates.append(str(lot.get("transactionDate")))

            if cost_basis > 0:
                pnl = sell_value - cost_basis
                pnl_percent = (pnl / cost_basis * 100)
                enriched_trade["costBasis"] = cost_basis
                enriched_trade["sellValue"] = sell_value
                enriched_trade["pnl"] = pnl
                enriched_trade["pnlPercent"] = pnl_percent
                enriched_trade["matchedBuyDate"] = ", ".join(matched_buy_dates) if matched_buy_dates else None
            else:
                enriched_trade["costBasis"] = cost_basis
                enriched_trade["sellValue"] = sell_value
                enriched_trade["pnl"] = None
                enriched_trade["pnlPercent"] = None
                enriched_trade["matchedBuyDate"] = None
        else:
            enriched_trade["costBasis"] = None
            enriched_trade["sellValue"] = None
            enriched_trade["pnl"] = None
            enriched_trade["pnlPercent"] = None
            enriched_trade["matchedBuyDate"] = None

        enriched.append(enriched_trade)

    return enriched


def _get_trade_filter(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Build a unique filter query for a trade document.

    Uses ``reference`` when available, otherwise falls back to a compound
    of ``accountNumber`` and ``transactionDate``, or finally to
    ``accountNumber`` alone.
    """
    for field in ("reference", "transactionId", "tradeId", "transactionID", "tradeID"):
        value = doc.get(field)
        if value:
            return {"accountNumber": doc.get("accountNumber"), field: value}

    if doc.get("accountNumber") and doc.get("transactionDate"):
        return {
            "accountNumber": doc.get("accountNumber"),
            "transactionDate": doc.get("transactionDate"),
        }

    account_number = doc.get("accountNumber")
    if account_number:
        return {"accountNumber": account_number}

    return {}


def upsert_trades(documents: List[Dict[str, Any]]) -> None:
    """Upsert trade documents into the ``trades`` collection.

    Each document is matched using a unique trade identifier when available.
    Documents without an identifiable key are inserted as-is.

    After upserting, any documents in the collection whose ``accountNumber``
    is not present in the current active batch are removed, ensuring that
    trades for inactive accounts are purged from the local store.

    Args:
        documents: List of trade dicts to upsert.

    Raises:
        RuntimeError: If the MongoDB connection or upsert operation fails.
    """
    if not documents:
        print("No trade documents to upsert.")
        return

    db_name = os.getenv("DATABASE_NAME", "VESTRA_PROD")
    client = _get_mongo_client()
    try:
        db = client[db_name]
        collection = db["trades"]

        collection.create_index("accountNumber", background=True)

        upserted = 0
        active_account_numbers: set = set()
        total = len(documents)
        print(f"Starting upsert of {total} trade document(s) into '{db_name}.trades'.")
        for idx, doc in enumerate(documents, start=1):
            filter_query = _get_trade_filter(doc)
            account_number = doc.get("accountNumber")
            if account_number:
                active_account_numbers.add(account_number)

            if filter_query:
                collection.replace_one(filter_query, doc, upsert=True)
            else:
                collection.insert_one(doc)
            upserted += 1

            if idx % 50 == 0 or idx == total:
                print(f"Upsert progress: {idx}/{total} trade document(s) processed.")

        if active_account_numbers:
            print(f"Removing trades for inactive account(s) from '{db_name}.trades'...")
            delete_filter = {
                "accountNumber": {
                    "$nin": list(active_account_numbers),
                    "$exists": True,
                    "$ne": None,
                }
            }
            delete_result = collection.delete_many(delete_filter)
            if delete_result.deleted_count:
                print(f"Removed {delete_result.deleted_count} trade document(s) for inactive account(s) from '{db_name}.trades'.")
            else:
                print(f"No inactive trade document(s) to remove from '{db_name}.trades'.")

        print(f"Upserted {upserted} trade document(s) into '{db_name}.trades'.")
    except PyMongoError as e:
        raise RuntimeError(f"Failed to upsert trades into MongoDB: {e}") from e
    finally:
        client.close()


def main() -> None:
    """Fetch equity holding transactions for each active client account and upsert into MongoDB."""
    client_documents = _fetch_active_client_documents_from_mongo()

    if not client_documents:
        print("No active client documents found in MongoDB 'clients' collection; exiting.")
        return

    print(f"Fetched {len(client_documents)} active client document(s) from MongoDB 'clients' collection.")

    seen_account_numbers = set()
    skipped_missing_account = 0
    processed = 0
    failed: List[str] = []
    all_trades: List[Dict[str, Any]] = []

    for client in client_documents:
        account_number = str(client.get("accountNumber", ""))
        if not account_number:
            skipped_missing_account += 1
            continue
        if account_number in seen_account_numbers:
            continue
        seen_account_numbers.add(account_number)

        adviser_code = client.get("adviserCode")
        call_item = dict(client)

        try:
            data = fetch_equity_holding_transactions(call_item)
        except RuntimeError as e:
            print(f"ERROR fetching trades for accountNumber={account_number}: {e}")
            failed.append(account_number)
            continue

        header = f"\n--- Trades for adviserCode={adviser_code or 'N/A'} accountNumber={account_number} ---\n"
        print(header, end="")
        print(json.dumps(data, indent=2, ensure_ascii=False))

        trades = _normalize_trades_documents(data)
        if trades:
            for trade in trades:
                trade["accountNumber"] = account_number
            all_trades.extend(_enrich_trades_with_pnl(trades))
        else:
            all_trades.append({"accountNumber": account_number})

        processed += 1

    print(f"\nSummary: {processed} account(s) processed, {skipped_missing_account} skipped (missing accountNumber), {len(failed)} failed: {failed}")

    if all_trades:
        print(f"Total trades collected: {len(all_trades)}. Proceeding to upsert.")
        upsert_trades(all_trades)
    else:
        print("No trade documents collected; skipping database upsert.")


if __name__ == "__main__":
    main()
