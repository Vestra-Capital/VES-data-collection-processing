"""HTTP client for the Morrison Securities Account Equity Holdings API.

Imports the shared base configuration from ``configuration`` and exposes a
``fetch_account_equity_holdings()`` helper that performs a GET request against the
``equityholdings/v1`` endpoint.  The module is intentionally side-effect-free
on import, so it can be safely reused by tests or other Python modules.

When executed directly, this module fetches equity holdings for each client
account returned by the MongoDB ``clients`` collection, normalises the records,
and upserts them into the MongoDB ``portfolios`` collection.

Typical usage::

    from data.002_get_portfolio_holdings import fetch_account_equity_holdings, upsert_portfolios

    scope = {
        "organisationCode": "TPSSCS",
        "branchCode": "SO",
        "adviserCode": "VO2",
        "accountNumber": "12345",
        "includeZeroHoldings": False,
    }
    holdings = fetch_account_equity_holdings(scope)
    upsert_portfolios(holdings)
"""

import json
import os
from collections import defaultdict
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

# API endpoint path for account equity holdings.
ENDPOINT_PATH: str = "/equityholdings/v1"

# Full request URL composed from shared base host and endpoint path.
API_URL: str = BASE_URL + ENDPOINT_PATH


def _build_url(scope_item: Dict[str, Any]) -> str:
    """Build the account equity holdings API URL from a scoping item."""
    url = API_URL
    params: Dict[str, Any] = {}
    if scope_item.get("organisationCode"):
        params["organisationCode"] = scope_item["organisationCode"]
    if scope_item.get("branchCode"):
        params["branchCode"] = scope_item["branchCode"]
    if scope_item.get("adviserCode"):
        params["adviserCode"] = scope_item["adviserCode"]
    account_number = scope_item.get("accountNumber")
    if account_number:
        params["accountNumber"] = account_number
    if "includeZeroHoldings" in scope_item:
        params["includeZeroHoldings"] = "true" if scope_item["includeZeroHoldings"] else "false"

    if params:
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{API_URL}?{query_string}"

    return url


def fetch_account_equity_holdings(scope_item: Dict[str, Any]) -> Dict[str, Any]:
    """Fetch account equity holdings from the Morrison Securities API.

    Args:
        scope_item: Dictionary containing scoping parameters such as
            ``organisationCode``, ``branchCode``, ``adviserCode``,
            ``accountNumber``, and ``includeZeroHoldings``.

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


def save_to_txt(data: Dict[str, Any], filepath: str) -> None:
    """Save JSON data to a plain text file.

    .. note::
        This helper is currently unused by the pipeline but is retained
        for ad-hoc debugging or future export requirements.
    """
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(json.dumps(data, indent=2, ensure_ascii=False))


def _extract_scope_items(config: Any) -> list:
    """Extract a list of scope item dicts from the configuration API response."""
    items: list = []
    if isinstance(config, list):
        items = [item for item in config if isinstance(item, dict)]
    elif isinstance(config, dict):
        for value in config.values():
            if isinstance(value, list):
                items = [item for item in value if isinstance(item, dict)]
                if items:
                    break
        if not items and isinstance(config, dict):
            items = [config]
    return items


def _extract_client_documents(config: Any) -> List[Dict[str, Any]]:
    """Extract client documents from the 'clients' collection in the config.

    .. note::
        This helper is currently unused by the pipeline but is retained
        for potential future use when processing client-level data.
    """
    clients: List[Dict[str, Any]] = []
    if isinstance(config, dict):
        raw_clients = config.get("clients")
        if isinstance(raw_clients, list):
            clients = [client for client in raw_clients if isinstance(client, dict)]
    return clients


def _fetch_client_documents_from_mongo() -> List[Dict[str, Any]]:
    """Fetch client documents from the MongoDB ``clients`` collection.

    Returns:
        List of client document dicts, each containing at least ``accountNumber``.

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
        raise RuntimeError(f"Failed to fetch clients from MongoDB: {e}") from e
    finally:
        client.close()


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


_MARKET_CODE_YF_MAP = {
    "ASX": "AX",
}


def _normalize_holding_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of the holding document with derived fields.

    Adds the following derived fields:
    - ``marketCode_yf``: mapped from ``marketCode`` using a known exchange
      mapping (e.g. ``ASX`` -> ``AX``).
    - ``securityDescription``: reformatted from ALL-CAPS to title case.

    The original document is not mutated.

    Args:
        doc: Single equity holding dict from the API response.

    Returns:
        New dict containing both original fields and derived holding fields.
    """
    normalized = dict(doc)

    market_code = normalized.get("marketCode")
    if isinstance(market_code, str):
        normalized["marketCode_yf"] = _MARKET_CODE_YF_MAP.get(
            market_code.strip().upper(), market_code.strip()
        )

    security_description = normalized.get("securityDescription")
    if isinstance(security_description, str) and security_description.strip():
        normalized["securityDescription"] = security_description.strip().title()

    return normalized


def _normalize_holdings_documents(data: Any) -> List[Dict[str, Any]]:
    """Normalize API response data into a list of document dicts.

    The equity holdings API wraps results in a top-level object whose
    ``data``/``Data``/``results``/``items`` field contains the array of
    holding records.  This helper unwraps that envelope and returns only
    the holding dicts.

    Args:
        data: Parsed JSON from ``fetch_account_equity_holdings()``.

    Returns:
        List of equity holding dictionaries.
    """
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        for key in ("data", "Data", "results", "items", "holdings", "equityHoldings"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [data]
    return []


def upsert_portfolios(documents: List[Dict[str, Any]], active_account_numbers: set = None) -> None:
    """Upsert portfolio documents into the ``portfolios`` collection.

    Each document is expected to represent one account and is matched by
    ``accountNumber``.  Documents without ``accountNumber`` are inserted
    as-is.

    After upserting, any documents in the collection whose ``accountNumber``
    is not present in ``active_account_numbers`` are removed, ensuring that
    inactive accounts or accounts with empty holdings are purged from the
    local store.

    Args:
        documents: List of portfolio document dicts to upsert.
        active_account_numbers: Set of account numbers that are considered
            active and have non-empty holdings.  If omitted, it is derived
            from the documents being upserted.

    Raises:
        RuntimeError: If the MongoDB connection or upsert operation fails.
    """
    if not documents:
        print("No documents to upsert.")
        return

    if active_account_numbers is None:
        active_account_numbers = {doc.get("accountNumber") for doc in documents if doc.get("accountNumber")}

    db_name = os.getenv("DATABASE_NAME", "VESTRA_PROD")
    client = _get_mongo_client()
    try:
        db = client[db_name]
        collection = db["portfolios"]

        upserted = 0
        for doc in documents:
            filter_query: Dict[str, Any] = {}
            account_number = doc.get("accountNumber")
            if account_number:
                filter_query["accountNumber"] = account_number

            if filter_query:
                collection.replace_one(filter_query, doc, upsert=True)
            else:
                collection.insert_one(doc)
            upserted += 1

        print(f"Upserted {upserted} document(s) into '{db_name}.portfolios'.")

        if active_account_numbers:
            print("Removing inactive/empty portfolio document(s)...")
            delete_filter = {
                "accountNumber": {
                    "$nin": list(active_account_numbers),
                    "$exists": True,
                    "$ne": None,
                }
            }
            delete_result = collection.delete_many(delete_filter)
            if delete_result.deleted_count:
                print(f"Removed {delete_result.deleted_count} inactive/empty portfolio document(s).")
            else:
                print("No inactive/empty portfolio document(s) to remove.")
    except PyMongoError as e:
        raise RuntimeError(f"Failed to upsert documents into MongoDB: {e}") from e
    finally:
        client.close()


def main() -> None:
    client_documents = _fetch_client_documents_from_mongo()

    if not client_documents:
        print("No client documents found in MongoDB 'clients' collection; exiting.")
        return

    print(f"Fetched {len(client_documents)} client document(s) from MongoDB 'clients' collection.")

    seen_account_numbers = set()
    skipped_missing_account = 0
    processed = 0
    failed: List[str] = []
    documents_by_account: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for client in client_documents:
        account_number = str(client.get("accountNumber", ""))
        if not account_number:
            skipped_missing_account += 1
            continue
        if account_number in seen_account_numbers:
            continue
        seen_account_numbers.add(account_number)

        adviser_code = client.get("adviserCode")

        for include_zero_holdings in (False,):
            call_item = dict(client)
            call_item["includeZeroHoldings"] = include_zero_holdings

            try:
                data = fetch_account_equity_holdings(call_item)
            except RuntimeError as e:
                print(f"ERROR fetching accountNumber={account_number}: {e}")
                failed.append(account_number)
                continue

            header = f"\n--- Result for adviserCode={adviser_code or 'N/A'} accountNumber={account_number} includeZeroHoldings={include_zero_holdings} ---\n"
            print(header, end="")
            print(json.dumps(data, indent=2, ensure_ascii=False))

            documents = _normalize_holdings_documents(data)
            if documents:
                documents_by_account[account_number].extend(
                    _normalize_holding_document(doc) for doc in documents
                )

        processed += 1

    print(f"\nSummary: {processed} account(s) processed, {skipped_missing_account} skipped (missing accountNumber), {len(failed)} failed: {failed}")

    active_account_numbers = set(documents_by_account.keys())

    portfolio_documents: List[Dict[str, Any]] = []
    for account_number, holdings in documents_by_account.items():
        portfolio_doc = {
            "accountNumber": account_number,
            "holdings": holdings,
        }
        portfolio_documents.append(portfolio_doc)

    if portfolio_documents:
        upsert_portfolios(portfolio_documents, active_account_numbers)
    else:
        print("No portfolio documents collected; skipping database upsert.")


if __name__ == "__main__":
    main()
