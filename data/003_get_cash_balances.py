"""HTTP client for the Morrison Securities Account Cash Balance API.

Imports the shared base configuration from ``configuration`` and exposes a
``fetch_account_cash_balance()`` helper that performs a GET request against the
``cashbalance/v1`` endpoint.  The module is intentionally side-effect-free
on import, so it can be safely reused by tests or other Python modules.

When executed directly, this module fetches cash balance for each client
account returned by the MongoDB ``clients`` collection, normalises the records,
and upserts them into the MongoDB ``portfolios`` collection as a sibling
``cash`` node alongside the existing ``holdings`` field.

Typical usage::

    from data.003_get_cash_balance import fetch_account_cash_balance, upsert_cash_balances

    scope = {
        "organisationCode": "TPSSCS",
        "branchCode": "SO",
        "adviserCode": "VO2",
        "accountNumber": "12345",
    }
    cash = fetch_account_cash_balance(scope)
    upsert_cash_balances(cash)
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

# API endpoint path for account cash balance.
ENDPOINT_PATH: str = "/cashbalances/v1"

# Full request URL composed from shared base host and endpoint path.
API_URL: str = BASE_URL + ENDPOINT_PATH


def _build_url(scope_item: Dict[str, Any]) -> str:
    """Build the account cash balance API URL from a scoping item."""
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

    if params:
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{API_URL}?{query_string}"

    return url


def fetch_account_cash_balance(scope_item: Dict[str, Any]) -> Dict[str, Any]:
    """Fetch account cash balance from the Morrison Securities API.

    Args:
        scope_item: Dictionary containing scoping parameters such as
            ``organisationCode``, ``branchCode``, ``adviserCode``,
            and ``accountNumber``.

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


def _normalize_cash_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of the cash document with derived fields.

    The original document is not mutated.

    Args:
        doc: Single cash balance dict from the API response.

    Returns:
        New dict containing both original fields and derived cash fields.
    """
    return dict(doc)


def _normalize_cash_documents(data: Any) -> List[Dict[str, Any]]:
    """Normalize API response data into a list of document dicts.

    The cash balance API wraps results in a top-level object whose
    ``data``/``Data``/``results``/``items`` field contains the array of
    cash balance records.  This helper unwraps that envelope and returns only
    the cash balance dicts.

    Args:
        data: Parsed JSON from ``fetch_account_cash_balance()``.

    Returns:
        List of cash balance dictionaries.
    """
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        for key in ("data", "Data", "results", "items", "cashBalance", "cashBalances"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [data]
    return []


def upsert_cash_balances(documents: List[Dict[str, Any]]) -> None:
    """Upsert cash balance documents into the ``portfolios`` collection.

    Each document is expected to represent one account's cash balance and is
    matched by ``accountNumber``.  Documents without ``accountNumber`` are
    inserted as-is.

    The ``cash`` field is set using ``$set`` so that existing ``holdings``
    data is preserved.

    After upserting, any documents in the collection whose ``accountNumber``
    is not present in the current batch are NOT removed, because cash balances
    are a subset of portfolio data and holdings may exist independently.

    Args:
        documents: List of cash balance dicts to upsert.

    Raises:
        RuntimeError: If the MongoDB connection or upsert operation fails.
    """
    if not documents:
        print("No cash balance documents to upsert.")
        return

    db_name = os.getenv("DATABASE_NAME", "VESTRA_PROD")
    client = _get_mongo_client()
    try:
        db = client[db_name]
        collection = db["portfolios"]

        upserted = 0
        for doc in documents:
            cash_data = _normalize_cash_document(doc)
            account_number = cash_data.get("accountNumber")
            if not account_number:
                continue

            filter_query: Dict[str, Any] = {"accountNumber": account_number}
            update = {
                "$set": {"cash": cash_data},
                "$setOnInsert": {"holdings": []},
            }

            collection.update_one(filter_query, update, upsert=True)
            upserted += 1

        print(f"Upserted {upserted} cash balance document(s) into '{db_name}.portfolios'.")
    except PyMongoError as e:
        raise RuntimeError(f"Failed to upsert cash balances into MongoDB: {e}") from e
    finally:
        client.close()


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
    documents_by_account: Dict[str, List[Dict[str, Any]]] = {}

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
            data = fetch_account_cash_balance(call_item)
        except RuntimeError as e:
            print(f"ERROR fetching cash balance for accountNumber={account_number}: {e}")
            failed.append(account_number)
            continue

        header = f"\n--- Cash Balance for adviserCode={adviser_code or 'N/A'} accountNumber={account_number} ---\n"
        print(header, end="")
        print(json.dumps(data, indent=2, ensure_ascii=False))

        documents = _normalize_cash_documents(data)
        if documents:
            documents_by_account[account_number] = documents
        else:
            documents_by_account[account_number] = [{}]

        processed += 1

    print(f"\nSummary: {processed} account(s) processed, {skipped_missing_account} skipped (missing accountNumber), {len(failed)} failed: {failed}")

    cash_documents: List[Dict[str, Any]] = []
    for account_number, cash_list in documents_by_account.items():
        for cash_doc in cash_list:
            cash_doc["accountNumber"] = account_number
            cash_documents.append(cash_doc)

    if cash_documents:
        upsert_cash_balances(cash_documents)
    else:
        print("No cash balance documents collected; skipping database upsert.")


if __name__ == "__main__":
    main()
