"""HTTP client for the Morrison Securities Trading Accounts API.

Imports the shared base configuration from ``configuration`` and exposes a
``fetch_trading_accounts()`` helper that performs a GET request against the
``tradingaccounts/v2`` endpoint.  The module is intentionally side-effect-free
on import, so it can be safely reused by tests or other Python modules.

When executed directly, this module fetches trading accounts for each adviser
scope returned by the data-access API, enriches the records with derived
client fields, and upserts them into the MongoDB ``clients`` collection.

Typical usage::

    from data.get_trading_account import fetch_trading_accounts, upsert_clients

    scope = {
        "organisationCode": "TPSSCS",
        "branchCode": "SO",
        "adviserCode": "VO2",
        "includeInactive": True,
    }
    accounts = fetch_trading_accounts(scope)
    upsert_clients(accounts)
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
    fetch_data,
)

# Load environment variables from ``.env`` in the project root.
load_dotenv()

# API endpoint path for trading accounts.
ENDPOINT_PATH: str = "/tradingaccounts/v2"

# Full request URL composed from shared base host and endpoint path.
API_URL: str = BASE_URL + ENDPOINT_PATH


def _build_url(scope_item: Dict[str, Any]) -> str:
    """Build the trading accounts API URL from a scoping item.

    Args:
        scope_item: Dictionary of query parameters such as
            ``organisationCode``, ``branchCode``, ``adviserCode``,
            ``accountNumber``, and ``includeInactive``.

    Returns:
        Fully-qualified URL ready for use with ``urlopen``.
    """
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
    if "includeInactive" in scope_item:
        params["includeInactive"] = "true" if scope_item["includeInactive"] else "false"

    if params:
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{API_URL}?{query_string}"

    return url


def fetch_trading_accounts(scope_item: Dict[str, Any]) -> Dict[str, Any]:
    """Fetch trading accounts from the Morrison Securities API.

    Args:
        scope_item: Dictionary containing scoping parameters such as
            ``organisationCode``, ``branchCode``, ``adviserCode``,
            ``accountNumber``, and ``includeInactive``.

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


def _extract_scope_items(config: Any) -> list:
    """Extract a list of scope item dicts from the configuration API response.

    The data-access API can return either a list of scope items or a dict
    that contains a list under one of several possible keys.  This helper
    normalises both shapes into a simple list of dicts.

    Args:
        config: Parsed JSON from ``fetch_data()``.

    Returns:
        List of dictionaries representing adviser/branch/organisation scope.
    """
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


def _normalize_to_documents(data: Any) -> List[Dict[str, Any]]:
    """Normalize API response data into a list of document dicts.

    The trading accounts API wraps results in a top-level object whose
    ``Data`` field contains the array of account records.  This helper
    unwraps that envelope and returns only the account dicts.

    Args:
        data: Parsed JSON from ``fetch_trading_accounts()``.

    Returns:
        List of trading account dictionaries.
    """
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        for key in ("accounts", "tradingAccounts", "data", "Data", "results", "items"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [data]
    return []


def _extract_first_last_name(account_name: str) -> tuple:
    """Extract first and last name from an ``accountName`` string.

    Strips common titles, handles joint accounts by taking the first person,
    and falls back to using the full string for organisation/company names.

    Args:
        account_name: Raw ``accountName`` value from the Morrison API.

    Returns:
        Tuple of ``(first_name, last_name)``.
    """
    if not account_name:
        return "", ""

    name = account_name.strip()
    parts = name.split("+")
    primary = parts[0].strip()

    tokens = primary.split()
    titles = {"MR", "MRS", "MS", "DR", "PROF", "SIR", "MADAM", "LORD", "LADY"}
    filtered = [token for token in tokens if token.upper().rstrip(".") not in titles]

    if len(filtered) == 0:
        return "", ""
    if len(filtered) == 1:
        return filtered[0], ""
    return filtered[0], filtered[-1]


def _extract_email(doc: Dict[str, Any]) -> str:
    """Extract the best available email address from a trading account document.

    Preference order:
    1. ``emailAddress``
    2. ``contractNoteEmailAddress``

    Args:
        doc: Single trading account dict from the API response.

    Returns:
        Normalised email address string, or empty string if none available.
    """
    email = doc.get("emailAddress")
    if isinstance(email, str) and email.strip():
        return email.strip()
    contract_email = doc.get("contractNoteEmailAddress")
    if isinstance(contract_email, str) and contract_email.strip():
        return contract_email.strip()
    return ""


def _extract_telephone(doc: Dict[str, Any]) -> str:
    """Extract the best available telephone number from a trading account document.

    Preference order:
    1. ``mobilePhone``
    2. ``workPhone``
    3. ``homePhone``

    Args:
        doc: Single trading account dict from the API response.

    Returns:
        Normalised telephone string, or empty string if none available.
    """
    for field in ("mobilePhone", "workPhone", "homePhone"):
        value = doc.get(field)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _enrich_client_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of the trading account document with mandatory client fields.

    Adds the following derived fields:
    - ``first_name``
    - ``last_name``
    - ``email``
    - ``telephone``
    - ``client_category``

    The original document is not mutated.

    Args:
        doc: Raw trading account dict from the API.

    Returns:
        New dict containing both original fields and derived client fields.
    """
    enriched = dict(doc)
    account_name = enriched.get("accountName", "")
    first_name, last_name = _extract_first_last_name(account_name)
    enriched["first_name"] = first_name
    enriched["last_name"] = last_name
    enriched["email"] = _extract_email(enriched)
    enriched["telephone"] = _extract_telephone(enriched)
    enriched["client_category"] = "Wealth Management"
    return enriched


_PROTECTED_CLIENT_FIELDS = ("first_name", "last_name", "email", "telephone", "client_category")


def upsert_clients(documents: List[Dict[str, Any]]) -> None:
    """Upsert trading account documents into the ``clients`` collection.

    Each document is matched by ``accountNumber`` when present.  Documents
    without ``accountNumber`` are inserted as-is.  Every document is
    enriched with derived client fields before being written.

    For existing documents, protected client fields (``first_name``,
    ``last_name``, ``email``, ``telephone``, and ``client_category``) are
    preserved from the database and not overwritten by the API payload.

    Args:
        documents: List of trading account dicts to upsert.

    Raises:
        RuntimeError: If the MongoDB connection or upsert operation fails.
    """
    if not documents:
        print("No documents to upsert.")
        return

    db_name = os.getenv("DATABASE_NAME", "VESTRA_PROD")
    client = _get_mongo_client()
    try:
        db = client[db_name]
        collection = db["clients"]

        upserted = 0
        for doc in documents:
            enriched_doc = _enrich_client_document(doc)
            filter_query: Dict[str, Any] = {}
            account_number = enriched_doc.get("accountNumber")
            if account_number:
                filter_query["accountNumber"] = account_number

            if filter_query:
                existing = collection.find_one(filter_query)
                if existing:
                    for field in _PROTECTED_CLIENT_FIELDS:
                        if field in existing:
                            enriched_doc[field] = existing[field]
                collection.replace_one(filter_query, enriched_doc, upsert=True)
            else:
                collection.insert_one(enriched_doc)
            upserted += 1

        print(f"Upserted {upserted} document(s) into '{db_name}.clients'.")
    except PyMongoError as e:
        raise RuntimeError(f"Failed to upsert documents into MongoDB: {e}") from e
    finally:
        client.close()


if __name__ == "__main__":
    import json as _json

    config = fetch_data()
    scope_items = _extract_scope_items(config)

    seen_adviser_codes = set()
    all_documents: List[Dict[str, Any]] = []

    for item in scope_items:
        adviser_code = item.get("adviserCode")
        if adviser_code in seen_adviser_codes:
            continue
        if adviser_code:
            seen_adviser_codes.add(adviser_code)

        for include_inactive in (True, False):
            call_item = dict(item)
            call_item["includeInactive"] = include_inactive

            data = fetch_trading_accounts(call_item)
            print(f"\n--- Result for adviserCode={adviser_code or 'N/A'} includeInactive={include_inactive} ---")
            print(_json.dumps(data, indent=2, ensure_ascii=False))

            documents = _normalize_to_documents(data)
            if documents:
                all_documents.extend(documents)

    if all_documents:
        upsert_clients(all_documents)
    else:
        print("No trading account documents collected; skipping database upsert.")
