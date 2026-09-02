"""Configuration and HTTP client for the Morrison Securities Data Access API.

This module centralises all configuration, environment loading, and HTTP
logic for the Vestra Capital data collection pipeline.  Keeping these
concerns in one place avoids duplication and makes it straightforward to
swap hosts, credentials, or headers in a single location.

Environment variables are loaded from the project root ``.env`` file at
import time so that ``os.getenv`` calls resolve to the developer's local
values rather than hard-coded secrets.

Typical usage::

    from data.configuration import fetch_data
    payload = fetch_data()
"""

import json
import os
from typing import Any, Dict, NoReturn
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from dotenv import load_dotenv

# Load environment variables from ``.env`` in the project root so that
# ``os.getenv`` calls below resolve to user-supplied values rather than
# hard-coded secrets or placeholders.
load_dotenv()

# Base URL for the Morrison Securities Data Access API host.
#
# Override with the ``MORRISON_API_BASE_URL`` environment variable when
# targeting a non-default host (e.g. a staging or regional endpoint).
# This should be the domain root only; the API path is defined separately
# in ``ENDPOINT_PATH``.
BASE_URL: str = os.getenv(
    "MORRISON_API_BASE_URL",
    "https://api.morrisonsecurities.com/backoffice",
).rstrip("/")

# API endpoint path appended to ``BASE_URL``.
ENDPOINT_PATH: str = "/dataaccess/v1"

# Full request URL composed from host and endpoint path.
API_URL: str = BASE_URL + ENDPOINT_PATH

# Default request headers sent with every API call.
#
# ``x-api-key`` carries the bearer-style credential issued by Morrison
# Securities.  A ``User-Agent`` is included because some API gateways
# reject requests from default Python urllib clients.
HEADERS: Dict[str, str] = {
    "x-api-key": os.getenv("MORRISON_ACCESS_KEY", ""),
    "Accept": "application/json",
    "User-Agent": "ves-morrison-securities-data-access/1.0",
}


def _raise_for_status(error: HTTPError) -> NoReturn:
    """Read an ``HTTPError`` response body and re-raise it as ``RuntimeError``.

    Using ``NoReturn`` tells type-checkers that this function always raises,
    which in turn allows callers to satisfy strict return-type contracts
    without an explicit ``return`` on the error path.
    """
    try:
        # The error body may contain structured details (e.g. JSON error
        # payload) that are useful when debugging auth or validation issues.
        body = error.read().decode("utf-8", errors="replace")
    except Exception:
        body = ""
    raise RuntimeError(
        f"API request failed: {error.code} {error.reason} - {body}"
    ) from error


def fetch_data() -> Any:
    """Fetch data from the Morrison Securities Data Access API.

    This is the primary entry point for retrieving configuration or scope
    data from Morrison Securities.  The function performs a single GET
    request against the configured ``API_URL`` and returns the parsed JSON
    payload.

    Returns:
        Parsed JSON response as a dictionary.

    Raises:
        RuntimeError: If the API key is missing, the response is empty,
            the response body is not valid JSON, or the server returns
            an HTTP error.
    """
    if not HEADERS["x-api-key"]:
        raise RuntimeError("MORRISON_ACCESS_KEY is missing. Check your .env file.")

    url = API_URL
    request = Request(url, headers=HEADERS, method="GET")

    try:
        with urlopen(request) as response:
            raw = response.read().decode("utf-8", errors="replace")

            # An empty body with a 200 status is unusual; treat it as an
            # error so callers don't receive an empty dict silently.
            if not raw.strip():
                raise RuntimeError("API returned an empty response.")

            try:
                return json.loads(raw)
            except json.JSONDecodeError as e:
                # The server returned a successful HTTP status but the
                # body is not JSON (often an HTML error or login page).
                # Surface the content type, status, and URL to aid debugging.
                content_type = response.headers.get("Content-Type", "unknown")
                status = getattr(response, "status", "unknown")
                raise RuntimeError(
                    f"API returned non-JSON content (status={status}, "
                    f"Content-Type={content_type}). URL: {url} "
                    f"Response preview: {raw[:200]}"
                ) from e
    except HTTPError as e:
        _raise_for_status(e)

    # This line is unreachable in practice because every path above either
    # returns a value or raises, but it satisfies type-checkers that
    # require an explicit return on all code paths.
    raise RuntimeError("Unexpected state: request completed without returning data.")


if __name__ == "__main__":
    import json as _json

    data = fetch_data()
    print(_json.dumps(data, indent=2, ensure_ascii=False))
