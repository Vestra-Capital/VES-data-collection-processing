"""Yahoo Finance HTTP session utilities with rate-limit protection.

Every yfinance call gets a brand-new ``requests.Session`` (no global reuse),
a randomized ``User-Agent`` header, and exponential-backoff retries on HTTP
429 (Too Many Requests) to avoid Yahoo Finance rate limits on Render.
"""

import logging
import random
import time
from typing import Any, Callable, Optional

import requests
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

_USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) "
    "Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Edge/120.0.0.0",
)


def create_session() -> requests.Session:
    """Return a fresh ``requests.Session`` with a randomized User-Agent.

    A new session is created on every call so no session, connection pool,
    or cookies are reused across yfinance requests.
    """
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": random.choice(_USER_AGENTS),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "close",
        }
    )
    # Disable connection pooling so each session uses a fresh TCP connection.
    adapter = HTTPAdapter(max_retries=0)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def retry_on_429(
    func: Callable[..., Any],
    *args: Any,
    max_retries: int = 5,
    base_delay: float = 2.0,
    max_delay: float = 60.0,
    **kwargs: Any,
) -> Any:
    """Call *func*, retrying with exponential backoff on HTTP 429.

    Args:
        func: Callable to invoke.
        max_retries: Maximum number of retry attempts.
        base_delay: Initial delay in seconds.
        max_delay: Maximum delay in seconds.
    Returns:
        The return value of *func*.
    Raises:
        The last exception if all retries are exhausted.
    """
    delay = base_delay
    last_exc: Optional[Exception] = None

    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except requests.HTTPError as exc:
            response = exc.response
            if response is not None and response.status_code == 429:
                last_exc = exc
                if attempt < max_retries:
                    sleep_for = min(delay * (2**attempt), max_delay)
                    sleep_for += random.uniform(0, 1)  # jitter
                    logger.warning(
                        "Yahoo Finance rate limited (429). "
                        "Retrying in %.1fs (attempt %d/%d)...",
                        sleep_for,
                        attempt + 1,
                        max_retries,
                    )
                    time.sleep(sleep_for)
                    continue
            raise
        except Exception as exc:  # noqa: BLE001 - retry on transient errors too
            last_exc = exc
            if attempt < max_retries:
                sleep_for = min(delay * (2**attempt), max_delay)
                sleep_for += random.uniform(0, 1)
                logger.warning(
                    "Yahoo Finance request failed: %s. "
                    "Retrying in %.1fs (attempt %d/%d)...",
                    exc,
                    sleep_for,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(sleep_for)
                continue
            raise

    raise last_exc  # type: ignore[unreachable]
