"""Quick test script to fetch the current price for a single symbol.

Usage:
    python test_single_symbol.py <SYMBOL>

Examples:
    python test_single_symbol.py BHP.AX
    python test_single_symbol.py CBA.AX

Environment variables (optional, loaded from .env if present):
    MONGODB_SRV, DATABASE_NAME, MARKET_DATA_PROVIDER, PRICE_POLL_INTERVAL_SECONDS
"""

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) if "__file__" in dir() else os.getcwd()
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

from market.yfinance_provider import YFinanceProvider


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python test_single_symbol.py <SYMBOL>")
        print("Example: python test_single_symbol.py BHP.AX")
        sys.exit(1)

    symbol = sys.argv[1].upper()
    provider = YFinanceProvider()

    print(f"Fetching current price for {symbol}...")

    try:
        prices = provider.fetch_current_prices([symbol])
    except Exception as exc:
        print(f"ERROR: Failed to fetch price for {symbol}: {exc}")
        sys.exit(1)

    if symbol in prices:
        print(f"{symbol}: {prices[symbol]}")
    else:
        print(f"{symbol}: No price data returned (possibly rate-limited or delisted)")
        sys.exit(1)


if __name__ == "__main__":
    main()
