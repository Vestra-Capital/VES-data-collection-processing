"""Yahoo Finance implementation of the market data provider abstraction.

Every API call uses a fresh yfinance session (no global reuse) and retries
with exponential backoff on empty results / rate limits to avoid Yahoo
Finance rate limits on Render.
"""

from typing import Any, Dict, List

import yfinance as yf

from market.provider import MarketDataProvider
from market.yfinance_session import retry_on_429


class YFinanceProvider(MarketDataProvider):
    """Fetch instrument metadata and current prices from Yahoo Finance."""

    def fetch_instrument_metadata(self, symbol: str) -> Dict[str, Any]:
        ticker = yf.Ticker(symbol)
        info = retry_on_429(lambda: ticker.info)

        result: Dict[str, Any] = {}

        sector = info.get("sector")
        if isinstance(sector, str) and sector.strip():
            result["sector"] = sector.strip()
        else:
            result["sector"] = "Others"

        industry = info.get("industry")
        if isinstance(industry, str) and industry.strip():
            result["industry"] = industry.strip()
        else:
            result["industry"] = "Others"

        dividend_yield = info.get("dividendYield")
        if dividend_yield is None:
            dividend_yield = 0.0
        result["dividend_yield"] = dividend_yield

        current_price = info.get("currentPrice")
        if current_price is not None:
            result["currentPrice"] = current_price

        return result

    def fetch_current_prices(self, symbols: list) -> Dict[str, float]:
        if not symbols:
            return {}

        def _download_with_retry():
            data = yf.download(symbols, period="1d", progress=False)
            if data.empty:
                raise RuntimeError("Empty yfinance download result — possible rate limit")
            return data

        try:
            data = retry_on_429(_download_with_retry)
        except Exception as exc:
            logger.warning(
                "yfinance download failed for %d symbol(s) after retries: %s",
                len(symbols),
                exc,
            )
            return {}

        if data.empty:
            return {}

        result: Dict[str, float] = {}

        close_col = data.get("Close")
        if close_col is None:
            return result

        for symbol in symbols:
            try:
                price_series = close_col[symbol]
                price = float(price_series.iloc[-1])
                if price > 0:
                    result[symbol] = price
            except (TypeError, ValueError, IndexError, KeyError):
                continue

        return result

    def fetch_instrument_timeseries(self, symbol: str, period: str = "1y") -> List[Dict[str, Any]]:
        """Fetch historical price timeseries for a single instrument from Yahoo Finance.

        Args:
            symbol: Yahoo Finance ticker symbol.
            period: Look-back window recognised by yfinance (default ``1y``).

        Returns:
            List of ``{"date": str, "price": float}`` dicts sorted by date
            ascending.  Returns an empty list if no history is available.
        """
        if not symbol:
            return []

        ticker = yf.Ticker(symbol)
        hist = retry_on_429(
            lambda: ticker.history(period=period, auto_adjust=True)
        )

        if hist is None or hist.empty:
            return []

        result: List[Dict[str, Any]] = []
        for idx, row in hist.iterrows():
            try:
                date_str = idx.strftime("%Y-%m-%d")
                price = float(row["Close"])
                result.append({"date": date_str, "price": price})
            except (TypeError, ValueError, AttributeError):
                continue

        result.sort(key=lambda item: item["date"])
        return result
