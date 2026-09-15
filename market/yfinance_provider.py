"""Yahoo Finance implementation of the market data provider abstraction."""

from typing import Any, Dict, List

import yfinance as yf

from market.provider import MarketDataProvider


class YFinanceProvider(MarketDataProvider):
    """Fetch instrument metadata and current prices from Yahoo Finance."""

    def fetch_instrument_metadata(self, symbol: str) -> Dict[str, Any]:
        ticker = yf.Ticker(symbol)
        info = ticker.info
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

        try:
            data = yf.download(symbols, period="1d", progress=False)
        except Exception:
            return {}

        if data.empty:
            return {}

        result: Dict[str, float] = {}

        if len(symbols) == 1:
            symbol = symbols[0]
            close_col = data.get("Close")
            if close_col is not None:
                try:
                    price = float(close_col.iloc[-1])
                    result[symbol] = price
                except (TypeError, ValueError, IndexError):
                    pass
            return result

        close_col = data.get("Close")
        if close_col is None or not hasattr(close_col, "columns"):
            return result

        for symbol in symbols:
            if symbol not in close_col.columns:
                continue
            try:
                price = float(close_col[symbol].iloc[-1])
                if price > 0:
                    result[symbol] = price
            except (TypeError, ValueError, IndexError):
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

        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=period, auto_adjust=True)
        except Exception:
            return []

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
