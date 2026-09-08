"""Yahoo Finance implementation of the market data provider abstraction."""

from typing import Any, Dict

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
