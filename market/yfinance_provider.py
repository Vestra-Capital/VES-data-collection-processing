"""Yahoo Finance implementation of the market data provider abstraction."""

from typing import Any, Dict

from market.provider import MarketDataProvider


class YFinanceProvider(MarketDataProvider):
    """Fetch instrument metadata from Yahoo Finance via ``yfinance``."""

    def fetch_instrument_metadata(self, symbol: str) -> Dict[str, Any]:
        import yfinance as yf

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

        return result
