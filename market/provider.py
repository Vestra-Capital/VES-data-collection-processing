"""Abstraction layer for market data providers.

This module defines the ``MarketDataProvider`` interface, which decouples the
rest of the pipeline from any specific market data vendor.  Concrete
implementations can be added without touching the consumers.
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List


class MarketDataProvider(ABC):
    """Interface for fetching instrument metadata and current prices."""

    @abstractmethod
    def fetch_instrument_metadata(self, symbol: str) -> Dict[str, Any]:
        """Fetch static metadata for a single instrument."""

    @abstractmethod
    def fetch_current_prices(self, symbols: list) -> Dict[str, float]:
        """Fetch the current price for multiple symbols.

        Args:
            symbols: List of instrument ticker symbols.

        Returns:
            Mapping of ``symbol`` to current price. Symbols that could not
            be fetched are omitted.
        """

    @abstractmethod
    def fetch_instrument_timeseries(self, symbol: str, period: str = "1y") -> List[Dict[str, Any]]:
        """Fetch historical timeseries data for a single instrument.

        Args:
            symbol: Instrument ticker symbol.
            period: Look-back period (e.g. ``1y``, ``6mo``, ``2y``).

        Returns:
            List of dicts with ``date`` (``YYYY-MM-DD``) and ``price``
            (close price), sorted ascending by date.  Returns an empty
            list if the data is unavailable.
        """
