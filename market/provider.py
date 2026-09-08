"""Abstraction layer for market data providers.

This module defines the ``MarketDataProvider`` interface, which decouples the
rest of the pipeline from any specific market data vendor.  Concrete
implementations can be added without touching the consumers.
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict


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
