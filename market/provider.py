"""Abstraction layer for market data providers.

This module defines the ``MarketDataProvider`` interface, which decouples the
rest of the pipeline from any specific market data vendor.  Concrete
implementations can be added without touching the consumers.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any


class MarketDataProvider(ABC):
    """Interface for fetching instrument metadata from a market data provider."""

    @abstractmethod
    def fetch_instrument_metadata(self, symbol: str) -> Dict[str, Any]:
        """Fetch metadata for a single instrument.

        Args:
            symbol: The instrument ticker symbol (e.g. ``"CBA.AX"``).

        Returns:
            A dictionary that may contain the following keys:
                - ``sector``
                - ``industry``
                - ``dividend_yield``
            Keys for fields that are not available from the provider are
            omitted.
        """
