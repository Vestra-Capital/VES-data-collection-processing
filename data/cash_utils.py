"""Shared cash calculation utilities.

Centralises the derivation of ``totalCash`` from a portfolio's cash
sub-document so the same logic is used across the fees reporting pipeline,
the portfolio metrics batch, and the historical fees recalc script.
"""

import math
from typing import Any


def _to_float(value: Any) -> float:
    """Safely convert a value to float.

    Args:
        value: Value to convert.

    Returns:
        Numeric float value, or ``0.0`` if the value is missing, not numeric,
        or NaN.
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        result = float(value)
        if math.isnan(result):
            return 0.0
        return result
    if isinstance(value, str):
        try:
            result = float(value)
            if math.isnan(result):
                return 0.0
            return result
        except (TypeError, ValueError):
            return 0.0
    return 0.0


def calculate_total_cash(cash_data: Any) -> float:
    """Sum the bank balance across all cash bank accounts for an account.

    Args:
        cash_data: Cash sub-document from the portfolio document.

    Returns:
        Total cash value as a float.
    """
    total = 0.0
    if not isinstance(cash_data, dict):
        return total
    bank_accounts = cash_data.get("bankAccounts") or []
    if not isinstance(bank_accounts, list):
        return total
    for bank_account in bank_accounts:
        if not isinstance(bank_account, dict):
            continue
        total += _to_float(bank_account.get("bankBalance"))
    return total
