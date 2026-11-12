"""Diagnostic script to inspect why nav_timeseries is incomplete/missing for a given account.

Reads from MongoDB (no writes), prints the portfolio's holdings, stored nav_timeseries,
and the instruments/timeseries backing each holding symbol. Also recomputes
nav_timeseries via _calculate_nav_timeseries to show what would be produced.
"""

import os
import sys
from typing import Any, Dict, List

from dotenv import load_dotenv
from pymongo import MongoClient

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

from batch.get_portfolios_nav import (
    _build_instrument_price_map,
    _build_instrument_map,
    _calculate_nav_timeseries,
    _smooth_price_map,
    fetch_instruments,
)


def main(account_number: str) -> None:
    mongo_srv = os.getenv("MONGODB_SRV")
    db_name = os.getenv("DATABASE_NAME", "VESTRA_PROD")
    if not mongo_srv:
        raise RuntimeError("MONGODB_SRV is missing. Check your .env file.")

    client = MongoClient(mongo_srv, serverSelectionTimeoutMS=15000)
    try:
        db = client[db_name]

        port = db["portfolios"].find_one({"accountNumber": account_number})
        if not port:
            print(f"No portfolio found for accountNumber={account_number}")
            # try int
            port = db["portfolios"].find_one({"accountNumber": int(account_number)})
            if not port:
                print(f"No portfolio found for accountNumber={int(account_number)} either")
                return
            account_number = str(port.get("accountNumber", account_number))

        print(f"=== Portfolio {account_number} ===")
        print("DB:", db_name)
        print("Stored nav_timeseries entries:", len(port.get("nav_timeseries") or []))
        nts = port.get("nav_timeseries") or []
        if nts:
            print("  first date:", nts[0]["date"])
            print("  last date:", nts[-1]["date"])
        else:
            print("  (empty or missing)")

        holdings = port.get("holdings") or []
        print("\nHoldings count:", len(holdings))
        symbols: List[str] = []
        for h in holdings:
            sc = h.get("securityCode")
            mcyf = h.get("marketCode_yf")
            th = h.get("totalHolding")
            sym = f"{sc}.{mcyf}" if sc and mcyf else None
            if sym:
                symbols.append(sym)
            print(
                f"  - securityCode={sc} marketCode_yf={mcyf} symbol={sym} "
                f"totalHolding={th} desc={h.get('securityDescription')}"
            )

        if not symbols:
            print("\nNo symbols extracted from holdings; nav_timeseries will be empty.")
            return

        unique_symbols = sorted(set(symbols))
        print(f"\nUnique symbols ({len(unique_symbols)}): {unique_symbols}")

        instruments = fetch_instruments(db_name, unique_symbols)
        print(f"Instruments found in MongoDB: {len(instruments)}")
        present_symbols = {doc.get("symbol") for doc in instruments if doc.get("symbol")}
        missing_symbols = [s for s in unique_symbols if s not in present_symbols]
        if missing_symbols:
            print(f"  MISSING from instruments collection: {missing_symbols}")

        price_map = _build_instrument_price_map(instruments)
        print("\nPrice map summary:")
        for sym in unique_symbols:
            dp = price_map.get(sym)
            if not dp:
                print(f"  {sym}: NO PRICE DATA")
                continue
            dates = sorted(dp)
            print(f"  {sym}: {len(dates)} price points, {dates[0]} -> {dates[-1]}")

        smoothed = _smooth_price_map(price_map)
        recomputed = _calculate_nav_timeseries(holdings, smoothed)
        print(f"\nRecomputed nav_timeseries entries: {len(recomputed)}")
        if recomputed:
            print("  first date:", recomputed[0]["date"])
            print("  last date:", recomputed[-1]["date"])
            print("  sample:", recomputed[:3], "...", recomputed[-3:])
        else:
            print("  (empty)")

        print("\n=== Diagnosis ===")
        if not nts:
            print("- Stored nav_timeseries is EMPTY/MISSING.")
        elif len(recomputed) > len(nts):
            print(f"- Stored has {len(nts)} entries; recompute yields {len(recomputed)}. Run update to backfill.")
        elif len(recomputed) < len(nts):
            print(f"- Stored has {len(nts)} entries; recompute yields {len(recomputed)}. Data loss on rewrite.")
        else:
            print("- Stored count matches recompute count.")

        if missing_symbols:
            print("- Some holding symbols are missing from instruments collection -> their holdings are skipped.")
        empty_price = [s for s in unique_symbols if not price_map.get(s)]
        if empty_price:
            print(f"- Symbols with no price data (holding skipped): {empty_price}")

    finally:
        client.close()


if __name__ == "__main__":
    acct = sys.argv[1] if len(sys.argv) > 1 else "115490"
    main(acct)
