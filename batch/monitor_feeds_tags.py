"""Batch script to monitor feeds.tags collection for instrument symbols,
parse them into securityCode/marketCode_yf, and upsert into instruments.

Reads all documents from the ``feeds.tags`` collection, extracts comma-separated
instrument tags (e.g. ``"CBA.AX, BHP.AX, ^XJO"``), splits each tag into
``securityCode`` (everything before the dot) and ``marketCode_yf`` (everything
after the dot, empty string if there is no dot such as ``^XJO``), and upserts
the result into the ``instruments`` collection.

Environment variables:
    MONGODB_SRV — MongoDB connection string.
    DATABASE_NAME — Database name (defaults to ``VESTRA_PROD``).
    FEEDS_COLLECTION — Collection name to read tags from (defaults to ``feeds``).
        Each document should have a ``tags`` array field (e.g.
        ``["^XJO", "^AXJT", "USOIL"]``) or a comma-separated string.
"""

import logging
import os
import sys
from typing import Any, Dict, List

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

logger = logging.getLogger(__name__)


def get_mongo_client() -> MongoClient:
    mongo_srv = os.getenv("MONGODB_SRV")
    if not mongo_srv:
        raise RuntimeError("MONGODB_SRV is missing. Check your .env file.")
    return MongoClient(mongo_srv)


def fetch_tags_documents(db_name: str) -> List[Dict[str, Any]]:
    """Read all documents from the feeds tags collection.

    Fetches the latest 1000 documents ordered by date_time DESC.
    """
    client = get_mongo_client()
    try:
        db = client[db_name]
        collection_name = os.getenv("FEEDS_COLLECTION", "feeds")
        collection = db[collection_name]
        cursor = collection.find({}, {"_id": 0}).sort("date_time", -1).limit(1000)
        documents = list(cursor)
        # Debug: list all collections if empty
        if not documents:
            all_colls = db.list_collection_names()
            logger.info(
                "Collections in '%s': %s", db_name, all_colls[:20]
            )
            logger.info(
                "Collection '%s' has %d document(s).",
                collection_name,
                collection.count_documents({}),
            )
        return documents
    except PyMongoError as e:
        raise RuntimeError(f"Failed to fetch tags from MongoDB: {e}") from e
    finally:
        client.close()


def parse_symbol(symbol: str) -> Dict[str, str]:
    """Split a symbol like ``CBA.AX`` or ``^XJO`` into securityCode/marketCode_yf.

    Rules:
        - ``CBA.AX``   → securityCode="CBA",  marketCode_yf="AX"
        - ``^XJO``     → securityCode="^XJO", marketCode_yf=""
        - ``MSFT``     → securityCode="MSFT", marketCode_yf=""
    """
    symbol = symbol.strip()
    if not symbol:
        return {"securityCode": "", "marketCode_yf": ""}

    if "." in symbol:
        parts = symbol.split(".", 1)
        return {"securityCode": parts[0], "marketCode_yf": parts[1]}

    return {"securityCode": symbol, "marketCode_yf": ""}


def extract_symbols_from_tags(documents: List[Dict[str, Any]]) -> List[str]:
    """Extract instrument tags from feeds.tags documents.

    Looks only at the ``tags`` field of each document. Handles both
    comma-separated string values (e.g. "CBA.AX, BHP.AX")
    and array values (e.g. ["^XJO", "^AXJT", "USOIL"]).
    """
    symbols: List[str] = []
    for doc in documents:
        tags_value = doc.get("tags")
        if tags_value is None:
            continue
        if isinstance(tags_value, str):
            tags = tags_value.split(",")
        elif isinstance(tags_value, list):
            tags = [str(v) for v in tags_value]
        else:
            continue
        for tag in tags:
            tag = tag.strip()
            if tag and tag not in symbols:
                symbols.append(tag)
    return symbols


def upsert_instruments(db_name: str, instruments: List[Dict[str, Any]]) -> None:
    """Upsert instrument documents into MongoDB."""
    if not instruments:
        return

    client = get_mongo_client()
    try:
        db = client[db_name]
        collection = db["instruments"]

        upserted = 0
        for instrument in instruments:
            symbol = instrument.get("symbol")
            if not symbol:
                continue
            collection.replace_one({"symbol": symbol}, instrument, upsert=True)
            upserted += 1

        logger.info(
            "Upserted %d instrument(s) into '%s.instruments'.", upserted, db_name
        )
    except PyMongoError as e:
        raise RuntimeError(f"Failed to upsert instruments into MongoDB: {e}") from e
    finally:
        client.close()


def main() -> None:
    db_name = os.getenv("DATABASE_NAME", "VESTRA_PROD")
    collection_name = os.getenv("FEEDS_COLLECTION", "feeds")
    print(f"Fetching tags from '{db_name}.{collection_name}'...")
    documents = fetch_tags_documents(db_name)
    print(f"Fetched {len(documents)} tag document(s).")

    symbols = extract_symbols_from_tags(documents)
    print(f"Extracted {len(symbols)} unique symbol(s): {symbols}")

    if not symbols:
        print("No symbols found; exiting.")
        return

    instruments: List[Dict[str, Any]] = []
    for symbol in symbols:
        parsed = parse_symbol(symbol)
        instruments.append(
            {
                "symbol": symbol,
                "securityCode": parsed["securityCode"],
                "marketCode_yf": parsed["marketCode_yf"],
                "securityDescription": "",
                "currency": "",
            }
        )

    upsert_instruments(db_name, instruments)
    print(f"Upserted {len(instruments)} instrument(s) into '{db_name}.instruments'.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()