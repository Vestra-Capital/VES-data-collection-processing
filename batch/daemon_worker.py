"""Combined Background Worker for the VES data-collection daemons.

Runs all three long-lived polling loops concurrently in a single Render
Background Worker service to save on Render costs (one service instead of
three):

  - batch/monitor_instruments.py      -> detect and enrich new instruments
  - batch/update_portfolios_and_risk_metrics.py -> maintain NAV timeseries and risk metrics
  - batch/price_streamer.py           -> poll current instrument prices

Each daemon runs in its own daemon thread with its own error handling, so a
failure in one loop does not kill the others. Render sees a single Background
Worker process.

Usage (Render Background Worker):
    Build command:  pip install -r requirements.txt
    Start command:  python batch/daemon_worker.py

Environment variables (set in the Render dashboard Environment tab):
    MONGODB_SRV               - MongoDB connection string (required)
    DATABASE_NAME             - Target database (default: VESTRA_PROD)
    MORRISON_ACCESS_KEY       - Morrison API access key (required)
    MORRISON_API_BASE_URL     - Morrison API base URL (required)
    MARKET_DATA_PROVIDER      - Market data provider module path
    PRICE_POLL_INTERVAL_SECONDS - Seconds between price polls (default: 15)
    UPDATE_PORTFOLIOS_POLL_INTERVAL_SECONDS - Seconds between portfolio/risk cycles (default: 300)
"""

import logging
import os
import sys
import threading

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

from batch.monitor_instruments import main as monitor_instruments_main
from batch.price_streamer import main as price_streamer_main
from batch.update_portfolios_and_risk_metrics import main as update_portfolios_main

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [daemon_worker] %(message)s",
)
logger = logging.getLogger(__name__)

DAEMONS = [
    ("monitor-instruments", monitor_instruments_main),
    ("update-portfolios-risk", update_portfolios_main),
    ("update-prices", price_streamer_main),
]


def _run(name: str, target) -> None:
    """Run a daemon's main loop, logging but not propagating fatal errors."""
    try:
        target()
    except SystemExit:
        logger.info("%s daemon exited.", name)
    except Exception:
        logger.exception("%s daemon exited unexpectedly; continuing others.", name)


def main() -> None:
    logger.info("Starting combined daemon worker with %d daemon(s).", len(DAEMONS))

    threads = [
        threading.Thread(target=_run, args=(name, fn), daemon=True, name=name)
        for name, fn in DAEMONS
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    logger.info("Combined daemon worker shutting down.")


if __name__ == "__main__":
    main()
