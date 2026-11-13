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
    SKIP_WEEKENDS             - Set to "false" to run daemons on Saturday/Sunday too (default: "true")
"""

import datetime
import logging
import os
import sys
import threading
import time
from zoneinfo import ZoneInfo

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

SYDNEY_TZ = ZoneInfo("Australia/Sydney")
WEEKEND_DAYS = {5, 6}  # Saturday=5, Sunday=6
MONDAY_START_HOUR = 8  # Start daemons at 08:00 Sydney time on Monday


def is_weekend_or_before_monday_start() -> bool:
    """Return True if it's the weekend, or Monday before 08:00 Sydney time."""
    now_sydney = datetime.datetime.now(SYDNEY_TZ)
    if now_sydney.weekday() in WEEKEND_DAYS:
        return True
    if now_sydney.weekday() == 0 and now_sydney.hour < MONDAY_START_HOUR:
        return True
    return False


def wait_for_monday_start() -> None:
    """Sleep until Monday 08:00 Sydney time if it's the weekend or early Monday."""
    while is_weekend_or_before_monday_start():
        now_sydney = datetime.datetime.now(SYDNEY_TZ)
        if now_sydney.weekday() in WEEKEND_DAYS:
            days_ahead = 7 - now_sydney.weekday()  # Sat(5)->2, Sun(6)->1
        else:
            days_ahead = 0  # Monday before 08:00
        next_start = (now_sydney + datetime.timedelta(days=days_ahead)).replace(
            hour=MONDAY_START_HOUR, minute=0, second=0, microsecond=0
        )
        sleep_seconds = max((next_start - now_sydney).total_seconds(), 60)
        sleep_seconds = min(sleep_seconds, 3600)
        logger.info(
            "Outside operating window (weekend or before Monday 08:00 Sydney). "
            "Daemons paused until %s (sleeping %ds).",
            next_start.strftime("%A %Y-%m-%d %H:%M %Z"),
            int(sleep_seconds),
        )
        time.sleep(sleep_seconds)
    logger.info("Operating window reached. Starting daemons.")


def _run(name: str, target) -> None:
    """Run a daemon's main loop, logging but not propagating fatal errors."""
    try:
        target()
    except SystemExit:
        logger.info("%s daemon exited.", name)
    except Exception:
        logger.exception("%s daemon exited unexpectedly; continuing others.", name)


def main() -> None:
    skip_weekends = os.getenv("SKIP_WEEKENDS", "true").lower() == "true"

    if skip_weekends:
        wait_for_monday_start()

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
