import subprocess
import sys
from pathlib import Path

# Ordered list of data collection scripts to execute sequentially.
# Each script fetches and persists a specific domain of data.
SCRIPTS = [
    "data/001_get_trading_account.py",
    "data/002_get_portfolio_holdings.py",
    "data/003_get_cash_balances.py",
    "data/004_get_trades_history.py",
]

# Resolve the project root so child scripts run with the expected working directory.
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    """Run the data collection pipeline sequentially.

    Executes each script in ``SCRIPTS`` using the current Python interpreter.
    Execution stops at the first failure and returns the offending exit code.

    Returns:
        int: ``0`` if all scripts succeed, otherwise the failing script's
        exit code.
    """
    for script in SCRIPTS:
        print(f"[collect_data] Running {script}...")
        result = subprocess.run(
            [sys.executable, script],
            cwd=PROJECT_ROOT,
        )
        if result.returncode != 0:
            print(
                f"[collect_data] {script} failed with exit code {result.returncode}.",
                file=sys.stderr,
            )
            return result.returncode
        print(f"[collect_data] {script} completed successfully.")
    print("[collect_data] All data collection scripts completed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
