"""HTML report generator for client portfolios.

Queries the MongoDB ``clients`` and ``portfolios`` collections and writes a
self-contained HTML file that lists every client as a collapsible section
showing:
  - Client identity fields (name, account number, adviser).
  - A table of equity holdings (security, market, units, value).
  - Cash balance summary.

The output file can be opened directly in a browser; no server is required.

Typical usage::

    from reporting.generate_html_report_clients import generate_client_report

    generate_client_report(output_path="reports/clients_report.html")

Environment variables required:
    MONGODB_SRV: MongoDB connection string.
    DATABASE_NAME: Database name (defaults to ``VESTRA_PROD``).
"""

import html
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# Load environment variables from the project root ``.env`` file.
project_root = Path(__file__).resolve().parent.parent
load_dotenv(project_root / ".env")

logger = logging.getLogger(__name__)

DATABASE_NAME = os.getenv("DATABASE_NAME", "VESTRA_PROD")
CLIENTS_COLLECTION = "clients"
PORTFOLIOS_COLLECTION = "portfolios"

EMAIL_HEADER = """
  <span style="font-family:Arial">
    <hr>
    <h1><span style="font-size: 1.5em; font-family: 'Arial Black'; font-weight: 999;">VESTRA</span><br><span style="font-size: 0.75em; letter-spacing: 7.2; font-family: 'Arial Narrow', Arial, sans-serif; font-weight: lighter;">CAPITAL</span></h1>
    <hr>
    <br>
"""

EMAIL_FOOTER = """
    <br>
    <br>
    <hr>
    <span style="font-size: 0.75em;">
    This report is from <strong>Vestra Capital</strong> (<a href="https://www.vestracapital.com.au">vestracapital.com.au</a>)
    <br>
    <p>If you have any questions, please do not hesitate to contact us: <a href="mailto:team@vestracapital.com.au">team@vestracapital.com.au</a></p>
    </span>
    <hr>
    <span style="font-size: 0.6em;">
      General info only, not personal advice. Consider your aims and finances before acting. Review full terms and seek independent advice — <a href="https://www.vestracapital.com.au/">Privacy Policy</a>&nbsp;| &nbsp;<a href="https://www.vestracapital.com.au/">Terms of Use</a>
    </span>
  </span>
"""


def _get_mongo_client() -> MongoClient:
    """Create and return a MongoDB client from environment configuration.

    Returns:
        A ``MongoClient`` instance connected to the cluster specified by
        ``MONGODB_SRV`` in ``.env``.

    Raises:
        RuntimeError: If ``MONGODB_SRV`` is not configured.
    """
    mongo_srv = os.getenv("MONGODB_SRV")
    if not mongo_srv:
        raise RuntimeError("MONGODB_SRV is missing. Check your .env file.")
    return MongoClient(mongo_srv)


def fetch_clients() -> List[Dict[str, Any]]:
    """Fetch all client documents from the MongoDB ``clients`` collection.

    Returns:
        List of client document dicts, each containing at least ``accountNumber``.

    Raises:
        RuntimeError: If the MongoDB connection or query fails.
    """
    client = _get_mongo_client()
    try:
        db = client[DATABASE_NAME]
        collection = db[CLIENTS_COLLECTION]
        documents = list(collection.find({}, {"_id": 0}))
        return [doc for doc in documents if isinstance(doc, dict)]
    except PyMongoError as e:
        raise RuntimeError(f"Failed to fetch clients from MongoDB: {e}") from e
    finally:
        client.close()


def fetch_portfolios() -> Dict[str, Dict[str, Any]]:
    """Fetch all portfolio documents keyed by ``accountNumber``.

    Returns:
        Dictionary mapping each ``accountNumber`` to its portfolio document
        (containing ``holdings`` and optionally ``cash``).

    Raises:
        RuntimeError: If the MongoDB connection or query fails.
    """
    client = _get_mongo_client()
    try:
        db = client[DATABASE_NAME]
        collection = db[PORTFOLIOS_COLLECTION]
        documents = list(collection.find({}, {"_id": 0}))
        portfolios: Dict[str, Dict[str, Any]] = {}
        for doc in documents:
            if isinstance(doc, dict):
                account_number = doc.get("accountNumber")
                if account_number:
                    portfolios[str(account_number)] = doc
        return portfolios
    except PyMongoError as e:
        raise RuntimeError(f"Failed to fetch portfolios from MongoDB: {e}") from e
    finally:
        client.close()


def _safe_str(value: Any) -> str:
    """Convert a value to a safe HTML string.

    Args:
        value: Value to stringify.

    Returns:
        HTML-escaped string representation, or empty string for ``None``.
    """
    if value is None:
        return ""
    return html.escape(str(value))


def _format_currency(value: Any, currency: str = "AUD") -> str:
    """Format a numeric value as a currency string.

    Args:
        value: Numeric value to format.
        currency: Currency code to append.

    Returns:
        Formatted currency string like ``"1,234.56 AUD"`` or the raw string
        if the value is not numeric.
    """
    if value is None:
        return f"0.00 {currency}"
    try:
        return f"{float(value):,.2f} {currency}"
    except (TypeError, ValueError):
        return _safe_str(value)


def build_client_section(client: Dict[str, Any], portfolio: Dict[str, Any] | None, index: int) -> str:
    """Build the collapsible HTML section for a single client.

    Args:
        client: Client document dict.
        portfolio: Portfolio document dict (may be ``None``).
        index: Zero-based index used to generate a unique element ID.

    Returns:
        HTML string for the client's collapsible section.
    """
    account_number = _safe_str(client.get("accountNumber", ""))
    account_name = _safe_str(client.get("accountName", ""))
    first_name = _safe_str(client.get("first_name", ""))
    last_name = _safe_str(client.get("last_name", ""))
    adviser_code = _safe_str(client.get("adviserCode", ""))
    org_code = _safe_str(client.get("organisationCode", ""))
    branch_code = _safe_str(client.get("branchCode", ""))
    client_category = _safe_str(client.get("client_category", ""))
    email = _safe_str(client.get("email", ""))
    telephone = _safe_str(client.get("telephone", ""))

    display_name = f"{first_name} {last_name}".strip() or account_name or account_number
    section_id = f"client-{index}"

    holdings_rows = ""
    total_holdings_value = 0.0
    holdings_count = 0

    if portfolio:
        holdings = portfolio.get("holdings", []) or []
        if isinstance(holdings, list) and holdings:
            for holding in holdings:
                if not isinstance(holding, dict):
                    continue
                description = _safe_str(holding.get("securityDescription", ""))
                market_code_yf = _safe_str(holding.get("marketCode_yf", ""))
                security_code = _safe_str(holding.get("securityCode", ""))
                units_raw = holding.get("totalHolding")
                value_raw = holding.get("marketValue")
                currency = _safe_str(holding.get("currency", "AUD"))

                units_str = _safe_str(units_raw)
                value_str = ""
                if value_raw is not None:
                    try:
                        total_holdings_value += float(value_raw)
                        value_str = _format_currency(value_raw, currency)
                        holdings_count += 1
                    except (TypeError, ValueError):
                        value_str = _safe_str(value_raw)

                if security_code and market_code_yf:
                    ticker_cell = f"{security_code}.{market_code_yf}"
                else:
                    ticker_cell = security_code or market_code_yf

                holdings_rows += (
                    f"<tr>"
                    f"<td style=\"padding:6px 10px;border:1px solid #ccc;\">{description}</td>"
                    f"<td style=\"padding:6px 10px;border:1px solid #ccc;\">{ticker_cell}</td>"
                    f"<td style=\"padding:6px 10px;border:1px solid #ccc;text-align:right;\">{units_str}</td>"
                    f"<td style=\"padding:6px 10px;border:1px solid #ccc;text-align:right;\">{value_str}</td>"
                    f"</tr>\n"
                )

    if not holdings_rows:
        holdings_rows = (
            f"<tr>"
            f"<td colspan=\"4\" style=\"padding:6px 10px;border:1px solid #ccc;text-align:center;color:#666;\">"
            f"No holdings found.</td></tr>\n"
        )

    holdings_value_display = _format_currency(total_holdings_value) if total_holdings_value else "0.00 AUD"

    cash_section = ""
    if portfolio:
        cash_data = portfolio.get("cash")
        if isinstance(cash_data, dict) and cash_data:
            bank_accounts = cash_data.get("bankAccounts") or []
            if isinstance(bank_accounts, list) and bank_accounts:
                bank_rows = ""
                total_cash = 0.0
                for bank_account in bank_accounts:
                    if not isinstance(bank_account, dict):
                        continue
                    bank_name = _safe_str(bank_account.get("bankAccountName", ""))
                    bank_number = _safe_str(bank_account.get("bankAccountNumber", ""))
                    bank_balance_raw = bank_account.get("bankBalance")
                    bank_balance_display = _format_currency(bank_balance_raw)
                    try:
                        total_cash += float(bank_balance_raw)
                    except (TypeError, ValueError):
                        pass
                    bank_rows += (
                        f"<tr>"
                        f"<td style=\"padding:6px 10px;border:1px solid #ccc;\">{bank_name}</td>"
                        f"<td style=\"padding:6px 10px;border:1px solid #ccc;\">{bank_number}</td>"
                        f"<td style=\"padding:6px 10px;border:1px solid #ccc;text-align:right;\">{bank_balance_display}</td>"
                        f"</tr>\n"
                    )
                total_cash_display = _format_currency(total_cash)
                cash_section = (
                    f"<h4 style=\"margin-top:10px;\">Cash Balance ({len([a for a in bank_accounts if isinstance(a, dict)])} bank account(s)) &mdash; Total: {total_cash_display}</h4>"
                    f"<table style=\"border-collapse:collapse;font-family:Arial,sans-serif;font-size:0.9em;width:100%;\">"
                    f"<thead><tr style=\"background:#f2f2f2;\">"
                    f"<th style=\"padding:6px 10px;border:1px solid #ccc;text-align:left;\">Bank Account Name</th>"
                    f"<th style=\"padding:6px 10px;border:1px solid #ccc;text-align:left;\">Bank Account Number</th>"
                    f"<th style=\"padding:6px 10px;border:1px solid #ccc;text-align:right;\">Balance</th>"
                    f"</tr></thead><tbody>{bank_rows}</tbody></table>"
                )
            else:
                cash_section = "<h4 style=\"margin-top:10px;\">Cash Balance</h4><p>No bank accounts found.</p>"
        elif portfolio.get("cash") is not None:
            cash_section = "<h4 style=\"margin-top:10px;\">Cash Balance</h4><p>No cash data available.</p>"

    section = f"""
    <div style=\"margin-bottom:12px;border:1px solid #ddd;border-radius:4px;overflow:hidden;\">
      <button
        type=\"button\"
        onclick=\"toggleSection('{section_id}')\"
        style=\"width:100%;text-align:left;padding:10px 14px;background:#f7f7f7;border:none;cursor:pointer;font-size:1em;font-family:Arial,sans-serif;display:flex;justify-content:space-between;align-items:center;\"
      >
        <span><strong>{index + 1}. {display_name}</strong> &nbsp;<span style=\"color:#555;font-size:0.85em;\">({account_number})</span></span>
        <span id=\"{section_id}-arrow\" style=\"font-size:0.8em;\">&#9660;</span>
      </button>
      <div id=\"{section_id}\" style=\"display:none;padding:12px 14px;background:#fff;\">
        <p style=\"margin-top:0;\">
          <strong>Account:</strong> {account_name} &nbsp;|&nbsp;
          <strong>Account Number:</strong> {account_number} &nbsp;|&nbsp;
          <strong>Adviser:</strong> {adviser_code} &nbsp;|&nbsp;
          <strong>Organisation:</strong> {org_code} &nbsp;|&nbsp;
          <strong>Branch:</strong> {branch_code}<br>
          <strong>Client Category:</strong> {client_category} &nbsp;|&nbsp;
          <strong>Email:</strong> {email} &nbsp;|&nbsp;
          <strong>Telephone:</strong> {telephone}
        </p>
        <h4 style=\"margin-top:10px;\">Holdings ({holdings_count} item(s)) &mdash; Total Value: {holdings_value_display}</h4>
        <table style=\"border-collapse:collapse;font-family:Arial,sans-serif;font-size:0.9em;width:100%;\">
          <thead>
            <tr style=\"background:#f2f2f2;\">
              <th style=\"padding:6px 10px;border:1px solid #ccc;text-align:left;\">Security</th>
              <th style=\"padding:6px 10px;border:1px solid #ccc;text-align:left;\">Ticker</th>
              <th style=\"padding:6px 10px;border:1px solid #ccc;text-align:right;\">Units</th>
              <th style=\"padding:6px 10px;border:1px solid #ccc;text-align:right;\">Value</th>
            </tr>
          </thead>
          <tbody>
            {holdings_rows}
          </tbody>
        </table>
        {cash_section}
      </div>
    </div>
    """
    return section


def build_html_report(clients: List[Dict[str, Any]], portfolios: Dict[str, Dict[str, Any]]) -> str:
    """Build the full HTML document for the client report.

    Args:
        clients: List of client document dicts.
        portfolios: Dictionary mapping account numbers to portfolio documents.

    Returns:
        Complete HTML document as a string.
    """
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    client_count = len(clients)

    client_sections = ""
    for index, client in enumerate(clients):
        account_number = str(client.get("accountNumber", ""))
        portfolio = portfolios.get(account_number)
        client_sections += build_client_section(client, portfolio, index)

    body = (
        f"<p>Report generated at <strong>{generated_at}</strong> covering "
        f"<strong>{client_count}</strong> client(s).</p>"
        f"{client_sections}"
    )

    html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport\" content=\"width=device-width, initial-scale=1.0\">
  <title>Client Portfolio Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 20px; color: #222; }}
    h1 {{ font-family: 'Arial Black', Arial, sans-serif; }}
    h4 {{ margin-bottom: 6px; }}
    button {{ outline: none; }}
    button:focus {{ outline: 2px solid #888; }}
  </style>
</head>
<body>
{EMAIL_HEADER}
{body}
{EMAIL_FOOTER}
<script>
  function toggleSection(id) {{
    var section = document.getElementById(id);
    var arrow = document.getElementById(id + '-arrow');
    if (section.style.display === 'none') {{
      section.style.display = 'block';
      arrow.innerHTML = '&#9660;';
    }} else {{
      section.style.display = 'none';
      arrow.innerHTML = '&#9654;';
    }}
  }}
</script>
</body>
</html>
"""
    return html_doc


def generate_client_report(output_path: str | None = None) -> str:
    """Fetch client and portfolio data and write the HTML report to disk.

    Args:
        output_path: Optional file path for the HTML report.  When ``None``,
            the report is written to ``reporting/client_portfolio_report.html``
            relative to the project root.

    Returns:
        The absolute path of the generated HTML file.

    Raises:
        RuntimeError: If the data fetch or file write fails.
    """
    if output_path is None:
        output_path = str(project_root / "reporting" / "client_portfolio_report.html")

    logger.info("Fetching clients from '%s.%s'...", DATABASE_NAME, CLIENTS_COLLECTION)
    clients = fetch_clients()
    logger.info("Fetched %d client(s).", len(clients))

    logger.info("Fetching portfolios from '%s.%s'...", DATABASE_NAME, PORTFOLIOS_COLLECTION)
    portfolios = fetch_portfolios()
    logger.info("Fetched %d portfolio document(s).", len(portfolios))

    if not clients:
        logger.warning("No clients found; generating empty report.")

    html_content = build_html_report(clients, portfolios)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html_content, encoding="utf-8")
    logger.info("HTML report written to %s", out.resolve())
    return str(out.resolve())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    try:
        path = generate_client_report()
        print(f"Report generated: {path}")
    except Exception as exc:
        logger.error("Failed to generate client report: %s", exc, exc_info=True)
        raise SystemExit(1)
