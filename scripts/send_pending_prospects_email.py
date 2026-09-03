"""Script to send an email containing an HTML table of prospects with status "Pending".

Queries the MongoDB ``prospects`` collection for documents whose ``status``
field equals ``"Pending"`` (case-insensitive) and sends a branded email
containing an HTML table of matching prospects.

The email table includes the following columns:
    - First Name
    - Last Name
    - Email
    - Telephone
    - Preferred Topic

Environment variables required:
    MONGODB_SRV: MongoDB connection string.
    DATABASE_NAME: Database name (defaults to ``VESTRA_PROD``).
    BREVO_API_KEY: Brevo API key for sending the email.
    BREVO_EMAIL_SENDER: Verified Brevo sender address.
    NOTIFICATION_EMAIL: Recipient address for the pending-prospects report
        (optional; falls back to ``BREVO_EMAIL_SENDER``).
"""

import importlib.util
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

project_root = Path(__file__).resolve().parent.parent

# Load environment variables from the project root ``.env`` file.
load_dotenv(project_root / ".env")

# ``send_email`` lives in the ``email`` package at the project root.  We load
# it dynamically here because this script is invoked directly (not as part of
# the ``email`` package) and a standard absolute import would require
# manipulating ``sys.path`` in a less explicit way.
send_email_module_path = project_root / "email" / "send_email.py"
_spec = importlib.util.spec_from_file_location("send_email", send_email_module_path)
_send_email_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_send_email_module)
send_email = _send_email_module.send_email

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DATABASE_NAME = os.getenv("DATABASE_NAME", "VESTRA_PROD")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "prospects")
PENDING_STATUS = "Pending"
# Recipient for the pending-prospects report.  Falls back to the verified
# Brevo sender address when ``NOTIFICATION_EMAIL`` is not explicitly set.
NOTIFICATION_EMAIL = os.getenv("NOTIFICATION_EMAIL") or os.getenv("BREVO_EMAIL_SENDER")


def get_mongo_client() -> MongoClient:
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


def fetch_pending_prospects() -> list[dict]:
    """Query the MongoDB ``prospects`` collection for documents with status ``Pending``.

    Performs a case-insensitive regex match on the ``status`` field and
    returns all matching prospect documents as a list of dictionaries.

    Returns:
        List of prospect document dicts whose ``status`` matches ``Pending``.

    Raises:
        RuntimeError: If the MongoDB connection or query operation fails.
    """
    client = get_mongo_client()
    try:
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        cursor = collection.find({"status": {"$regex": f"^{PENDING_STATUS}$", "$options": "i"}})
        return list(cursor)
    except PyMongoError as e:
        raise RuntimeError(f"Failed to query MongoDB: {e}") from e
    finally:
        client.close()


def build_html_table(prospects: list[dict]) -> str:
    """Build an HTML table string from a list of prospect documents.

    The table includes the following columns:
    ``first_name``, ``last_name``, ``email``, ``telephone``, and
    ``preferredTopic``.  If the prospect list is empty, a placeholder
    message is returned instead of an empty table.

    Args:
        prospects: List of prospect dicts to render.

    Returns:
        HTML string representing the prospects table or an empty-state message.
    """
    if not prospects:
        return "<p>No prospects with status <strong>Pending</strong> were found.</p>"

    fields = [
        ("first_name", "First Name"),
        ("last_name", "Last Name"),
        ("email", "Email"),
        ("telephone", "Telephone"),
        ("preferredTopic", "Preferred Topic"),
    ]

    header_cells = "".join(f"<th style=\"padding:6px 10px;border:1px solid #ccc;text-align:left;\">{label}</th>" for _, label in fields)
    header_row = f"<tr style=\"background:#f2f2f2;\">{header_cells}</tr>"

    rows = []
    for doc in prospects:
        cells = "".join(
            f"<td style=\"padding:6px 10px;border:1px solid #ccc;\">{doc.get(key, '')}</td>"
            for key, _ in fields
        )
        rows.append(f"<tr>{cells}</tr>")

    table = f"""
<table style=\"border-collapse:collapse;font-family:Arial,sans-serif;font-size:0.9em;width:100%;\">
  <thead>{header_row}</thead>
  <tbody>
    {''.join(rows)}
  </tbody>
</table>
"""
    return table


def build_email_body(table_html: str, count: int) -> str:
    """Compose the HTML email body wrapping the prospects table.

    Args:
        table_html: HTML table string produced by :func:`build_html_table`.
        count: Number of pending prospects being reported.

    Returns:
        HTML string containing a summary paragraph and the prospects table.
    """
    return (
        f"<p>There are currently <strong>{count}</strong> prospect(s) with status "
        f"<strong>Pending</strong>:</p>"
        f"{table_html}"
    )


def send_pending_prospects_email(recipient: str | None = None) -> None:
    """Fetch pending prospects and send a branded HTML report email.

    Queries the ``prospects`` collection for documents whose ``status``
    field equals ``"Pending"`` (case-insensitive), builds an HTML table
    of the matching records, and sends the report to the specified
    recipient via :func:`email.send_email.send_email`.

    Args:
        recipient: Optional override for the report recipient.  When
            ``None``, the address is resolved from ``NOTIFICATION_EMAIL``
            or ``BREVO_EMAIL_SENDER`` in the environment.

    Raises:
        RuntimeError: If no recipient can be determined or if the email
            send operation fails.
    """
    recipient = recipient or NOTIFICATION_EMAIL
    if not recipient:
        raise RuntimeError(
            "No recipient email configured. Set NOTIFICATION_EMAIL or BREVO_EMAIL_SENDER in .env."
        )

    logger.info("Fetching prospects with status '%s' from '%s.%s'...", PENDING_STATUS, DATABASE_NAME, COLLECTION_NAME)
    prospects = fetch_pending_prospects()
    count = len(prospects)
    logger.info("Found %d pending prospect(s).", count)

    if count == 0:
        logger.info("No pending prospects found; skipping email send.")
        return

    table_html = build_html_table(prospects)
    message = build_email_body(table_html, count)

    subject = f"Pending Prospects Report – {count} Prospect(s)"
    logger.info("Sending email to %s with subject '%s'.", recipient, subject)

    send_email({
        "email": recipient,
        "subject": subject,
        "message": message,
    })
    logger.info("Email sent successfully.")


if __name__ == "__main__":
    try:
        send_pending_prospects_email()
    except Exception as exc:
        logger.error("Failed to send pending prospects email: %s", exc, exc_info=True)
        sys.exit(1)
