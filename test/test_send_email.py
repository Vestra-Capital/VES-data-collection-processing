"""
Test script to send a sample email via the Brevo API.

This script is intended for manual validation of the send_email utility.
It will send a branded test message to a fixed recipient address.

Prerequisites:
    - ``BREVO_API_KEY`` and ``BREVO_EMAIL_SENDER`` must be set in ``.env``.
    - The recipient address should be changed to a real mailbox before
      executing this script outside of a controlled test environment.
"""

import logging
import sys
import os

# Configure logging before importing project modules so that all log
# output uses the same format and level from the start.
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)

# Ensure the project root is on the path so send_email.py can be imported
# regardless of the working directory from which this script is invoked.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from email.send_email import send_email


def main():
    """Send a single test email to verify the Brevo integration."""
    # TODO: Replace with a real test recipient before running outside
    # of a controlled environment.
    test_options = {
        'email': 'daiviet@vestracapital.com.au',
        'subject': 'Automated Email System Test',
        'message': '<p>This is a <strong>test automated email, from the email automation system</strong> from the Vestra Capital email utility.</p>',
    }

    response = send_email(test_options)
    # The Brevo API returns a JSON body containing at least ``messageId``.
    # In a production test harness, assert on ``response['messageId']`` to
    # confirm delivery acceptance.
    print(f"Brevo response: {response}")


if __name__ == '__main__':
    main()
