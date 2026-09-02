"""
Test script to send a sample email via the Brevo API.

This script is intended for manual validation of the send_email utility.
It will send a branded test message to a fixed recipient address.
"""

import logging
import sys
import os

# Configure logging before importing project modules.
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)

# Ensure the project root is on the path so send_email.py can be imported.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from send_email import send_email


def main():
    test_options = {
        'email': 'daiviet@vestracapital.com.au',
        'subject': 'Automated Email System Test',
        'message': '<p>This is a <strong>test automated email, from the email automation system</strong> from the Vestra Capital email utility.</p>',
    }

    send_email(test_options)


if __name__ == '__main__':
    main()
