"""
Email sending utility for Vestra Capital communications.

Uses the Brevo (formerly Sendinblue) transactional email API to send
HTML emails with a branded header and footer.
"""

import logging
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

# Load environment variables from the project .env file so the module
# can be used directly without requiring the caller to export variables.
load_dotenv(Path(__file__).resolve().parent / '.env')

# Module-level logger for email sending operations.
logger = logging.getLogger(__name__)

# Common HTML wrapper for all outbound emails to maintain brand consistency.
EMAIL_HEADER = """
  <span style="font-family:Arial">
    <hr>
    <h1><span style="font-size: 1.5em; font-family: 'Arial Black'; font-weight: 999;">VESTRA</span><br><span style="font-size: 0.75em; letter-spacing: 7.2; font-family: 'Arial Narrow', Arial, sans-serif; font-weight: lighter;">CAPITAL</span></h1>
    <hr>
    <br>
"""

# Closing HTML appended to every email containing contact details and legal links.
EMAIL_FOOTER = """
    <br>
    <br>
    <hr>
    <span style="font-size: 0.75em;">
    This email is from <strong>Vestra Capital</strong> (<a href="https://www.vestracapital.com.au">vestracapital.com.au</a>)
    <br>
    <p>If you have any questions, please do not hesitate to contact us: <a href="mailto:team@vestracapital.com.au">team@vestracapital.com.au</a></p>
    </span>
    <hr>
    <span style="font-size: 0.6em;">
      General info only, not personal advice. Consider your aims and finances before acting. Review full terms and seek independent advice — <a href="https://www.vestracapital.com.au/privacy-policy">Privacy Policy</a>&nbsp;| &nbsp;<a href="https://www.vestracapital.com.au/terms-of-service">Terms of Use</a>
    </span>
  </span>
"""


def send_email(options):
    """
    Send a transactional email via the Brevo API.

    Args:
        options (dict): Email payload containing the following keys:
            - email (str): Recipient email address.
            - subject (str): Email subject line.
            - message (str): HTML body content inserted between header and footer.

    Returns:
        dict: Parsed JSON response from the Brevo API.

    Raises:
        ValueError: If required environment variables are missing.
        requests.HTTPError: If the Brevo API returns an unsuccessful status code.
    """
    try:
        logger.info('Attempting to send email with options: %s', options)

        # Required environment variables for Brevo authentication and sender identity.
        api_key = os.getenv('BREVO_API_KEY')
        sender_email = os.getenv('BREVO_EMAIL_SENDER')

        if not api_key or not sender_email:
            raise ValueError('BREVO_API_KEY and BREVO_EMAIL_SENDER must be set in environment variables.')

        headers = {
            'api-key': api_key,
            'Content-Type': 'application/json',
            'accept': 'application/json',
        }

        # Wrap the caller's message with the standardised branded header and footer.
        payload = {
            'sender': {'email': sender_email, 'name': 'Vestra Capital'},
            'to': [{'email': options['email']}],
            'subject': options['subject'],
            'htmlContent': f"{EMAIL_HEADER}{options['message']}{EMAIL_FOOTER}",
        }

        # Brevo SMTP transactional email endpoint.
        response = requests.post(
            'https://api.brevo.com/v3/smtp/email',
            headers=headers,
            json=payload,
        )

        logger.debug('Brevo API response status: %s', response.status_code)
        logger.debug('Brevo API response body: %s', response.text)

        response.raise_for_status()

        response_body = response.json()
        message_id = response_body.get('messageId', 'N/A')
        logger.info('Email accepted by Brevo to %s (messageId: %s)', options['email'], message_id)

        return response_body
    except Exception as error:
        logger.error('Error in send_email function: %s', error, exc_info=True)
        raise error
