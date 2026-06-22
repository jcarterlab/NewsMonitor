"""
Email summary module.

This module orchestrates the sending of processed news summaries via email.
It loads recipient data, filters active users and manages retry logic for
reliable delivery using the Resend API.
"""

import logging
import resend
import pandas as pd
import time
import markdown


# ----------------------------------------------------------------------
# LOGGING SETUP
# ----------------------------------------------------------------------

logger = logging.getLogger(__name__)



# ----------------------------------------------------------------------
# HELPER FUNCTIONS 
# ----------------------------------------------------------------------

def send_email(final_summary, recipient, today_date, config):
    """
    Send a news summary email to a single recipient.

    Args:
        final_summary (str):
            Final summary text to send.
        recipient (str):
            Email address of the recipient.
        today_date (str):
            Date string for the subject line.
        config (module):
            Configuration module containing various email settings.

    Returns:
        dict:
            Response from the Resend API.
    """
    try:
        resend.api_key = config.RESEND_API_KEY

        html_summary = markdown.markdown(final_summary)

        response = resend.Emails.send({
        'from': config.FROM_EMAIL,
        'to': recipient,
        'subject': f'News summary {today_date}',
        'html': f'<p>{html_summary}</p>'
        })
    
    except Exception:
        raise


    return response



# ----------------------------------------------------------------------
# ORCHESTRATION FUNCTIONS 
# ----------------------------------------------------------------------

def email_summary(final_summary, today_date, config):
    """
    Send the final summary email to all active recipients.

    Args:
        final_summary (str):
            Final summary text to send.
        today_date (str):
            Date string for the subject line.
        config (module):
            Configuration module containing email settings.
    """
    emails_path = config.EMAILS_PATH
    retry_attempts = config.EMAIL_RETRY_ATTEMPTS
    wait_time = config.EMAIL_WAIT_TIME

    try:
        emails_df = pd.read_csv(emails_path, encoding='utf-8')
    except FileNotFoundError:
        raise RuntimeError(f'{emails_path} not found')
    
    if emails_df.empty:
        raise RuntimeError(f'{emails_path} is empty')

    required_cols = {'email', 'is_active'}
    missing_cols = required_cols - set(emails_df.columns)
    if missing_cols:
        raise RuntimeError(f'{emails_path} missing required columns: {sorted(missing_cols)}')
    
    active_emails = emails_df.loc[
        emails_df['is_active']
        .astype(str)
        .str.strip()
        .str.lower()
        .isin(['true', '1', 'yes']),
        'email'
    ].str.strip()

    active_emails = active_emails[active_emails.ne('')]

    if active_emails.empty:
        return

    successful_sends = 0

    for i, recipient in enumerate(active_emails):
        email_sent = False

        for attempt in range(1, retry_attempts + 1):

            try:
                response = send_email(final_summary, recipient, today_date, config)
           
                if 'id' in response:
                    email_sent = True
                    successful_sends += 1
                    break
            
            except Exception:
                logger.error(
                    'Email send failed recipient=%s attempt=%d',
                    recipient,
                    attempt,
                    exc_info=True
                )

            if attempt < retry_attempts:
                time.sleep(wait_time)

        if not email_sent:
            logger.error(
                'Could not send email recipient=%s attempts=%d',
                recipient,
                retry_attempts
            )

        if i < len(active_emails):
            time.sleep(wait_time)