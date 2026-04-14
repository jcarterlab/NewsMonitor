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
    logger.info(
        'Sending email recipient=%s date=%s',
        recipient,
        today_date
    )
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
        logger.error(
            'Email sending failed recipient=%s date=%s',
            recipient,
            today_date,
            exc_info=True
        )
        raise

    logger.info(
        'Email sent successfully recipient=%s date=%s',
        recipient,
        today_date
    )

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

    logger.info(
        'Starting email summary path=%s date=%s',
        emails_path,
        today_date
    )

    try:
        emails_df = pd.read_csv(emails_path, encoding='utf-8')
    except FileNotFoundError:
        logger.error(
            'Emails file not found path=%s',
            emails_path,
            exc_info=True
        )
        raise RuntimeError(f'{emails_path} not found')
    
    if emails_df.empty:
        logger.error('Emails file is empty path=%s', emails_path)
        raise RuntimeError(f'{emails_path} is empty')

    required_cols = {'email', 'is_active'}
    missing_cols = required_cols - set(emails_df.columns)
    if missing_cols:
        logger.error(
            'Emails file missing required columns path=%s missing=%s',
            emails_path,
            sorted(missing_cols)
        )
        raise RuntimeError(f'{emails_path} missing required columns: {sorted(missing_cols)}')
    
    logger.info(
        'Loaded email recipients total=%d',
        len(emails_df)
    )
    
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
        logger.warning(
            'No active email recipients found path=%s',
            emails_path
        )
        return
    
    logger.info(
        'Active email recipients identified count=%d',
        len(active_emails)
    )

    successful_sends = 0

    for i, recipient in enumerate(active_emails):
        email_sent = False

        logger.info(
            'Processing email recipient recipient=%s recipient_index=%d total_recipients=%d',
            recipient,
            i,
            len(active_emails)
        )

        for attempt in range(1, retry_attempts + 1):
            logger.info(
                'Sending email recipient=%s attempt=%d max_attempts=%d',
                recipient,
                attempt,
                retry_attempts
            )

            try:
                response = send_email(final_summary, recipient, today_date, config)
           
                if 'id' in response:
                    logger.info(
                        'Email sent recipient=%s attempt=%d message_id=%s',
                        recipient,
                        attempt,
                        response['id']
                    )
                    email_sent = True
                    successful_sends += 1
                    break
            
                logger.warning(
                    'Email response missing id recipient=%s attempt=%d response=%s',
                    recipient,
                    attempt,
                    response
                )
            
            except Exception:
                logger.error(
                    'Email send failed recipient=%s attempt=%d',
                    recipient,
                    attempt,
                    exc_info=True
                )

            if attempt < retry_attempts:
                logger.info(
                    'Retrying email after wait recipient=%s wait_time=%s',
                    recipient,
                    wait_time
                )
                time.sleep(wait_time)

        if not email_sent:
            logger.error(
                'Could not send email recipient=%s attempts=%d',
                recipient,
                retry_attempts
            )

        if i < len(active_emails):
            time.sleep(wait_time)

    logger.info(
        'Sent email summary successful_sends=%d active_emails=%d',
        successful_sends,
        len(active_emails)
    )