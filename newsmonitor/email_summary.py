import resend
import pandas as pd
import time
import markdown

def send_email(final_summary, recipient, today_date, config):
    """
    Send a risk summary email to a single recipient.

    Args:
        final_summary (str):
            Final summary text to send.
        recipient (str):
            Email address of the recipient.
        today_date (str):
            Date string for the subject line.
        config (module):
            Configuration module containing email settings.

    Returns:
        dict:
            Response from the Resend API.
    """
    resend.api_key = config.RESEND_API_KEY

    html_summary = markdown.markdown(final_summary)

    response = resend.Emails.send({
      'from': config.FROM_EMAIL,
      'to': recipient,
      'subject': f'Risk summary {today_date}',
      'html': f'<p>{html_summary}</p>'
    })

    return response


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
    ]

    active_emails = active_emails.str.strip()

    if active_emails.empty:
        print('No active email recipients found.')
        return

    for i, recipient in enumerate(active_emails):
        email_sent = False

        for attempt in range(1, config.EMAIL_RETRY_ATTEMPTS + 1):
            try:
                response = send_email(final_summary, recipient, today_date, config)
           
                if 'id' in response:
                    print(f'Email sent to {recipient}: {response["id"]}')
                    email_sent = True
                    break
            
                print(
                    f'\nError: email response missing id\n'
                    f'    recipient={recipient}\n'
                    f'    attempt={attempt}\n'
                    f'    response={response}\n'
                )
            
            except Exception as e:
                print(
                    f'\nError: email failed to send\n'
                    f'    recipient={recipient}\n'
                    f'    attempt={attempt}\n'
                    f'    {type(e).__name__}: {e}\n'
                )

            if attempt < config.EMAIL_RETRY_ATTEMPTS:
                time.sleep(config.EMAIL_WAIT_TIME)

        if i < len(active_emails) - 1:
            time.sleep(config.EMAIL_WAIT_TIME)

        if not email_sent:
            print(
                f'\nError: could not send email to {recipient}. '
                f'Moving on after {config.EMAIL_RETRY_ATTEMPTS} attempts.\n'
            )