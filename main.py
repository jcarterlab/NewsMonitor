import sys
from datetime import datetime, timezone
import logging
import config
from logging_config import setup_logging
from google import genai
from newsmonitor.scrape_headlines import scrape_headlines
from newsmonitor.deduplicate_headlines import deduplicate_headlines
from newsmonitor.identify_target_headlines import identify_target_headlines
from newsmonitor.scrape_stories import scrape_stories
from newsmonitor.summarise_stories import summarise_stories
from newsmonitor.store_data import store_data
from newsmonitor.email_summary import email_summary


# ----------------------------------------------------------------------
# LOGGING CONFIGURATION
# ----------------------------------------------------------------------

setup_logging(logging.INFO, logging.DEBUG, config)
logger = logging.getLogger(__name__)
run_id = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')


# ----------------------------------------------------------------------
# MAIN PIPELINE
# ----------------------------------------------------------------------

def run_pipeline(client, today_date, config):
    """
    Run the complete targeted news monitoring pipeline.

    Args:
        client (object):
            Gemini client instance.
        today_date (str):
            Date string used to contextualise summarisation.
        config (module):
            Configuration module containing pipeline settings.

    Returns:
        str:
            Final summary generated from relevant news stories.
    """
    logger.info('run_id=%s | Starting run...', run_id)

    # Headline collection
    headlines_df = scrape_headlines(config)
    new_headlines_df = deduplicate_headlines(headlines_df, config)

    # Headline identification
    target_headlines_df = identify_target_headlines(client, new_headlines_df, config)

    # Story text processing
    story_texts = scrape_stories(target_headlines_df, config)
    final_summary = summarise_stories(client, story_texts, today_date, config)

    # Summary validation
    if not final_summary or len(final_summary.split()) < config.MIN_SUMMARY_WORDS:
        logger.error('run_id=%s | Summary failed validation', run_id)
        raise RuntimeError('Summary failed validation')

    # Data storage 
    store_data(final_summary, new_headlines_df, today_date, config)

    # Email the summary (if email enabled)
    if config.EMAIL_ENABLED:
        email_summary(final_summary, today_date, config)

    logger.info('run_id=%s - Ending run.\n', run_id)

    return final_summary


# ----------------------------------------------------------------------
# ENTRY POINT
# ----------------------------------------------------------------------

if __name__ == '__main__':
    try:
        if not config.GEMINI_API_KEY:
            raise RuntimeError('Please set your Gemini API key in the .env file.')
        
        if config.EMAIL_ENABLED and not config.RESEND_API_KEY:
            raise RuntimeError('Please set your Resend API key in the .env file.')
        
        client = genai.Client(api_key=config.GEMINI_API_KEY)
        today_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        
        summary = run_pipeline(client, today_date, config)

        if not summary:
            logger.error('run_id=%s | Pipeline completed but no summary generated', run_id)
            sys.exit(1)

    except Exception:
        logger.exception('run_id=%s | Fatal error in pipeline', run_id)
        sys.exit(1)