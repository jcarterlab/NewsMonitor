import os
from datetime import datetime, timezone

from google import genai

import config
from news_monitoring_pipeline.scrape_headlines import scrape_headlines
from news_monitoring_pipeline.deduplicate_headlines import deduplicate_headlines
from news_monitoring_pipeline.identify_risk_headlines import identify_risk_headlines
from news_monitoring_pipeline.scrape_stories import scrape_stories
from news_monitoring_pipeline.summarise_stories import summarise_stories
from news_monitoring_pipeline.store_headlines import store_headlines

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
    # Headline collection
    headlines_df = scrape_headlines(config)
    new_headlines_df = deduplicate_headlines(headlines_df, config)

    # Risk identification
    risk_headlines_df = identify_risk_headlines(client, new_headlines_df, config)

    # Story processing
    story_texts = scrape_stories(risk_headlines_df, config)
    final_summary = summarise_stories(client, story_texts, today_date, config)

    # Storage (if summary generation is successful)
    if final_summary and len(final_summary.split()) >= 50:
        store_headlines(new_headlines_df, config)
        
    return final_summary


# ----------------------------------------------------------------------
# ENTRY POINT
# ----------------------------------------------------------------------

if __name__ == "__main__":
    gemini_api_key = os.getenv('GEMINI_API_KEY')
    if not gemini_api_key:
        raise RuntimeError('GEMINI_API_KEY not found in environment.')
    
    client = genai.Client(api_key=gemini_api_key)
    today_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    final_summary = run_pipeline(client, today_date, config)

    print('\n--- Final Summary ---\n')
    print(final_summary)