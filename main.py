import pandas as pd
from datetime import datetime, timezone
from google import genai
import os
from dotenv import load_dotenv
import config
from risk_pipeline.scrape_headlines import scrape_headlines
from risk_pipeline.identify_risk_headlines import identify_risk_headlines
from risk_pipeline.scrape_stories import scrape_stories
from risk_pipeline.summarise_stories import summarise_stories



# ----------------------------------------------------------------------
# PARAMETERS
# ----------------------------------------------------------------------

llm_retry_attempts = 3
llm_wait_time = 12
llm_story_words_batch_size = 12000

today_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
entity_description = 'a logistics firm'
risk_type = 'transport disruption events'
risk_confidence_threshold = 95



# ----------------------------------------------------------------------
# ENVIRONMENT SETUP
# ----------------------------------------------------------------------

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY not found in environment.")

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))


# ----------------------------------------------------------------------
# MAIN PIPELINE
# ----------------------------------------------------------------------


'''
headlines_df = scrape_headlines('links.csv', config)
headlines_df.to_csv('headlines.csv', index=False)
'''
headlines_df = pd.read_csv('headlines.csv')

risk_headlines_df = identify_risk_headlines(
    client, 
    headlines_df, 
    entity_description, 
    risk_type,
    risk_confidence_threshold,
    config
)

print(risk_headlines_df)





'''

story_texts = scrape_stories(risk_headlines_df, request_timeout)

executive_summary = summarise_stories(
    client,
    llm_retry_attempts,
    llm_wait_time,
    llm_story_words_batch_size,
    story_texts, 
    today_date, 
    entity_description, 
    risk_type
)

print(executive_summary)

'''

