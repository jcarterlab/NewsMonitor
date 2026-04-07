"""
Configuration settings module.

This module loads values from environment variables where 
available with sensible defaults provided for local execution.
"""


from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv()


# --------------------------------------------------
# Core API keys
# --------------------------------------------------

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

# --------------------------------------------------
# Project paths
# --------------------------------------------------

# path to links.csv
BASE_DIR = Path(__file__).resolve().parent
LINKS_PATH = BASE_DIR / os.getenv('LINKS_FILE', 'links.csv')

# path to news_data.db
DATA_DIR = BASE_DIR / 'data'
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / 'news.db'

# path to emails.csv
EMAILS_PATH = BASE_DIR / os.getenv('EMAILS_FILE', 'emails.csv')


# --------------------------------------------------
# Monitoring parameters
# --------------------------------------------------

TOPIC_OF_CONCERN = os.getenv('TOPIC_OF_CONCERN', 'transport disruption events') # topic to monitor
ENTITY_OF_CONCERN = os.getenv('ENTITY_OF_CONCERN', 'a logistics firm operating in Colombia') # type of organisation monitoring
IDENTIFICATION_CONFIDENCE_THRESHOLD = int(os.getenv('IDENTIFICATION_CONFIDENCE_THRESHOLD', 95)) # percentage (0–100)

# --------------------------------------------------
# Pipeline parameters
# --------------------------------------------------

# Request settings
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', 10)) # seconds 

# Headline filtering
MIN_HEADLINE_LENGTH = int(os.getenv('MIN_HEADLINE_LENGTH', 25)) # characters
LLM_HEADLINE_BATCH_SIZE = int(os.getenv('LLM_HEADLINE_BATCH_SIZE', 40)) # headlines

# LLM configuration
LLM_RETRY_ATTEMPTS = int(os.getenv('LLM_RETRY_ATTEMPTS', 3)) # retries
LLM_WAIT_TIME = int(os.getenv('LLM_WAIT_TIME', 10)) # seconds
BASIC_MODEL = os.getenv('BASIC_MODEL', 'gemini-2.5-flash') # model type
ADVANCED_MODEL = os.getenv('ADVANCED_MODEL', 'gemini-2.5-pro') # model type

# News story processing
LLM_STORY_WORDS_BATCH_SIZE = int(os.getenv('LLM_STORY_WORDS_BATCH_SIZE', 12000)) # words

# Summary quality threshold
MIN_SUMMARY_WORDS = int(os.getenv('MIN_SUMMARY_WORDS', 50)) # words

# --------------------------------------------------
# Email parameters
# --------------------------------------------------

# API keys
RESEND_API_KEY = os.getenv('RESEND_API_KEY')

# Email settings
EMAIL_ENABLED = os.getenv('EMAIL_ENABLED', 'false').lower() in ('true', '1', 'yes') # boolean
FROM_EMAIL = os.getenv('FROM_EMAIL') or 'onboarding@resend.dev'
EMAIL_RETRY_ATTEMPTS = int(os.getenv('EMAIL_RETRY_ATTEMPTS', 3)) # retries
EMAIL_WAIT_TIME = int(os.getenv('EMAIL_WAIT_TIME', 2)) # second
