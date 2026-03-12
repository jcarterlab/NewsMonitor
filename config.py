"""
Configuration settings module.

This module loads values  from environment variables where 
available, with sensible defaults provided for local execution.
"""


from dotenv import load_dotenv
import os

load_dotenv()


# --------------------------------------------------
# Links file path
# --------------------------------------------------

LINKS_FILE = os.getenv('LINKS_FILE', 'links.csv') # path to links file


# --------------------------------------------------
# Request settings
# --------------------------------------------------

REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', 10)) # seconds 


# --------------------------------------------------
# Headline filtering
# --------------------------------------------------

MIN_HEADLINE_LENGTH = int(os.getenv('MIN_HEADLINE_LENGTH', 25)) # characters
LLM_HEADLINE_BATCH_SIZE = int(os.getenv('LLM_HEADLINE_BATCH_SIZE', 40)) # headlines


# --------------------------------------------------
# LLM configuration
# --------------------------------------------------

LLM_RETRY_ATTEMPTS = int(os.getenv('LLM_RETRY_ATTEMPTS', 3)) # retries
LLM_WAIT_TIME = int(os.getenv('LLM_WAIT_TIME', 10)) # seconds
BASIC_MODEL = os.getenv('BASIC_MODEL', 'gemini-2.5-flash') # model type
ADVANCED_MODEL = os.getenv('ADVANCED_MODEL', 'gemini-2.5-flash') # model type


# --------------------------------------------------
# News story processing
# --------------------------------------------------

LLM_STORY_WORDS_BATCH_SIZE = int(os.getenv('LLM_STORY_WORDS_BATCH_SIZE', 12000)) # words


# --------------------------------------------------
# Risk configuration
# --------------------------------------------------

ENTITY_OF_CONCERN = os.getenv('ENTITY_OF_CONCERN', 'a logistics firm') # type of organisation
RISK_TYPE = os.getenv('RISK_TYPE', 'transport disruption events') # type of risk
RISK_CONFIDENCE_THRESHOLD = int(os.getenv('RISK_CONFIDENCE_THRESHOLD', 95)) # percentage (0–100)