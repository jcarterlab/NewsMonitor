from dotenv import load_dotenv
import os

load_dotenv()

REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', 10))
MIN_HEADLINE_LENGTH = int(os.getenv('MIN_HEADLINE_LENGTH', 25))
LLM_HEADLINE_BATCH_SIZE = int(os.getenv('LLM_HEADLINE_BATCH_SIZE', 40))
LLM_RETRY_ATTEMPTS = int(os.getenv('LLM_RETRY_ATTEMPTS', 3))
LLM_WAIT_TIME = int(os.getenv("LLM_WAIT_TIME", 10))
MODEL = os.getenv('MODEL', 'gemini-2.5-flash')