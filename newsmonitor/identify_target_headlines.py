"""
Target headline identification module.

This module processes scraped headlines and uses an LLM to identify those 
that may be of interest to a certain entity based on a specific topic.
"""

import logging
import time
import re
from newsmonitor.build_prompts import headline_identification_prompt 


# ----------------------------------------------------------------------
# LOGGING SETUP
# ----------------------------------------------------------------------

logger = logging.getLogger(__name__)



# ----------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------

def number_headlines(new_headlines_df):
    """
    Convert a DataFrame of headlines into numbered headline strings.

    Args:
        new_headlines_df (pd.DataFrame):
            DataFrame containing a 'headline' column with scraped headlines.

    Returns:
        list[str]:
            A list of headline strings each prefixed with an index number.
    """

    headlines = new_headlines_df['headline'].fillna('')

    numbered_headlines = [
        f'{i}. {headline or ""}'
        for i, headline in enumerate(headlines, start=1)
    ]

    return numbered_headlines


def batch_headlines(numbered_headlines, config):
    """
    Split numbered headlines into newline-separated batches for LLM processing.

    Args:
        numbered_headlines (list[str]):
            List of numbered headline strings.
        config (module):
            Configuration module containing 'LLM_HEADLINE_BATCH_SIZE'.

    Returns:
        list:
            List of strings each containing up to 'LLM_HEADLINE_BATCH_SIZE' numbered headlines.
    """
    batch_size = config.LLM_HEADLINE_BATCH_SIZE

    if not numbered_headlines:
        return []

    batches = []

    for start in range(0, len(numbered_headlines), batch_size):
        batches.append(
            '\n'.join(numbered_headlines[start:start + batch_size])
        )

    return batches


def extract_index_numbers(response, max_len):
    """
    Extract index numbers from a Gemini response containing a Python-style list.

    Args:
        response (object):
            Gemini response object expected to contain a 'text' attribute.
        max_len (int):
            Maximum possible number of headlines (used to validate indices before they are used with '.iloc').

    Returns:
        tuple:
            list[int]:
                List of zero-based indices extracted from the Gemini response or an empty list.
            str:
                Parsing status (e.g. True or False).
    """
    if response is None:
        return [], False
    
    text = getattr(response, 'text', None)

    if not text:
        return [], False
    
    if '[' not in text or ']' not in text:
        return [], False
    
    try:
        inside = text.split('[', 1)[1].split(']', 1)[0]

        indices = [
            int(n) - 1 for n in re.findall(r"\d+", inside)
        ]

        validated_indices = [
            i for i in indices if 0 <= i < max_len
        ]

        return validated_indices, True
    
    except ValueError:
        return [], False


def return_target_headlines(client, prompt, i, max_len, config):
    """
    Call Gemini to identify target headline indices for a single headline batch. 

    Args:
        client (object):
             Gemini client instance. 
        prompt (str):
            The full prompt text for this batch of headlines.
        i (int):
            Batch number (used for logging).
        max_len (int):
            Maximum number of headlines (used to validate indices before they are used with '.iloc').
        config (module):
            Configuration module containing 'LLM_RETRY_ATTEMPTS', 'LLM_WAIT_TIME' and 'BASIC_MODEL'. 

    Returns:
        list[int]:
            List of zero-based indices representing selected target headlines or an empty list.
    """
    retry_attempts = config.LLM_RETRY_ATTEMPTS
    wait_time = config.LLM_WAIT_TIME
    model = config.BASIC_MODEL
    wait_time = config.LLM_WAIT_TIME

    for attempt in range(1, retry_attempts + 1):

        backoff_time = wait_time * (2 ** (attempt - 1))
        
        
        try:

            response = client.models.generate_content(
                model=model, 
                contents=prompt
            )
            index_numbers, response_parsed = extract_index_numbers(response, max_len)

            if response_parsed:
                return index_numbers
            
            if attempt < retry_attempts:
                time.sleep(backoff_time)
            else:
                return []

        except Exception:
            if attempt < retry_attempts:
                time.sleep(backoff_time)
            else:
                return []         



# ----------------------------------------------------------------------
# ORCHESTRATION FUNCTIONS 
# ----------------------------------------------------------------------

def identify_target_headlines(client, new_headlines_df, config):
    """
    Identify target headlines using an LLM.

    Args:
        client (object):
            Gemini client instance. 
        new_headlines_df (pd.DataFrame):
            DataFrame containing a 'headline' column with scraped headlines.
        config (module):
            Configuration module containing 'LLM_RETRY_ATTEMPTS', 'LLM_WAIT_TIME', 
            'BASIC_MODEL', 'LLM_HEADLINE_BATCH_SIZE', 'ENTITY_OF_CONCERN', 
            'TOPIC_OF_CONCERN', and 'IDENTIFICATION_CONFIDENCE_THRESHOLD'.

    Returns:
        pd.DataFrame:
            Subset of the input DataFrame containing target headlines.
    """
    max_len = len(new_headlines_df)

    numbered_headlines = number_headlines(new_headlines_df)

    headline_batches = batch_headlines(numbered_headlines, config)

    all_indices = []

    for i, batch in enumerate(headline_batches, start=1):
        prompt = headline_identification_prompt(
            batch,
            config
        )

        indices = return_target_headlines(
            client,
            prompt,
            i,
            max_len,
            config
        )

        all_indices.extend(indices)

    unique_indices = set(all_indices)
    dup_count = len(all_indices) - len(unique_indices)

    dup_count = len(all_indices) - len(set(all_indices))

    if dup_count > 0:
        logger.warning(
            'Duplicate indices detected duplicate_count=%d total_indices=%d',
            dup_count,
            len(all_indices)
        )

    return new_headlines_df.iloc[all_indices]
