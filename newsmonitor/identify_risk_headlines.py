"""
Risk headline identification module.

This module processes scraped news headlines and uses an LLM to identify 
those that may represent risks to a specified entity and risk category.
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
    logger.debug(
        'Numbering headlines count=%d',
        len(new_headlines_df)
    )

    headlines = new_headlines_df['headline'].fillna('')

    numbered_headlines = [
        f'{i}. {headline or ""}'
        for i, headline in enumerate(headlines, start=1)
    ]

    logger.debug(
        'Numbered headlines generated count=%d',
        len(numbered_headlines)
    )

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
        logger.debug('No headlines to batch')
        return []
    
    logger.debug(
        'Batching headlines count=%d batch_size=%d',
        len(numbered_headlines),
        batch_size
    )

    batches = []

    for start in range(0, len(numbered_headlines), batch_size):
        batches.append(
            '\n'.join(numbered_headlines[start:start + batch_size])
        )

    logger.debug(
        'Created headline batches count=%d',
        len(batches)
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
        logger.error('Gemini response is None')
        return [], False
    
    text = getattr(response, 'text', None)

    if not text:
        logger.error('Gemini returned empty response')
        return [], False
    
    if '[' not in text or ']' not in text:
        logger.warning(
            'No list found in Gemini response text=%s', 
            text[:100]
        )
        return [], False
    
    try:
        inside = text.split('[', 1)[1].split(']', 1)[0]

        indices = [
            int(n) - 1 for n in re.findall(r"\d+", inside)
        ]

        validated_indices = [
            i for i in indices if 0 <= i < max_len
        ]

        logger.debug(
            'Extracted indices count=%d valid_count=%d max_len=%d',
            len(indices),
            len(validated_indices),
            max_len
        )

        return validated_indices, True
    
    except ValueError:
        logger.error(
            'Failed to parse indices from Gemini response text=%s',
            text[:200],
            exc_info=True
        )
        return [], False



# ----------------------------------------------------------------------
# RISK IDENTIFICATION FUNCTIONS
# ----------------------------------------------------------------------

def return_risk_headlines(client, prompt, i, max_len, config):
    """
    Call Gemini to identify risk-relevant headline indices for a single headline batch. 

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
            List of zero-based indices representing selected risk headlines or an empty list.
    """
    retry_attempts = config.LLM_RETRY_ATTEMPTS
    wait_time = config.LLM_WAIT_TIME
    model = config.BASIC_MODEL

    logger.debug(
        'Requesting risk headline identification batch=%d model=%s max_len=%d retry_attempts=%d',
        i,
        model,
        max_len,
        retry_attempts
    )
    
    for attempt in range(1, retry_attempts + 1):
        try:
            logger.debug(
                'Calling Gemini batch=%d attempt=%d/%d',
                i,
                attempt,
                retry_attempts
            )

            response = client.models.generate_content(
                model=model, 
                contents=prompt
            )
            index_numbers, response_parsed = extract_index_numbers(response, max_len)

            if response_parsed:
                logger.debug(
                    'Parsed risk headline indices batch=%d attempt=%d/%d count=%d',
                    i,
                    attempt,
                    retry_attempts,
                    len(index_numbers)
                )
                return index_numbers
            
            if attempt < retry_attempts:
                sleep_seconds = wait_time * attempt
                logger.warning(
                    'Could not parse Gemini response batch=%d attempt=%d/%d; retrying in %ds',
                    i,
                    attempt,
                    retry_attempts,
                    sleep_seconds
                )
                time.sleep(sleep_seconds)
            else:
                logger.error(
                    'Could not parse Gemini response batch=%d after %d attempts',
                    i,
                    retry_attempts
                )
                return []

        except Exception:
            if attempt < retry_attempts:
                sleep_seconds = wait_time * attempt
                logger.warning(
                    'LLM call failed batch=%d attempt=%d/%d; retrying in %ds',
                    i,
                    attempt,
                    retry_attempts,
                    sleep_seconds,
                    exc_info=True
                )
                time.sleep(sleep_seconds)
            else:
                logger.error(
                    'LLM call failed batch=%d after %d attempts',
                    i,
                    retry_attempts,
                    exc_info=True
                )
                return []         



# ----------------------------------------------------------------------
# ORCHESTRATION FUNCTIONS 
# ----------------------------------------------------------------------

def identify_risk_headlines(client, new_headlines_df, config):
    """
    Identify potential risk-related headlines using an LLM.

    Args:
        client (object):
            Gemini client instance. 
        new_headlines_df (pd.DataFrame):
            DataFrame containing a 'headline' column with scraped headlines.
        config (module):
            Configuration module containing 'LLM_RETRY_ATTEMPTS', 'LLM_WAIT_TIME', 
            'BASIC_MODEL', 'LLM_HEADLINE_BATCH_SIZE', 'ENTITY_OF_CONCERN', 'RISK_TYPE', 
            and 'RISK_CONFIDENCE_THRESHOLD'.

    Returns:
        pd.DataFrame:
            Subset of the input DataFrame containing headlines identified as potential risks.
    """
    max_len = len(new_headlines_df)

    logger.info(
        'Starting risk headline identification total_headlines=%d batch_size=%d model=%s',
        max_len,
        config.LLM_HEADLINE_BATCH_SIZE,
        config.BASIC_MODEL
    )

    numbered_headlines = number_headlines(new_headlines_df)

    headline_batches = batch_headlines(numbered_headlines, config)

    all_indices = []

    for i, batch in enumerate(headline_batches, start=1):
        logger.debug(
            'Processing batch batch_num=%d batch_size=%d',
            i,
            len(batch)
        )
        prompt = headline_identification_prompt(
            batch,
            config
        )

        indices = return_risk_headlines(
            client,
            prompt,
            i,
            max_len,
            config
        )

        logger.debug(
            'Batch completed batch_num=%d indices_found=%d',
            i,
            len(indices)
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

    logger.info(
        'Finished identifying risk headlines total_headlines=%d unique_headlines=%d',
        len(all_indices),
        len(unique_indices)
    )

    return new_headlines_df.iloc[all_indices]
