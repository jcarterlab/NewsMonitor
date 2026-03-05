import time
import re
from risk_pipeline.prompts import headline_identification_prompt 


# ----------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------

def number_headlines(headlines_df):
    """
    Convert a DataFrame of headlines into numbered headline strings.

    Args:
        headlines_df (pd.DataFrame):
            DataFrame containing a 'Headline' column with scraped headlines.

    Returns:
        list (str):
            A list of headline strings each prefixed with an index number.
    """
    return [
        f'{i}. {headline or ""}'
        for i, headline in enumerate(headlines_df['headline'], start=1)
    ]


def batch_headlines(numbered_headlines, config):
    """
    Split numbered headlines into newline-separated batches for LLM processing.

    Args:
        numbered_headlines (list):
            List of numbered headline strings representing batches for LLM processing.
        config (module):
            Configuration module containing 'LLM_HEADLINE_BATCH_SIZE'.

    Returns:
        list:
            List of strings containing numbered headlines whose size is based on 'LLM_HEADLINE_BATCH_SIZE'. 
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
            Maximum number of headlines (used to validate indices before they are used with '.iloc').

    Returns:
        tuple:
            indices (list):
                List of zero-based indices extracted from the Gemini response.
            status (str):
                Parsing status (e.g. 'ok', 'empty_response', 'no_list', 'parse_error').
    """
    if response is None:
        print('Error: Gemini response is None')
        return [], 'empty_response'
    
    text = getattr(response, 'text', None)

    if not text:
        print('Error: Gemini returned an empty response')
        return [], 'empty_response'
    
    if "[" not in text or "]" not in text:
        print('Error: no python list found in Gemini response')
        return [], 'no_list'
    
    try:
        inside = text.split('[', 1)[1].split(']', 1)[0]

        indices = [int(n) - 1 for n in re.findall(r"\d+", inside)]

        validated_indices = [i for i in indices if 0 <= i < max_len]

        print(f'Extracted {len(validated_indices)} indices')

        return validated_indices, 'ok'
    
    except ValueError as e:
        print('Error: unable to extract index numbers from python list:\n')
        print(f'    text={text}')
        print(f'    {type(e).__name__}: {e}')
        return [], 'parse_error'



# ----------------------------------------------------------------------
# RISK HEADINE IDENTIFICATION 
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
            Configuration module containing 'LLM_RETRY_ATTEMPTS', 'LLM_WAIT_TIME', 
            'MODEL' and 'LLM_HEADLINE_BATCH_SIZE'.

    Returns:
        list:
            List of zero-based indices representing selected risk headlines.
    """
    retry_attempts = config.LLM_RETRY_ATTEMPTS
    wait_time = config.LLM_WAIT_TIME
    model = config.MODEL

    
    for attempt in range(1, retry_attempts + 1):
        try:
            response = client.models.generate_content(
                model=model, 
                contents=prompt
            )
            index_numbers, status = extract_index_numbers(response, max_len)

            if status == 'ok':
                return index_numbers
            else:
                if attempt < retry_attempts:
                    print(f'Error: could not parse index numbers for batch {i} on attempt {attempt}/{retry_attempts}')
                    time.sleep(wait_time * attempt)
                else:
                    print(f'Error: parsing failed for batch {i} after {retry_attempts} attempts.')
                    return []

        except Exception as e:
            if attempt < retry_attempts:
                print(f'Error: LLM call failed batch {i} on attempt {attempt}/{retry_attempts}')
                print(f'    Error type: {type(e).__name__}')
                print(f'    Error message: {e}')
                time.sleep(wait_time * attempt)
            else:
                print(f'Error: LLM call failed for batch {i} after {retry_attempts} attempts.')
                print(f'    Error type: {type(e).__name__}')
                print(f'    Error message: {e}')
                return []         


def identify_risk_headlines(
        client, 
        headlines_df, 
        entity_description, 
        risk_type,
        risk_confidence_threshold,
        config
    ):
    """
    Identify potential risk-related headlines using an LLM.

    Args:
        client (object):
            Gemini client instance. 
        headlines_df (pd.DataFrame):
            DataFrame containing a 'Headline' column with scraped headlines.
        entity_description (str):
            Description of the entity type of concern (e.g. 'a logistics firm').
        risk_type (str):
            Description of the risk category being identified (e.g. 'port disruption events').
        risk_confidence_threshold (int):
            Minimum confidence level the LLM should use when selecting headlines.
        config (module):
            Configuration module containing 'LLM_RETRY_ATTEMPTS', 'LLM_WAIT_TIME', 
            'MODEL' and 'LLM_HEADLINE_BATCH_SIZE'.

    Returns:
        pd.DataFrame:
            Subset of the input DataFrame containing headlines identified as potential risks.
    """
    numbered_headlines = number_headlines(headlines_df)

    max_len = len(headlines_df)

    headline_batches = batch_headlines(numbered_headlines, config)

    all_indices = []

    for i, batch in enumerate(headline_batches, start=1):
        prompt = headline_identification_prompt(
            entity_description, 
            risk_type, 
            risk_confidence_threshold, 
            batch
        )

        indices = return_risk_headlines(
            client,
            prompt,
            i,
            max_len,
            config
        )

        all_indices.extend(indices)

    dup_count = len(all_indices) - len(set(all_indices))

    if dup_count > 0:
        print(f'Warning: {dup_count} duplicate indices')

    print(f'\nTotal indices: {len(all_indices)}\n')

    return headlines_df.iloc[all_indices]
