"""
News story summarisation module. 

This module converts scraped news story text into a final 
summary using a two-stage LLM summarisation process.
"""

import logging
import time
from newsmonitor.build_prompts import story_text_summarization_prompt, executive_summary_prompt


# ----------------------------------------------------------------------
# LOGGING SETUP
# ----------------------------------------------------------------------

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------

def batch_story_texts(story_texts, config):
    """
    Group story texts into batches that do not exceed a maximum word limit.

    Args:
        story_texts (list[str]):
            List of story texts for successfully scraped stories.
        config (module):
            Configuration module containing 'LLM_STORY_WORDS_BATCH_SIZE'.

    Returns:
        list[str]:
            List of batched story text strings ready for LLM processing.
    """
    max_words = config.LLM_STORY_WORDS_BATCH_SIZE

    if max_words <= 0:
        raise ValueError('max_words must be greater than 0')
    
    batches = []
    current_batch = []
    words_counter = 0

    for i, story in enumerate(story_texts, start=1):

        if not isinstance(story, str) or not story.strip():
            continue
        
        story_words = len(story.split())

        # Story larger than batch limit
        if story_words > max_words:

            if current_batch:
                batches.append(' '.join(current_batch))
                current_batch, words_counter = [], 0

            batches.append(story)
            continue

        # Story fits in current batch
        if words_counter + story_words <= max_words:
            current_batch.append(story)
            words_counter += story_words

        # Batch overflow
        else:
            batches.append(' '.join(current_batch))
            current_batch, words_counter = [story], story_words

    if current_batch:
        batches.append(' '.join(current_batch))

    return batches



# ----------------------------------------------------------------------
# ORCHESTRATION FUNCTIONS 
# ----------------------------------------------------------------------

def summarise_story_text_batches(client, story_text_batches, today_date, config):
    """
    Generate summaries for batches of scraped story text using a basic LLM model.

    Args:
        client (object):
            Gemini client instance. 
        story_text_batches (list[str]):
            List of story text batches created from scraped articles.
        today_date (str):
            Date string used to contextualize the summary generation.
        config (module):
            Configuration module containing 'LLM_RETRY_ATTEMPTS', 'LLM_WAIT_TIME', 'BASIC_MODEL', 
            'ENTITY_OF_CONCERN' and 'TOPIC_OF_CONCERN'.

    Returns:
        list[str] | None:
            List of LLM-generated summaries for successfully processed story text batches, or 
            None if all batches fail.
    """
    retry_attempts = config.LLM_RETRY_ATTEMPTS
    llm_wait_time = config.LLM_WAIT_TIME
    basic_model = config.BASIC_MODEL
    wait_time = config.LLM_WAIT_TIME

    total_summary_words = 0
    summaries = []

    for i, story_text in enumerate(story_text_batches, start=1):

        prompt = story_text_summarization_prompt(
            today_date, 
            story_text,
            config
        )

        for attempt in range(1, retry_attempts + 1):

            backoff_time = wait_time * (2 ** (attempt - 1))

            try:
                response = client.models.generate_content(
                    model=basic_model, 
                    contents=prompt
                )

                response_text = response.text
                summary_text = response_text.strip() if response_text else ''

                if not summary_text:
                    raise ValueError('Empty summary text returned')
                
                summary_word_count = len(summary_text.split())
                total_summary_words += summary_word_count
                summaries.append(summary_text)

                break

            except Exception:
                if attempt < retry_attempts:
                    time.sleep(backoff_time)
                else:
                    logger.error(
                        'LLM call failed batch=%d after %d attempts',
                        i,
                        retry_attempts,
                        exc_info=True
                    )

    if not summaries:
        return None

    return summaries


def get_executive_summary(client, story_text_summaries, today_date, config):
    """
    Generate an executive summary from story-text batch summaries using an advanced LLM model.

    Args:
        client (object):
            Gemini client instance. 
        story_text_summaries (list[str]):
            List of LLM-generated summaries for successfully processed story text batches.
        today_date (str):
            Date string used to contextualize the summary generation.
        config (module):
            Configuration module containing 'LLM_RETRY_ATTEMPTS', 'LLM_WAIT_TIME', 'ADVANCED_MODEL', 
            'ENTITY_OF_CONCERN' and 'TOPIC_OF_CONCERN'.

    Returns:
        str | None:
            Executive summary text if generation succeeds, otherwise None.
    """
    retry_attempts = config.LLM_RETRY_ATTEMPTS
    llm_wait_time = config.LLM_WAIT_TIME
    advanced_model = config.ADVANCED_MODEL
    wait_time = config.LLM_WAIT_TIME

    combined_summaries = "\n\n".join(story_text_summaries)

    prompt = executive_summary_prompt(
        today_date,
        combined_summaries,
        config
    )

    for attempt in range(1, retry_attempts + 1):

        backoff_time = wait_time * (2 ** (attempt - 1))

        try:
            response = client.models.generate_content(
                model=advanced_model, 
                contents=prompt
            )

            response_text = response.text
            summary_text = response_text.strip() if response_text else ''

            if not summary_text:
                raise ValueError('Empty executive summary text returned')
            
            summary_word_count = len(summary_text.split())
            
            return summary_text
            
        except Exception:
            if attempt < retry_attempts:
                time.sleep(backoff_time)
            else:
                return None


def summarise_stories(client, story_texts, today_date, config):
    """
    Generate a final summary from scraped story texts.

    Args:
        client (object):
            Gemini client instance.
        story_texts (list[str]):
            List of story texts for stories successfully scraped.
        today_date (str):
            Date string used to contextualize summary generation.
        config (module):
            Configuration module containing 'LLM_RETRY_ATTEMPTS', 'LLM_WAIT_TIME', 'BASIC_MODEL', 
            'ADVANCED_MODEL', 'ENTITY_OF_CONCERN' and 'TOPIC_OF_CONCERN'.

    Returns:
        str:
            Final summary text, or a fallback message if no relevant stories are
            identified or summarisation fails.
    """

    if not story_texts:
        return 'No relevant stories were identified.'
    
    story_text_batches = batch_story_texts(
        story_texts, 
        config
    )

    story_text_summaries = summarise_story_text_batches(
        client,
        story_text_batches, 
        today_date, 
        config
    )

    if not story_text_summaries:
        return 'The LLM was not able to generate a summary'

    if len(story_text_summaries) == 1:
        summary_word_count = len(story_text_summaries[0].split())
        return story_text_summaries[0]

    executive_summary = get_executive_summary(
        client,
        story_text_summaries,
        today_date,
        config
    )

    if not executive_summary:
        return 'The LLM was not able to generate a summary'

    return executive_summary