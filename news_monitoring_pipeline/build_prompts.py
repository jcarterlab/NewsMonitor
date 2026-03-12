"""
LLM prompt building module. 

This module constructs prompts to identify and summarise
news stories posing a potential risk to a certain entity.   
"""


# ----------------------------------------------------------------------
# RISK IDENTIFICATION PROMPT FUNCTIONS
# ----------------------------------------------------------------------

def headline_identification_prompt(batch, config):
    """
    Build an LLM prompt for identifying risk-relevant headlines.

    Args:
        entity_of_concern (str):
            Description of the entity potentially affected by the risk (e.g. 'a logistics firm').
        config (module):
            Configuration module containing 'ENTITY_OF_CONCERN', 'RISK_TYPE', 
            and 'RISK_CONFIDENCE_THRESHOLD'.

    Returns:
        str:
            Formatted prompt string to be sent to the LLM.
    """
    entity_of_concern = config.ENTITY_OF_CONCERN
    risk_type = config.RISK_TYPE
    risk_confidence_threshold = config.RISK_CONFIDENCE_THRESHOLD

    return f"""
    You are a skilled analyst. 
    I'm going to give you multiple article sources separated by index numbers (1., 32., 146. etc). 
    I want you to return the indices of headlines which pose a potential risk to {entity_of_concern} in terms of {risk_type}. 
    Only return indices for those headlines you are at least {risk_confidence_threshold}% sure pose such a risk. 
    Return the exact numbers shown next to the headlines, not relative positions within the headlines batch.
    I want you to return these indices as a python list without explanatory text or comments. 
    If there are no sources that pose such a risk, I want you to return an empty python list. 
        
    These are the article sources: 

    <HEADLINES>
    {batch}
    </HEADLINES>
    """



# ----------------------------------------------------------------------
# RISK SUMMARY PROMPT FUNCTIONS
# ----------------------------------------------------------------------

def story_text_summarization_prompt(today_date, story_text, config):
    """
    Build an LLM prompt for generating a summary of a single story text batch. 

    Args:
        today_date (str):
            Date string used to contextualize summary generation.
        story_text (str):
            Combined raw scraped text for successfully scraped news stories.
        config (module):
            Configuration module containing 'ENTITY_OF_CONCERN' and 'RISK_TYPE'.

    Returns:
        str:
            Formatted prompt string to be sent to the LLM.
    """
    entity_of_concern = config.ENTITY_OF_CONCERN
    risk_type = config.RISK_TYPE

    return f"""
    You are a highly experienced analyst. 
    I'm going to give you raw text scraped from one or more web pages on {today_date}.
    I want you to analyze the text and summarize events which pose a potential risk to {entity_of_concern} in terms of {risk_type}. 
    The text will be thousands of words long, but I want you to return a thoughtful summary of no more than 500 words. 
    Summarise factual developments in English without opinion, bias, sensationalism, inflammatory wording or speculation.
    Only summarize events present in the text - do not invent events or infer facts not explicitly stated.
    Pay attention to the logical consistency of your summary - do not say things that directly contradict one another.
    Return the information in clear report-style paragraphs without bullet points.
    Aim for moderately sized paragraphs, roughly 80-120 words where appropriate.
    Start the summary with a title followed by the first subheading on a new line. 
    Choose 3-5 appropriate subheadings where the content supports it to structure the summary.
    Make the title a Heading level 2 (##) and the subheadings a Heading level 3 (###).
    Split the text within each subheading into multiple paragraphs where necessary for better readability. 
    Group related information together within subheadings. 
    If there are not many relevant developments, include fewer subheadings and make it clear that there's not much relevant news.  
    You should put the subheading that is most important from a {entity_of_concern} perspective first.
    Do not include any other subheadings such as date. 
    Do not make anything in the main part of the text nor the title or subheading bold. 

    This is the raw text of the web pages:

    <RAW WEB TEXT>
    {story_text}
    </RAW WEB TEXT>
    """


def executive_summary_prompt(today_date, combined_summaries, config):
    """
    Build an LLM prompt for generating an executive summary of story text batch summaries. 

    Args:
        today_date (str):
            Date string used to contextualize summary generation.
        combined_summaries (str):
            Single string of LLM-generated summaries for successfully processed story text batches.
        config (module):
            Configuration module containing 'ENTITY_OF_CONCERN' and 'RISK_TYPE'.

    Returns:
        str:
            Formatted prompt string to be sent to the LLM.
    """
    entity_of_concern = config.ENTITY_OF_CONCERN
    risk_type = config.RISK_TYPE

    return f"""
    You are a highly experienced lead analyst. 
    I'm going to give multiple summaries from your team based on news from {today_date}. 
    I want you to produce an executive summary of events which pose a potential risk to {entity_of_concern} in terms of {risk_type}. 
    The combined summaries may be thousands of words long, but I want you to return a thoughtful summary of no more than 200 words. 
    Summarise factual developments in English without opinion, bias, sensationalism, inflammatory wording or speculation.
    Be highly critical and only summarize events present in the text - do not invent events or infer facts not explicitly stated.
    Pay attention to the logical consistency of your summary - do not say things that directly contradict one another.
    Return the information in clear report-style paragraphs without bullet points.
    Aim for moderately sized paragraphs, roughly 50-90 words where appropriate.
    Start the summary with a title followed by the first subheading on a new line. 
    Choose 2-4 appropriate subheadings where the content supports it to structure the summary.
    Make the title a Heading level 2 (##) and the subheadings a Heading level 3 (###).
    Split the text within each subheading into multiple paragraphs where necessary for better readability. 
    Group related information together within subheadings. 
    If there are not many relevant developments, include fewer subheadings and make it clear that there's not much relevant news. 
    I don't want unnecessary fluff.
    You should put the subheading that is most important from a {entity_of_concern} perspective first.
    Do not include any other subheadings such as date. 
    Do not make anything in the main part of the text nor the title or subheading bold.

    This is the combined text of summaries produced by your team so far:

    <COMBINED SUMMARY TEXT>
    {combined_summaries}
    </COMBINED SUMMARY TEXT>
    """