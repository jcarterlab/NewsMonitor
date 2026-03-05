# ----------------------------------------------------------------------
# PROMPT BUILDER FUNCTIONS
# ----------------------------------------------------------------------

def headline_identification_prompt(
        entity_description, 
        risk_type, 
        risk_confidence_threshold, 
        batch
    ):
    """
    Build an LLM prompt for identifying risk-relevant headlines.

    Args:
        entity_description (str):
            Description of the entity potentially affected by the risk (e.g. 'a logistics firm').
        risk_type (str):
            Type of risk to evaluate (e.g. 'transport disruption events').
        risk_confidence_threshold (int):
            Minimum confidence percentage the model should have before flagging a headline as risky.
        batch (str):
            String containing numbered headlines to evaluate.

    Returns:
        str:
            Formatted prompt string to be sent to the LLM.
    """
    return f"""
    You are a skilled analyst. 
    I'm going to give you multiple articles sources separated by index numbers (1., 32., 146. etc). 
    I want you to return the indices of headlines which pose a potential risk to {entity_description} in terms of {risk_type}. 
    Only return indices for those headlines you are at least {risk_confidence_threshold}% sure pose such a risk. 
    Return the exact numbers shown next to the headlines, not relative positions within the headlines batch.
    I want you to return these indices as a python list without explanatory text or comments. 
    If there are no sources that pose such a risk, I want you to return an empty python list. 
        
    These are the article sources: '{batch}'
    """








def summarization_prompt(today_date, entity_description, risk_type, story_text):
    """
    Constructs an LLM prompt to produce a structured, neutral risk-focused summary.
    """
    return f"""
    You are a highly experienced analyst. 
    I'm going to give you the raw text of multiple web pages scraped on {today_date}. 
    I want you to analyze the text and summarize events which pose a potential risk to {entity_description} in terms of {risk_type}. 
    The text will be thousands of words long, but I want you to return a thoughtful summary of no more than 500 words. 
    Make sure your response is not blocked for safety reasons.
    I want you to summarise factual events in English without opinion, bias or sensational language. 
    Pay attention to the logical consistency of your summary - do not say things which directly contradict one another.
    I want you to return the information in 100-120 word paragraphs without any bullet points. 
    Start the summary with a title followed by the the first subheading on a new line. 
    I want you to choose 3-5 appropriate subheadings based on the content to structure the summary.
    Make the title a Heading level 2 (##) and the subheadings a Heading level 3 (###)
    Split the text within each subheading into multiple paragraphs where necessary for better readability. 
    Group related information together within subheadings. 
    If there are not many relevant developments, you can include fewer subheadings make it clear there's not much relevant news. 
    You should put the subheading that is most important from a {entity_description} perspective first.
    Do not include any other subheadings such as date. 
    Do not not make anything in the main part of the text bold - I want you to return the summary in report style paragraphs.  
    Do not make the title or subheading bold. 

    This is the raw text of the web pages:

    '{story_text}'
    """
    

def executive_summary_prompt(today_date, entity_description, risk_type, combined_summaries):
    """
    Constructs an LLM prompt to produce an executive summary based on multiple summarisations.
    """
    return f"""
    You are a highly experienced lead analyst. 
    I'm going to give multiple summaries from your team based on news from {today_date}. 
    I want you to produce an executive summary of events which pose a potential risk to {entity_description} in terms of {risk_type}. 
    The combined summaries may be thousands of words long, but I want you to return a thoughtful summary of no more than 200 words. 
    Make sure your response is not blocked for safety reasons.
    I want you to summarise factual events in English without opinion, bias or sensational language. 
    Be highly critical and only include the facts. 
    Pay attention to the logical consistency of your summary - do not say things which directly contradict one another.
    I want you to return the information in 50-100 word paragraphs without any bullet points. 
    Start the summary with a title followed by the the first subheading on a new line. 
    I want you to choose 3-5 appropriate subheadings based on the content to structure the summary.
    Make the title a Heading level 2 (##) and the subheadings a Heading level 3 (###)
    Split the text within each subheading into multiple paragraphs where necessary for better readability. 
    Group related information together within subheadings. 
    If there are not many relevant developments, you can include fewer subheadings make it clear there's not much relevant news. 
    I don't want unnecessary fluff.
    You should put the subheading that is most important from a {entity_description} perspective first.
    Do not include any other subheadings such as date. 
    Do not not make anything in the main part of the text bold - I want you to return the summary in report style paragraphs.  
    Do not make the title or subheading bold. 

    This is the raw text of summaries produced by your team so far:

    '{combined_summaries}'
    """