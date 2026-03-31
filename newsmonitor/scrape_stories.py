"""
News story scraping module.

This module retrieves and extracts article text from news story 
URLs that have been identified as representing a potential risk.
"""

import requests
from bs4 import BeautifulSoup
import re


# ----------------------------------------------------------------------
# REGEX PATTERNS
# ----------------------------------------------------------------------

WHITESPACE_RE = re.compile(r'[\s\xa0]+')
ZERO_WIDTH_RE = re.compile(r'[\u200b\u200c\u200d\ufeff]')



# ----------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------

def extract_story_text(elements, story_url):
    """
    Extract text from BeautifulSoup Tag objects representing a news story.

    Args:
        elements (list[bs4.element.Tag]):
            List of BeautifulSoup Tag objects from which text should be extracted.
        story_url (str):
            URL of the news story.

    Returns:
        str | None:
            Text from a news story joined as a single string. 
    """
    if not elements:
        print('Warning: no usable text found:\n')
        print(f'    url={story_url}')
        return None

    seen = set()
    paragraphs = []

    for el in elements:
        text = el.get_text(separator=' ', strip=True)
        text = ZERO_WIDTH_RE.sub('', text)
        text = WHITESPACE_RE.sub(' ', text)

        if not text or text in seen:
            continue

        seen.add(text)
        paragraphs.append(text)

    if not paragraphs:
        print('Warning: no usable text found:\n')
        print(f'    url={story_url}')
        return None

    print(f'Unique paragraphs scraped: {len(paragraphs)}')
    return ' '.join(paragraphs) 



# ----------------------------------------------------------------------
# SCRAPING FUNCTIONS
# ----------------------------------------------------------------------

def scrape_story_elements(story_url, story_tag, story_class, config):
    """
    Fetch all matching HTML elements from a news story page.

    Args:
        story_url (str):
            URL of the news story.
        story_tag (str):
            HTML tag to search for.
        story_class (str | None):
            CSS class to filter by. If None, returns all matching tags.
        config:
            Configuration module containing 'REQUEST_TIMEOUT'.

    Returns:
        list:
            BeautifulSoup elements or an empty list if the request 
            fails or no matching elements are found.
    """
    try:
        response = requests.get(story_url, timeout=config.REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print('Error: unable to scrape story:\n')
        print(f'    url={story_url}')
        print(f'    {type(e).__name__}: {e}')
        return []

    soup = BeautifulSoup(response.content, "html.parser")

    if story_class:
        elements = soup.find_all(story_tag, class_=story_class)
    else:
        elements = soup.find_all(story_tag)

    if not elements:
        print(f'Warning: no elements found for {story_url}')

    return elements 



# ----------------------------------------------------------------------
# ORCHESTRATION FUNCTIONS 
# ----------------------------------------------------------------------

def scrape_stories(risk_headlines_df, config):
    """
    Scrape text for successfully extracted news stories in the dataframe.

    Args:
        risk_headlines_df (pd.DataFrame):
            DataFrame containing story URLs and scraping selectors.
        config:
            Configuration module containing 'REQUEST_TIMEOUT'.

    Returns:
        list[str]:
            List of raw scraped texts for successfully scraped news stories.
    """
    story_texts = []
    total_words = 0

    for row in risk_headlines_df.itertuples():
        story_url = row.link
        story_tag = row.story_tag
        story_class = row.story_class

        elements = scrape_story_elements(story_url, story_tag, story_class, config)

        if not elements:
            continue

        story_text = extract_story_text(elements, story_url)
        if not story_text:
            continue

        story_texts.append(story_text)
        total_words += len(story_text.split())

    print(f'\nTotal words: {total_words}\n')

    return story_texts