"""
News story scraping module.

This module retrieves and extracts article text from news story 
URLs that have been identified as target stories by an LLM.
"""

import logging
import requests
from bs4 import BeautifulSoup
import re


# ----------------------------------------------------------------------
# LOGGING SETUP
# ----------------------------------------------------------------------

logger = logging.getLogger(__name__)



# ----------------------------------------------------------------------
# REGEX PATTERNS
# ----------------------------------------------------------------------

WHITESPACE_RE = re.compile(r'[\s\xa0]+')
ZERO_WIDTH_RE = re.compile(r'[\u200b\u200c\u200d\ufeff]')
JUNK_PHRASES_RE = re.compile(
    r'(THIS WEEK ONLY: Save|REGISTER NOW)',
    re.IGNORECASE
)



# ----------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------

def extract_story_text(elements, website, story_url):
    """
    Extract text from BeautifulSoup Tag objects representing a news story.

    Args:
        elements (list[bs4.element.Tag]):
            List of BeautifulSoup Tag objects from which text should be extracted.
    website (str): 
            Website name of the news site.
        story_url (str):
            URL of the news story.

    Returns:
        str | None:
            Text from a news story joined as a single string. 
    """
    if not elements:
        return None

    seen = set()
    paragraphs = []

    for el in elements:
        text = el.get_text(separator=' ', strip=True)
        text = ZERO_WIDTH_RE.sub('', text)
        text = WHITESPACE_RE.sub(' ', text)

        if not text:
            continue

        if text in seen:
            continue

        if len(text) < 30 or JUNK_PHRASES_RE.search(text):
            continue

        seen.add(text)
        paragraphs.append(text)

    if not paragraphs:
        return None

    return ' '.join(paragraphs) 


def scrape_story_elements(website, story_url, story_tag, story_class, config):
    """
    Fetch all matching HTML elements from a news story page.

    Args:
        website (str): 
            Website name of the news site.
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
        response = requests.get(
            story_url, 
            headers=config.REQUEST_HEADER,
            timeout=config.REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        return []

    soup = BeautifulSoup(response.content, "html.parser")

    if story_class:
        elements = soup.find_all(story_tag, class_=story_class)
    else:
        elements = soup.find_all(story_tag)

    if not elements:
        logger.warning(
            'No elements found website=%s url=%s tag=%s class=%s',
            website,
            story_url,
            story_tag,
            story_class
        )

    return elements 



# ----------------------------------------------------------------------
# ORCHESTRATION FUNCTIONS 
# ----------------------------------------------------------------------

def scrape_stories(target_headlines_df, config):
    """
    Scrape text for successfully extracted news stories in the dataframe.

    Args:
        target_headlines_df (pd.DataFrame):
            DataFrame containing story URLs and scraping selectors.
        config:
            Configuration module containing 'REQUEST_TIMEOUT'.

    Returns:
        list[str]:
            List of raw scraped texts for successfully scraped news stories.
    """
    total_stories = len(target_headlines_df)
    story_texts = []
    total_words = 0

    for row in target_headlines_df.itertuples():
        website = row.website
        story_url = row.link
        story_tag = row.story_tag
        story_class = row.story_class

        elements = scrape_story_elements(
            website, 
            story_url, 
            story_tag, 
            story_class, 
            config
        )

        if not elements:
            continue

        story_text = extract_story_text(elements, website, story_url)
        if not story_text:
            continue

        story_texts.append(story_text)
        total_words += len(story_text.split())

    return story_texts