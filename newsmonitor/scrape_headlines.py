"""
Headline scraping module.

This module retrieves news listing pages, extracts headline text
and article links, and returns the results as a Pandas DataFrame.
"""

import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


# ----------------------------------------------------------------------
# LOGGING SETUP
# ----------------------------------------------------------------------

logger = logging.getLogger(__name__)



# ----------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------

def extract_text(element, page_url):
    """
    Return stripped text for a BeautifulSoup element.

    Args:
        element (bs4.element.Tag):
            BeautifulSoup Tag object from which text should be extracted.
        page_url (str):
            URL of the page to be scraped.

    Returns:
        str | None:
            Stripped element text, or None if element is None or has no text. 
    """
    if element is None:
        return None
    
    try: 
        text = element.get_text(' ', strip=True)
        logger.debug(
            'Extracted headline chars=%d text=%s',
            len(text) if text else 0,
            text[:80] if text else ''
        )
        return ' '.join(text.split()) if text else None
    except Exception:
        logger.error(
            'Failed to extract headline url=%s element=%s',
            page_url,
            repr(element)[:50],
            exc_info=True
        )        
        return None


def extract_link(element, page_url, base_url):
    """
    Extract an absolute URL from a BeautifulSoup element.

    Args:
        element (bs4.element.Tag):
            BeautifulSoup Tag object from which text should be extracted.
        page_url (str):
            URL of the page to be scraped.
        base_url (str):
            Initial part of the URL needed to create a full URL from relative links.

    Returns:
        str | None:
            Absolute URL, or None if not possible to construct.
    """
    if element is None or not base_url:
        return None
    
    try:
        href = element.get('href')
        link = urljoin(base_url, href) if href else None
        logger.debug(
            'Built link base=%s href=%s final=%s', 
            base_url, 
            href, 
            link
        )
        return link 

    except Exception:
        logger.error(
            'Failed to extract link url=%s element=%s',
            page_url,
            repr(element)[:50],
            exc_info=True
        ) 
        return None


def scrape_headline_elements(website, page_url, tag, config):
    """
    Retrieve a webpage and return all elements matching a given tag.

    Args:
        website (str): 
            Website name of the news site.
        page_url (str):
            URL of the page to be scraped.
        tag (str):
            HTML tag used to identify headline elements.
        config (module): 
            Configuration module containing 'REQUEST_TIMEOUT'.

    Returns:
        list[bs4.element.Tag] | None:
            List of BeautifulSoup elements matching the tag.
    """
    try:
        response = requests.get(
            page_url, 
            headers=config.REQUEST_HEADER,
            timeout=config.REQUEST_TIMEOUT
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        elements = soup.find_all(tag)
        logger.debug(
            'Found headline elements source=%s tag=%s found=%d', 
            website, 
            tag, 
            len(elements)
        )
        return elements

    except requests.exceptions.RequestException:
        logger.error(
            'Request failed url=%s tag=%s',
            page_url,
            tag,
            exc_info=True
        ) 
        return None



# ----------------------------------------------------------------------
# ORCHESTRATION FUNCTIONS 
# ----------------------------------------------------------------------

def process_headlines(
        website,
        page_url, 
        tag, 
        base_url, 
        story_tag, 
        story_class, 
        config
    ):
    """
    Create a Pandas DataFrame of headline texts and URLs from a news listing page.

    Args:
        website (str): 
            Website name of the news site.
        page_url (str): 
            URL of the news page to scrape.
        tag (str): 
            Tag name used to select headline elements (e.g., 'a', 'h2').
        base_url (str): 
            Base URL used to resolve relative hrefs.
        story_tag (str): 
            Tag name used later to scrape the news story body text.
        story_class (str): 
            Class name used later to scrape the news story body text.
        config (module): 
            Configuration module containing 'MIN_HEADLINE_LENGTH'.
    
    Returns:
        pd.DataFrame:
            Columns: headline, link, story_tag, story_class
    """
    columns = ['website', 'headline', 'link', 'story_tag', 'story_class']

    elements = scrape_headline_elements(website, page_url, tag, config)

    if elements is None:
        return pd.DataFrame(columns=columns)

    if not elements:
        logger.warning(
            'No headline elements found source=%s page_url=%s tag=%s',
            website,
            page_url,
            tag
        )
        return pd.DataFrame(columns=columns)

    headlines = []
    for el in elements: 
        text = extract_text(el, page_url)
        if not text or len(text) < config.MIN_HEADLINE_LENGTH:
            continue

        link = extract_link(el, page_url, base_url)
        if not link:
            continue

        headlines.append({
            'website': website,
            'headline': text,
            'link': link,
            'story_tag': story_tag,
            'story_class': story_class
        })

    logger.info(
        'Scraped headlines source=%s count=%d', 
        website, 
        len(headlines)
    )

    return pd.DataFrame(headlines, columns=columns)


def scrape_headlines(config):
    """
    Scrape headline text and links for each news source defined in a links CSV.

    Args:
        config (module): 
            Configuration module containing 'LINKS_PATH', 'REQUEST_TIMEOUT' and 
            'MIN_HEADLINE_LENGTH'. 
            
    Returns:
        pd.DataFrame: 
            Combined headlines with columns including headline, link, story_tag and story_class. 
    """
    links_path = config.LINKS_PATH
    try:
        links_df = pd.read_csv(links_path, encoding='utf-8')
    except FileNotFoundError:
        raise RuntimeError(f'File not found: {links_path}')
    
    if links_df.empty:
        raise RuntimeError(f'File is empty: {links_path}')
    
    required_cols = {'website', 'page_url','base_url', 'tag', 'story_tag', 'story_class'}
    missing_cols = required_cols - set(links_df.columns)
    if missing_cols:
        raise RuntimeError(f'File missing required columns: {links_path}; missing={sorted(missing_cols)}')
    
    headlines_dfs = []

    for row in links_df.itertuples(index=False):
        try:
            df = process_headlines(
                row.website,
                row.page_url, 
                row.tag, 
                row.base_url,
                row.story_tag,
                row.story_class,
                config
            )
            if not df.empty:
                headlines_dfs.append(df)

        except Exception:
            logger.error(
                'Failed to process source=%s url=%s tag=%s',
                row.website,
                row.page_url,
                row.tag,
                exc_info=True
            )
            continue
        
    if not headlines_dfs:
        raise RuntimeError('No headlines dataframes were created')

    headlines_df = pd.concat(headlines_dfs, ignore_index=True)

    if headlines_df.empty:
        raise RuntimeError('No headlines were extracted from any source')

    logger.info(
        'Finished scraping headlines source_count=%s headlines_count=%d', 
        len(links_df), 
        len(headlines_df)
    )

    return headlines_df
