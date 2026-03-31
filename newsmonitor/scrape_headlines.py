"""
Headline scraping module.

This module retrieves news listing pages, extracts headline text
and article links, and returns the results as a Pandas DataFrame.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import traceback


# ----------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------

def extract_text(element):
    """
    Return stripped text for a BeautifulSoup element.

    Args:
        element (bs4.element.Tag):
            BeautifulSoup Tag object from which text should be extracted.

    Returns:
        str | None:
            Stripped element text, or None if element is None or has no text. 
    """
    if element is None:
        return None
    
    try: 
        text = element.get_text(' ', strip=True)
        return ' '.join(text.split()) if text else None
    except Exception as e:
        print(
            f'Error: extract_text failed\n'
            f'    element={repr(element)[:200]}\n'
            f'    {type(e).__name__}: {e}'
        )
        return None


def extract_link(element, base_url):
    """
    Extract an absolute URL from a BeautifulSoup element.

    Args:
        element (bs4.element.Tag):
            BeautifulSoup Tag object from which text should be extracted.
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
        return urljoin(base_url, href) if href else None

    except Exception as e:
        print(
            f'Error: extract_link failed\n'
            f'    element={repr(element)[:200]}\n'
            f'    {type(e).__name__}: {e}'
        )
        return None



# ----------------------------------------------------------------------
# SCRAPING FUNCTIONS
# ----------------------------------------------------------------------

def scrape_headline_elements(page_url, tag, config):
    """
    Retrieve a webpage and return all elements matching a given tag.

    Args:
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
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; RiskPipelineBot/1.0)'
    }

    try:
        response = requests.get(
            page_url, 
            headers=headers, 
            timeout=config.REQUEST_TIMEOUT
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        return soup.find_all(tag)

    except requests.exceptions.RequestException as e:
        print(
            'Error: request failed\n'
            f'    page_url={page_url}\n'
            f'    tag={tag}\n'
            f'    {type(e).__name__}: {e}'
        )
        return None
    

def build_headlines_dataframe(
        page_url, 
        tag, 
        base_url, 
        story_tag, 
        story_class, 
        config
    ):
    """
    Build a Pandas DataFrame of headline texts and URLs from a news listing page.

    Args:
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
    columns = ['headline', 'link', 'story_tag', 'story_class']

    elements = scrape_headline_elements(page_url, tag, config)

    if elements is None:
        return pd.DataFrame(columns=columns)

    if not elements:
        print(
            'Warning: no elements found\n'
            f'    page_url={page_url}\n'
            f'    tag={tag}'
        )
        return pd.DataFrame(columns=columns)

    headlines = []
    for el in elements: 
        text = extract_text(el)
        if not text or len(text) < config.MIN_HEADLINE_LENGTH:
            continue

        link = extract_link(el, base_url)
        if not link:
            continue

        headlines.append({
            'headline': text,
            'link': link,
            'story_tag': story_tag,
            'story_class': story_class
        })

    print(
        f'Headlines returned: {len(headlines)} | '
        f'elements_found: {len(elements)}'
    )

    return pd.DataFrame(headlines, columns=columns)



# ----------------------------------------------------------------------
# ORCHESTRATION FUNCTIONS 
# ----------------------------------------------------------------------

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
        raise RuntimeError(f'{links_path} not found')
    
    if links_df.empty:
        raise RuntimeError(f'{links_path} is empty')
    
    required_cols = {'page_url', 'tag', 'base_url', 'story_tag', 'story_class'}
    missing_cols = required_cols - set(links_df.columns)
    if missing_cols:
        raise RuntimeError(f'{links_path} missing required columns: {sorted(missing_cols)}')

    print(f'\nLinks to be scraped: {len(links_df)}\n')

    headlines_dfs = []

    for row in links_df.itertuples(index=False):
        try:
            df = build_headlines_dataframe(
                row.page_url, 
                row.tag, 
                row.base_url,
                row.story_tag,
                row.story_class,
                config
            )
            if not df.empty:
                headlines_dfs.append(df)

        except Exception as e:
            print(f'Error: failed processing {row.page_url}: {e}')
            traceback.print_exc()
            continue
        
    if not headlines_dfs:
        raise RuntimeError('no headlines dataframes were created')

    headlines_df = pd.concat(headlines_dfs, ignore_index=True)

    if headlines_df.empty:
        raise RuntimeError('No headlines were extracted from any source')
    
    print(f'\nTotal headlines: {len(headlines_df)}\n')

    return headlines_df
