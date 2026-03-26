"""
Database storage module.

This module manages the SQLite database used to store scraped headlines,
retrieve existing links, filter duplicates and insert new headline data.
"""

import sqlite3


# ----------------------------------------------------------------------
# INITIALISATION FUNCTIONS
# ----------------------------------------------------------------------

def initialise_database(config):
    """
    Create the headlines table if it does not already exist.

    Args:
        config (module):
            Configuration module containing 'DB_PATH'.

    Returns:
        tuple:
            SQLite connection and cursor.
    """
    connection = sqlite3.connect(config.DB_PATH)
    cursor = connection.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS headlines (
            headline TEXT,
            link TEXT UNIQUE,
            story_tag TEXT,
            story_class TEXT
        )
    ''')

    return connection, cursor



# ----------------------------------------------------------------------
# DEDUPLICATION FUNCTIONS
# ----------------------------------------------------------------------

def get_existing_links(cursor):
    """
    Return links already stored in the headlines table.

    Args:
        cursor (sqlite3.Cursor):
            Active SQLite cursor.

    Returns:
        set:
            Set of existing headline links.
    """
    cursor.execute('''
        SELECT link FROM headlines
    ''')
    return {row[0] for row in cursor}


def filter_new_headlines(headlines_df, existing_links):
    """
    Filter out headlines whose links already exist in the database.

    Args:
        headlines_df (pandas.DataFrame):
            DataFrame containing scraped headlines.
        existing_links (set):
            Set of links already stored in the database.

    Returns:
        pandas.DataFrame:
            DataFrame containing only new headlines.
    """
    headlines_df = headlines_df.drop_duplicates(subset='link')

    return headlines_df[~headlines_df['link'].isin(existing_links)].copy()



# ----------------------------------------------------------------------
# STORAGE FUNCTIONS
# ----------------------------------------------------------------------

def insert_headlines(new_headlines_df, cursor):
    """
    Insert headline rows into the database.

    Args:
        new_headlines_df (pandas.DataFrame):
            DataFrame containing headline data.
        cursor (sqlite3.Cursor):
            Active SQLite cursor.
    """
    rows = new_headlines_df[
        ['headline', 'link', 'story_tag', 'story_class']
    ].itertuples(index=False, name=None)

    cursor.executemany('''
        INSERT OR IGNORE INTO headlines (
            headline, link, story_tag, story_class
        )
        VALUES (?, ?, ?, ?)
        ''',
        rows
    )