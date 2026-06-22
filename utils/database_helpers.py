"""
Database storage module.

This module manages the SQLite database used to store scraped headlines,
store generated summaries, retrieve existing links, filter duplicates,
and insert processed pipeline data.
"""

import logging
import sqlite3


# ----------------------------------------------------------------------
# LOGGING SETUP
# ----------------------------------------------------------------------

logger = logging.getLogger(__name__)



# ----------------------------------------------------------------------
# INITIALISATION FUNCTIONS
# ----------------------------------------------------------------------

def initialise_database(config):
    """
    Create required database tables if they do not already exist.

    Args:
        config (module):
            Configuration module containing 'DB_PATH'.

    Returns:
        tuple:
            SQLite connection and cursor.
    """
    try:

        connection = sqlite3.connect(config.DB_PATH)
        cursor = connection.cursor()

        cursor.execute("PRAGMA foreign_keys = ON")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS summaries (
                id INTEGER PRIMARY KEY,
                summary_text TEXT,
                date_generated TEXT,  
                topic TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS headlines (
                id INTEGER PRIMARY KEY,
                website TEXT,
                headline TEXT,
                link TEXT UNIQUE,
                story_tag TEXT,
                story_class TEXT,
                summary_id INTEGER,
                FOREIGN KEY (summary_id) REFERENCES summaries(id)
            )
        ''')

        return connection, cursor
    
    except Exception:
        raise



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

    try:
        cursor.execute('SELECT link FROM headlines')
        links = {row[0] for row in cursor}    
    except Exception: 
        raise

    return links


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

    try: 
        deduplicated_df = headlines_df.drop_duplicates(subset='link')
        new_headlines_df = deduplicated_df[~deduplicated_df['link'].isin(existing_links)].copy()

    except Exception:
        raise

    return new_headlines_df



# ----------------------------------------------------------------------
# STORAGE FUNCTIONS
# ----------------------------------------------------------------------

def insert_summary(summary_text, today_date, cursor, config):
    """
    Insert a generated summary into the database.

    Args:
        summary_text (str):
            Final summary text.
        today_date (str):
            Date the summary was generated.
        cursor (sqlite3.Cursor):
            Active SQLite cursor.
        config (module):
            Configuration module containing 'TOPIC_OF_CONCERN'.

    Returns:
        int:
            ID of the inserted summary.
    """
    topic = config.TOPIC_OF_CONCERN

    try:
        cursor.execute('''
            INSERT INTO summaries (
                summary_text, date_generated, topic
            )
            VALUES (?, ?, ?)
        ''',
            (summary_text, today_date, topic)
        )

        summary_id = cursor.lastrowid

    except Exception:
        raise

    return summary_id


def insert_headlines(new_headlines_df, summary_id, cursor):
    """
    Insert headline rows into the database.

    Args:
        new_headlines_df (pandas.DataFrame):
            DataFrame containing processed headline data.
        summary_id (int):
            ID of the summary associated with the headlines.
        cursor (sqlite3.Cursor):
            Active SQLite cursor.
    """

    try:
        df = new_headlines_df.copy()
        df['summary_id'] = summary_id

        rows = df[
            ['website', 'headline', 'link', 'story_tag', 'story_class', 'summary_id']
        ].itertuples(index=False, name=None)

        cursor.executemany('''
            INSERT OR IGNORE INTO headlines (
                website, headline, link, story_tag, story_class, summary_id
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            rows
        )

        inserted_count = cursor.rowcount

    except Exception:
        raise




    