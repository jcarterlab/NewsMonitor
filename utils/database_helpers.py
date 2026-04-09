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
        logger.info('Initialising database path=%s', config.DB_PATH)

        connection = sqlite3.connect(config.DB_PATH)
        cursor = connection.cursor()

        cursor.execute("PRAGMA foreign_keys = ON")

        logger.debug('Ensuring "summaries" table exists')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS summaries (
                id INTEGER PRIMARY KEY,
                summary_text TEXT,
                date_generated TEXT,  
                topic TEXT
            )
        ''')

        logger.debug('Ensuring "headlines" table exists')
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

        logger.info('Database initialised successfully path=%s', config.DB_PATH)

        return connection, cursor
    
    except Exception:
        logger.error(
            'Failed to initialise database path=%s',
            config.DB_PATH,
            exc_info=True
        )
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
    logger.debug('Fetching existing links from database')

    try:
        cursor.execute('SELECT link FROM headlines')
        links = {row[0] for row in cursor}    
    except Exception: 
        logger.error(
            'Failed to fetch existing links from headlines table', 
            exc_info=True
        )
        raise

    logger.debug(
        'Retrieved existing links count=%d', 
        len(links)
    )
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
    logger.debug(
        'Filtering new headlines initial_count=%d existing_links=%d',
        len(headlines_df),
        len(existing_links)
    )

    try: 
        deduplicated_df = headlines_df.drop_duplicates(subset='link')
        new_headlines_df = deduplicated_df[~deduplicated_df['link'].isin(existing_links)].copy()

    except Exception:
        logger.error(
            'Failed to filter new headlines',
            exc_info=True
        )
        raise

    logger.debug(
        'Filtered new headlines new_count=%d removed_duplicates=%d removed_existing=%d',
        len(new_headlines_df),
        len(headlines_df) - len(deduplicated_df),
        len(deduplicated_df) - len(new_headlines_df)
    )

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

    logger.debug(
        'Inserting summary date=%s topic=%s word_count=%d',
        today_date,
        topic,
        len(summary_text.split())
    )

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
        logger.error(
            'Failed to insert summary date=%s topic=%s',
            today_date,
            topic,
            exc_info=True
        )
        raise

    logger.info(
        'Inserted summary summary_id=%d date=%s topic=%s',
        summary_id,
        today_date,
        topic
    )

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
    logger.debug(
        'Inserting headlines summary_id=%d input_count=%d',
        summary_id,
        len(new_headlines_df)
    )

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
        logger.error(
            'Failed to insert headlines summary_id=%d',
            summary_id,
            exc_info=True
        )
        raise

    logger.info(
        'Inserted headlines summary_id=%d inserted_count=%d attempted=%d ignored=%d',
        summary_id,
        inserted_count,
        len(new_headlines_df),
        len(new_headlines_df) - inserted_count
    )




    