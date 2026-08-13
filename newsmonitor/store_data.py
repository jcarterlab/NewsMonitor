"""
Headline storage module.

This module orchestrates the storage of processed headlines by inserting
them into the database once processing has been completed.
"""

import logging
from utils.database_helpers import (
    initialise_database, 
    insert_summary, 
    insert_headlines
)


# ----------------------------------------------------------------------
# LOGGING SETUP
# ----------------------------------------------------------------------

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# STORAGE FUNCTIONS 
# ----------------------------------------------------------------------

def store_data(final_summary, new_headlines_df, today_date, config):
    """
    Store a generated summary and its associated headlines in the database.

    Args:
        final_summary (str):
            Final generated summary text.
        new_headlines_df (pandas.DataFrame):
            DataFrame containing processed new headlines.
        today_date (str):
            Date the summary was generated.
        config (module):
            Configuration module containing 'DB_PATH'.
    """
    connection = None

    try:
        connection, cursor = initialise_database(config)

        summary_id = insert_summary(final_summary, today_date, cursor, config)
        insert_headlines(new_headlines_df, summary_id, cursor)

        connection.commit()

    except Exception:
        if connection is not None:
            connection.rollback()
            
        raise

    finally:
        if connection is not None:
            connection.close()