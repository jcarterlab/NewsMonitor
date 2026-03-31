"""
Headline storage module.

This module orchestrates the storage of processed headlines by inserting
them into the database once processing has been completed.
"""

from utils.database_helpers import (
    initialise_database, 
    insert_summary, 
    insert_headlines
)


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
    connection, cursor = initialise_database(config)

    summary_id = insert_summary(final_summary, today_date, cursor, config)

    insert_headlines(new_headlines_df, summary_id, cursor)

    connection.commit()
    connection.close()