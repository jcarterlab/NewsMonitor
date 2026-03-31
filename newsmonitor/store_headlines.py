"""
Headline storage module.

This module orchestrates the storage of processed headlines by inserting
them into the database once processing has been completed.
"""

from utils.database import initialise_database, insert_headlines


# ----------------------------------------------------------------------
# ORCHESTRATION FUNCTIONS 
# ----------------------------------------------------------------------

def store_headlines(new_headlines_df, config):
    """
    Store new headlines in the database once processed. 

    Args:
        new_headlines_df (pandas.DataFrame):
            DataFrame containing processed new headlines.
        config (module):
            Configuration module containing 'DB_PATH'.
    """
    connection, cursor = initialise_database(config)

    insert_headlines(new_headlines_df, cursor)

    connection.commit()
    connection.close()