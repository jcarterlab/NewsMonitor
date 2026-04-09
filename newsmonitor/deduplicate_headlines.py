"""
Risk headline deduplication module.

This module orchestrates the deduplication of risk headlines by keeping 
only those whose links are not already stored in the database.
"""

import logging
from utils.database_helpers import (
    initialise_database, 
    get_existing_links, 
    filter_new_headlines
)


# ----------------------------------------------------------------------
# LOGGING SETUP
# ----------------------------------------------------------------------

logger = logging.getLogger(__name__)



# ----------------------------------------------------------------------
# DEDUPLICATION FUNCTIONS 
# ----------------------------------------------------------------------

def deduplicate_headlines(headlines_df, config):
    """
    Remove duplicate headlines based on previously stored links.

    Args:
        headlines_df (pandas.DataFrame):
            DataFrame containing scraped headlines.
        config (module):
            Configuration module containing 'DB_PATH'.

    Returns:
        pandas.DataFrame:
            DataFrame containing only new headlines.
    """
    connection, cursor = initialise_database(config)

    existing_links = get_existing_links(cursor)
    new_headlines_df = filter_new_headlines(headlines_df, existing_links)

    connection.commit()
    connection.close()

    logger.info('Deduplicated headlines count=%d', len(new_headlines_df))

    return new_headlines_df