"""
Risk headline deduplication module.

This module orchestrates the deduplication of risk headlines by keeping 
only those whose links are not already stored in the database.
"""

from utils.database_helpers import (
    initialise_database, 
    get_existing_links, 
    filter_new_headlines
)


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

    print(f'Total new headlines: {len(new_headlines_df)}\n')

    connection.commit()
    connection.close()

    return new_headlines_df