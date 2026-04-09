import pytest
import pandas as pd
from newsmonitor.deduplicate_headlines import deduplicate_headlines
from utils.database_helpers import (
    initialise_database, 
    insert_summary,
    insert_headlines
)


# ----------------------------------------------------------------------
# FIXTURES 
# ----------------------------------------------------------------------

@pytest.fixture
def db_config(tmp_path):
    class DummyConfig:
        DB_PATH = str(tmp_path / 'test_news_data.db')
        TOPIC_OF_CONCERN = 'risk A'
    return DummyConfig



# ----------------------------------------------------------------------
# TESTS 
# ----------------------------------------------------------------------

class TestDeduplicateHeadlines:
    def test_deduplicates_headlines(self, db_config):
        connection, cursor = initialise_database(db_config)

        summary_id = insert_summary('summary_text', 'today_date', cursor, db_config)

        old_headlines_df = pd.DataFrame({
            'website': ['website_1'],
            'headline': ['headline_1'],
            'link': ['link_1'],
            'story_tag': ['story_tag_1'],
            'story_class': ['story_class_1'],
            'summary_id': [summary_id]
        })

        insert_headlines(old_headlines_df, summary_id, cursor)

        connection.commit()
        connection.close()

        headlines_df = pd.DataFrame({
            'website': ['website_1', 'website_2'],
            'headline': ['headline_1', 'headline_2'],
            'link': ['link_1', 'link_2'],
            'story_tag': ['story_tag_1', 'story_tag_2'],
            'story_class': ['story_class_1', 'story_class_2']
        })

        deduplicated_headlines_df = deduplicate_headlines(headlines_df, db_config)

        assert len(deduplicated_headlines_df) == 1
        assert list(deduplicated_headlines_df[['headline', 'link']].iloc[0]) == ['headline_2', 'link_2']

    def test_returns_all_headlines_when_database_is_empty(self, db_config):
        headlines_df = pd.DataFrame({
            'website': ['website_1', 'website_2'],
            'headline': ['headline_1', 'headline_2'],
            'link': ['link_1', 'link_2'],
            'story_tag': ['story_tag_1', 'story_tag_2'],
            'story_class': ['story_class_1', 'story_class_2']
        })

        deduplicated_headlines_df = deduplicate_headlines(headlines_df, db_config)

        assert list(deduplicated_headlines_df['headline']) == ['headline_1', 'headline_2']

    def test_removes_duplicate_links_within_input_dataframe(self, db_config):
        headlines_df = pd.DataFrame({
            'website': ['website_1', 'website_1', 'website_2'],
            'headline': ['headline_1', 'headline_1', 'headline_2'],
            'link': ['link_1', 'link_1', 'link_2'],
            'story_tag': ['story_tag_1', 'story_tag_1', 'story_tag_2'],
            'story_class': ['story_class_1', 'story_class_1', 'story_class_2']
        })

        deduplicated_headlines_df = deduplicate_headlines(headlines_df, db_config)

        assert list(deduplicated_headlines_df['headline']) == ['headline_1', 'headline_2']

