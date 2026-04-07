import pytest
import pandas as pd
from newsmonitor.store_data import store_data
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

class TestStoreData:
    def test_stores_summary(self, db_config):
        new_headlines_df = pd.DataFrame({
            'headline': ['headline_1'],
            'link': ['link_1'],
            'story_tag': ['story_tag_1'],
            'story_class': ['story_class_1']
        })

        store_data('final_summary', new_headlines_df, 'today_date', db_config)

        connection, cursor = initialise_database(db_config)
        row = cursor.execute('SELECT summary_text FROM summaries').fetchone()

        assert row[0] == 'final_summary'

        connection.close()

    def test_stores_headlines(self, db_config):
        new_headlines_df = pd.DataFrame({
            'headline': ['headline_1', 'headline_2'],
            'link': ['link_1', 'link_2'],
            'story_tag': ['story_tag_1', 'story_tag_2'],
            'story_class': ['story_class_1', 'story_class_2']
        })

        store_data('final_summary', new_headlines_df, 'today_date', db_config)

        connection, cursor = initialise_database(db_config)
        rows = cursor.execute('SELECT headline, link FROM headlines').fetchall()

        assert len(rows) == 2
        assert set(rows) == {
            ('headline_1', 'link_1'),
            ('headline_2', 'link_2')
        }

        connection.close()

    def test_links_headlines_to_inserted_summary(self, db_config):
        new_headlines_df = pd.DataFrame({
            'headline': ['headline_1'],
            'link': ['link_1'],
            'story_tag': ['tag_1'],
            'story_class': ['class_1']
        })

        store_data('final_summary', new_headlines_df, 'today_date', db_config)

        connection, cursor = initialise_database(db_config)

        summary_row = cursor.execute('SELECT id, summary_text FROM summaries').fetchone()
        headline_row = cursor.execute('SELECT headline, summary_id FROM headlines').fetchone()

        assert summary_row[1] == 'final_summary'
        assert headline_row == ('headline_1', summary_row[0])

        connection.close()

