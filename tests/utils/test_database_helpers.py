import pytest
import sqlite3
import pandas as pd
from utils.database_helpers import (
    initialise_database, 
    get_existing_links,
    filter_new_headlines,
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
        RISK_TYPE = 'risk A'
    return DummyConfig



# ----------------------------------------------------------------------
# TESTS 
# ----------------------------------------------------------------------

class TestInitialiseDatabase:
    def test_creates_headlines_table(self, db_config):
        connection, cursor = initialise_database(db_config)
        cursor.execute('''
            SELECT name FROM sqlite_master 
                WHERE type='table' 
                AND name='headlines'
        ''')
        result = cursor.fetchone()

        assert isinstance(connection, sqlite3.Connection)
        assert isinstance(cursor, sqlite3.Cursor)
        assert result == ('headlines',)

        connection.close()

    def test_correct_headlines_schema(self, db_config):
        connection, cursor = initialise_database(db_config)
        cursor.execute('''
            PRAGMA table_info(headlines)
        ''')
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]

        assert column_names == [
            'id', 'headline', 'link', 'story_tag', 'story_class', 'summary_id'
            ]

        connection.close()

    def test_link_has_unique_constraint(self, db_config):
        connection, cursor = initialise_database(db_config)
        cursor.execute('''
            INSERT OR IGNORE INTO headlines (
                headline, link, story_tag, story_class
            )
            VALUES ('A', 'link1', 'p', 'main-text')
        ''')
        cursor.execute('''
            INSERT OR IGNORE INTO headlines (
                headline, link, story_tag, story_class
            )
            VALUES ('B', 'link1', 'p', 'paragraph')
        ''')
        rows = cursor.execute("SELECT * FROM headlines").fetchall()

        assert len(rows) == 1

        connection.close()


class TestGetExistingLinks:
    def test_single_link(self, db_config):
        connection, cursor = initialise_database(db_config)
        cursor.execute('''
            INSERT OR IGNORE INTO headlines (
                headline, link, story_tag, story_class
            )
            VALUES (?, ?, ?, ?)
            ''', 
            ('A', 'link1', 'p', 'text')
        )
        connection.commit()

        assert get_existing_links(cursor) == {'link1'}

        connection.close()

    def test_multiple_links(self, db_config):
        connection, cursor = initialise_database(db_config)
        cursor.executemany('''
            INSERT OR IGNORE INTO headlines (
                headline, link, story_tag, story_class
            )
            VALUES (?, ?, ?, ?)
            ''',
            [('A', 'link1', 'p', 'text'), ('B', 'link2', 'p', 'paragraph')]
        )
        connection.commit()

        assert get_existing_links(cursor) == {'link1', 'link2'}

        connection.close()

    def test_returns_empty_set_for_empty_table(self, db_config):
        connection, cursor = initialise_database(db_config)

        assert get_existing_links(cursor) == set()

        connection.close()

    def test_returns_only_unique_links(self, db_config):
        connection, cursor = initialise_database(db_config)
        cursor.executemany('''
            INSERT OR IGNORE INTO headlines (
                headline, link, story_tag, story_class
            )
            VALUES (?, ?, ?, ?)
            ''',
            [('A', 'link1', 'p', 'text'), ('B', 'link1', 'p', 'paragraph')]
        )
        connection.commit()

        assert get_existing_links(cursor) == {'link1'}

        connection.close()


class TestFilterNewHeadlines:
    def test_filters_existing_links(self):
        df = pd.DataFrame({
            'headline': ['A', 'B', 'C'],
            'link': ['link1', 'link2', 'link3']
        })
        existing_links = {'link1'}
        result = filter_new_headlines(df, existing_links)

        assert set(result['link']) == {'link2', 'link3'}

    def test_removes_duplicates_before_filtering(self):
        df = pd.DataFrame({
            'headline': ['A', 'A duplicate'],
            'link': ['link1', 'link1']
        })
        result = filter_new_headlines(df, set())

        assert len(result) == 1 

    def test_returns_empty_if_all_links_already_exist(self):
        df = pd.DataFrame({
            'headline': ['A', 'B'],
            'link': ['link1', 'link2']
        })
        existing_links = {'link1', 'link2'}
        result = filter_new_headlines(df, existing_links)

        assert result.empty

    def test_returns_copy_not_view(self):
        df = pd.DataFrame({
            'headline': ['A'],
            'link': ['link1']
        })
        result = filter_new_headlines(df, set())
        result.loc[0, 'headline'] = 'Changed'

        assert df.loc[0, 'headline'] == 'A'


class TestInsertHeadlines:
    def test_inserts_single_row(self, db_config):
        connection, cursor = initialise_database(db_config)
        insert_summary('summary_text', 'today_date', cursor, db_config)
        df = pd.DataFrame({
            'headline': ['A'],
            'link': ['link1'],
            'story_tag': ['p'],
            'story_class': ['text']
        })
        insert_headlines(df, 1, cursor)
        connection.commit()
        rows = cursor.execute('SELECT * FROM headlines').fetchall()

        assert rows == [(1, 'A', 'link1', 'p', 'text', 1)]

        connection.close()

    def test_inserts_multiple_rows(self, db_config):
        connection, cursor = initialise_database(db_config)
        insert_summary('summary_text', 'today_date', cursor, db_config)
        df = pd.DataFrame({
            'headline': ['A', 'B'],
            'link': ['link1', 'link2'],
            'story_tag': ['p', 'p'],
            'story_class': ['text', 'paragraph']
        })
        insert_headlines(df, 1, cursor)
        connection.commit()
        rows = cursor.execute('SELECT * FROM headlines').fetchall()

        assert rows == [
            (1, 'A', 'link1', 'p', 'text', 1),
            (2, 'B', 'link2', 'p', 'paragraph', 1)
        ]

        connection.close()

    def test_drops_duplicate_links(self, db_config):
        connection, cursor = initialise_database(db_config)
        insert_summary('summary_text', 'today_date', cursor, db_config)
        df = pd.DataFrame({
            'headline': ['A', 'A'],
            'link': ['link1', 'link1'],
            'story_tag': ['p', 'p'],
            'story_class': ['paragraph', 'paragraph']
        })
        insert_headlines(df, 1, cursor)
        connection.commit()
        rows = cursor.execute('SELECT * FROM headlines').fetchall()

        assert rows == [(1, 'A', 'link1', 'p', 'paragraph', 1)]

        connection.close()

    def test_empty_dataframe_inserts_nothing(self, db_config):
        connection, cursor = initialise_database(db_config)
        insert_summary('summary_text', 'today_date', cursor, db_config)
        df = pd.DataFrame(columns=[
            'headline', 'link', 'story_tag', 'story_class'
        ])
        insert_headlines(df, 1, cursor)
        connection.commit()
        rows = cursor.execute('SELECT * FROM headlines').fetchall()

        assert rows == []

        connection.close()

    def test_ignores_extra_dataframe_columns(self, db_config):
        connection, cursor = initialise_database(db_config)
        insert_summary('summary_text', 'today_date', cursor, db_config)
        df = pd.DataFrame({
            'headline': ['A'],
            'link': ['link1'],
            'story_tag': ['p'],
            'story_class': ['text'],
            'extra_column': ['to be ignored']
        })
        insert_headlines(df, 1, cursor)
        connection.commit()
        rows = cursor.execute('SELECT * FROM headlines').fetchall()

        assert rows == [(1, 'A', 'link1', 'p', 'text', 1)]

        connection.close()


    def test_raises_keyerror_if_required_column_missing(self, db_config):
        connection, cursor = initialise_database(db_config)
        insert_summary('summary_text', 'today_date', cursor, db_config)
        df = pd.DataFrame({
            'headline': ['A'],
            'link': ['link1'],
            'story_tag': ['p']
        })
        
        with pytest.raises(KeyError):
            insert_headlines(df, 1, cursor)

        connection.close()