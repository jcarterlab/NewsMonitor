import pytest
import sqlite3
import pandas as pd
from utils.database import initialise_database, get_existing_links


# ----------------------------------------------------------------------
# FIXTURES 
# ----------------------------------------------------------------------

@pytest.fixture
def db_config(tmp_path):
    class DummyConfig:
        DB_PATH = str(tmp_path / 'test_processed_headlines.db')
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

        assert column_names == ['headline', 'link', 'story_tag', 'story_class']

        connection.close()

    def test_link_has_unique_constraint(self, db_config):
        connection, cursor = initialise_database(db_config)
        cursor.execute('''
            INSERT OR IGNORE INTO headlines (
                headline, link, story_tag, story_class
            )
            VALUES ('A', 'same_link', 'tag', 'class')
        ''')
        cursor.execute('''
            INSERT OR IGNORE INTO headlines (
                headline, link, story_tag, story_class
            )
            VALUES ('B', 'same_link', 'tag', 'class')
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
            ('One', 'https://www.one.com', 'p', 'text')
        )
        connection.commit()

        assert get_existing_links(cursor) == {'https://www.one.com'}

        connection.close()

    def test_multiple_links(self, db_config):
        connection, cursor = initialise_database(db_config)
        cursor.executemany('''
            INSERT OR IGNORE INTO headlines (
                headline, link, story_tag, story_class
            )
            VALUES (?, ?, ?, ?)
            ''',
            [('One', 'https://www.one.com', 'p', 'text'), ('Two', 'https://www.two.com', 'p', 'paragraph')]
        )
        connection.commit()

        assert get_existing_links(cursor) == {'https://www.one.com', 'https://www.two.com'}

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
            [('One', 'https://www.one.com', 'p', 'text'), ('Two', 'https://www.one.com', 'p', 'paragraph')]
        )
        connection.commit()

        assert get_existing_links(cursor) == {'https://www.one.com'}

        connection.close()