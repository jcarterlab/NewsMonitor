import pytest
import sqlite3
from utils.database import initialise_database



@pytest.fixture
def db_config(tmp_path):
    class DummyConfig:
        DB_PATH = str(tmp_path / 'test_processed_headlines.db')
    return DummyConfig



class TestInitialiseDatabase:
    def test_creates_headlines_table(self, db_config):
        connection, cursor = initialise_database(db_config)
        cursor.execute('''
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='headlines'
        ''')
        result = cursor.fetchone()

        assert isinstance(connection, sqlite3.Connection)
        assert isinstance(cursor, sqlite3.Cursor)
        assert result == ('headlines',)

        connection.close()