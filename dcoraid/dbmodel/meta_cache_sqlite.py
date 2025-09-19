import json
import pathlib
import sqlite3


class SQLiteKeyJSONDatabase:
    def __init__(self, db_name: str | pathlib.Path):
        """
        Initialize the database connection.

        Parameters
        ----------
        db_name
            The name of the SQLite database file (create if not exists)
        """
        # Disable check_same_thread. We will take care of serializing write
        # operations.
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS key_value_store
            (key TEXT PRIMARY KEY, value TEXT)
        """)

    def __iter__(self):
        self.cursor.execute("SELECT value FROM key_value_store")
        while True:
            row = self.cursor.fetchone()
            if row is None:
                break
            yield json.loads(row[0])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __getitem__(self, key):
        return self.read(key)

    def __setitem__(self, key, value):
        self.create(key, value)

    def create(self, key, value):
        """
        Create a new key-value pair in the database.

        Args:
            key (str): The key for the new value.
            value (str): The value associated with the key.

        Returns:
            None
        """
        self.cursor.execute(
            'INSERT OR REPLACE INTO key_value_store VALUES (?, ?)',
            (key, json.dumps(value)))
        self.conn.commit()

    def read(self, key):
        """
        Read the value associated with a given key from the database.

        Args:
            key (str): The key to read the value for.

        Returns:
            str: The value associated with the key, or None
             if the key does not exist.
        """
        self.cursor.execute(
            "SELECT value FROM key_value_store WHERE key = ?",
            (key,))
        result = self.cursor.fetchone()
        if result is None:
            raise KeyError(f"Key '{key}' does not exist.")
        return json.loads(result[0])

    def pop(self, key):
        """
        Delete a key-value pair from the database.

        Args:
            key (str): The key to delete.

        Returns:
            None
        """
        self.cursor.execute(
            'DELETE FROM key_value_store WHERE key = ?',
            (key,))
        self.conn.commit()

    def close(self):
        """
        Close the database connection.

        Returns:
            None
        """
        self.conn.close()
