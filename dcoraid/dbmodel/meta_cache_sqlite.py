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
            CREATE TABLE IF NOT EXISTS kv_store
            (key TEXT PRIMARY KEY, json_data TEXT, blob_data TEXT)
        """)

    def __iter__(self):
        self.cursor.execute("SELECT json_data FROM kv_store")
        while True:
            row = self.cursor.fetchone()
            if row is None:
                break
            yield json.loads(row[0])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __getitem__(self, key: str) -> dict:
        """Read the dataset dictionary given the dataset ID"""
        return self.read(key)

    def __setitem__(self, key: str, data: tuple[dict, str]) -> None:
        """When setting data, you must pass the search glob alongside the dict

        Parameters
        ----------
        key
            dataset identifier
        data
            tuple of dataset dictionary and search blob
        """
        ddict, blob = data
        self.create(key, ddict, blob)

    def create(self, key: str, ddict: dict, blob: str):
        """
        Create a new key-value-blob triple in the database.

        Parameters
        ----------
        key
            The key for the new value.
        ddict
            Data dictionary that is encoded in JSON
        blob
            Text blob to store alongside data for searching

        """
        self.cursor.execute(
            'INSERT OR REPLACE INTO kv_store VALUES (?, ?, ?)',
            (key, json.dumps(ddict), blob))
        self.conn.commit()

    def insert_many(self, db_insert: list[tuple[str, dict, str]]) -> None:
        """Insert multiple datasets at once to this database

        The `db_insert` is a list tuples of the form
        `(dataset_id, dataset_dictionary, search_blob)`.
        """
        self.cursor.executemany('INSERT INTO kv_store VALUES (?, ?, ?)',
                                db_insert)
        self.conn.commit()

    def read(self, key: str) -> dict:
        """Return dataset dictionary for given dataset ID

        Parameters
        ----------
        key
            The key to read the dictionary for.

        Returns
        -------
        ddict
            The corresponding dictionary extracted from `json_data`
        """
        self.cursor.execute(
            "SELECT json_data FROM kv_store WHERE key = ?",
            (key,))
        result = self.cursor.fetchone()
        if result is None:
            raise KeyError(f"Key '{key}' does not exist.")
        return json.loads(result[0])

    def search(self, query):
        """Search for a string in the blob_data"""
        self.cursor.execute(
            'SELECT key FROM kv_store WHERE blob_data LIKE ?', (f'%{query}%',))
        return [item[0] for item in self.cursor.fetchall()]

    def pop(self, key):
        """
        Delete a key from the database.

        Parameters
        ----------
        key
            The key to delete.
        """
        self.cursor.execute(
            'DELETE FROM kv_store WHERE key = ?',
            (key,))
        self.conn.commit()

    def close(self):
        """
        Close the database connection.
        """
        self.conn.close()
