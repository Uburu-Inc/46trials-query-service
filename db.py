import sqlite3
from queries.db_view import generate_db_view

def query_db(db_path: str, query: str):
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()

        # Use executescript for multiple statements (DROP + CREATE view)
        cursor.executescript(generate_db_view())

        # Then run the user query
        cursor.execute(query)
        rows = cursor.fetchall()

        return [dict(row) for row in rows]
