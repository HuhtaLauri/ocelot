import psycopg2
from contextlib import contextmanager
from psycopg2.extensions import cursor
import json
from typing import Dict, List, Any


@contextmanager
def cursor(connection_string: str, commit: bool = False):
    conn = psycopg2.connect(connection_string)
    conn.autocommit = False

    try:
        cursor = conn.cursor()
        yield cursor
    except psycopg2.DatabaseError as err:
        raise err
    finally:
        if commit:
            conn.commit()
        conn.close()


def db_result_to_json(cursor: cursor) -> List[Dict[str, Any]]:

    # Get the column names from the cursor description
    columns = [desc[0] for desc in cursor.description]
    
    # Fetch all rows from the cursor
    rows = cursor.fetchall()
    
    # Create a list of dictionaries, each representing a row
    results = []
    for row in rows:
        results.append(dict(zip(columns, row)))
    
    # Convert the list of dictionaries to a JSON string
    return results

