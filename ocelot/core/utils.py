import psycopg2
from contextlib import contextmanager
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
