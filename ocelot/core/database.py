from typing import List, Dict, Any
from sqlalchemy import Table
from psycopg2.extensions import cursor
from psycopg2.errors import DataError

class Database:
    def __init__(self):
        self.tables: List[Table] = []


    def compare_tables(self, df_2: 'Database'):
        """
        Compare the existing of tables by schema and tables
        with self and passed Database
        """

        raise NotImplementedError
        
    @staticmethod
    def db_result_to_json(cursor: cursor) -> List[Dict[str, Any]]:

        # Get the column names from the cursor description
        if not cursor.description:
            raise DataError

        columns = [desc[0] for desc in cursor.description]
        
        # Fetch all rows from the cursor
        rows = cursor.fetchall()
        
        # Create a list of dictionaries, each representing a row
        results = []
        for row in rows:
            results.append(dict(zip(columns, row)))
        
        # Convert the list of dictionaries to a JSON string
        return results

if __name__ == "__main__":
    pass
