from typing import List, Dict, Any
from sqlalchemy import Table
from psycopg2.extensions import cursor
from psycopg2.errors import DataError
from sqlalchemy import MetaData
from sqlalchemy.orm import declarative_base
from ocelot.core.diff import Diff
from ocelot.core.change import TableChange, ColumnChange, Operation


class Database:
    def __init__(self):
        self.tables: List[Table] = []
        self.sync_databases = []
        self.base = declarative_base(metadata=MetaData())

    def compare_tables(self, df_2: "Database") -> Diff:
        """
        Compare the existing of tables by schema and tables
        with self and passed Database

        For each change create a Change object and append it
        to diff.changes
        """
        diff = Diff()

        this_tables = set(self.base.metadata.tables.keys())
        other_tables = set(df_2.base.metadata.tables.keys())

        common_tables = this_tables.intersection(other_tables)
        new_tables = this_tables.difference(other_tables)
        dropped_tables = other_tables.difference(this_tables)

        if new_tables:
            for table in new_tables:
                print(table)
                change = TableChange(operation=Operation.ADD, table_id=table)
                print(change)
                diff.add_change(change)

        return diff

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
